#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
 #include <unistd.h>
#include "buffer.h"
#include "util.h"
#include "request.h"
#include "response.h"
#include "metadata.h"
#include "main.h"
#include "conn.h"

static int32_t corr_id = 1001;

static void rewrite_request_size(struct buffer *req_buf, int req_size) {
    char buf[4];
    buf[0] = req_size >> 24;
    buf[1] = req_size >> 16;
    buf[2] = req_size >> 8;
    buf[3] = req_size & 0xff;
    memcpy(get_buffer_data(req_buf), buf, 4);
}

static int send_request(int cfd, struct buffer *req_buf) {
    int w_bytes = 0, w, total_bytes, rc;

    total_bytes = get_buffer_used(req_buf);
    if (total_bytes <= 4) return K_ERR; // fixed 4 bytes request size
    rewrite_request_size(req_buf, total_bytes - 4); // remove request size

    TIME_START();
    rc = wait_socket_data(cfd, 3000, CR_WRITE);
    if (rc <= 0) {
        logger(DEBUG, "send request error on wait_socket_data, as %s!", strerror(errno));
        return K_ERR;
    }

    while(w_bytes < total_bytes) {
        w = write(cfd, get_buffer_data(req_buf) + w_bytes, total_bytes - w_bytes); 
        if (w == -1 && errno != EAGAIN && errno != EINTR) {
            logger(DEBUG, "send request error, as %s!", strerror(errno));
            return K_ERR;
        }
        w_bytes += w;
    }
    TIME_END();
    logger(DEBUG, "Total time cost %lldus in send requst", TIME_COST());
    return 0;
}

static struct buffer *alloc_request_buffer(RequestId key) {
    char *client_id;
    struct client_config *conf;
    struct buffer *req_buf= alloc_buffer(16);
    if(!req_buf) return NULL;

    conf = get_conf();
    write_int32_buffer(req_buf, 0); // prealloc for request size
    write_int16_buffer(req_buf, key); // request type
    write_int16_buffer(req_buf, API_VERSION); // version
    write_int32_buffer(req_buf, ++corr_id); // correlation id
    client_id = conf->client_id;
    write_short_string_buffer(req_buf, client_id, strlen(client_id)); // client id
    return req_buf;
}

struct topic_metadata *get_topic_metadata(const char *topic) {
    struct topic_metadata * t_meta;
    struct metadata_cache *cache;
    
    cache = get_metacache();
    if ((t_meta = get_topic_metadata_from_cache(cache, topic)) != NULL) {
        return t_meta;
    }
    send_metadata_request(topic, 0);
    t_meta = get_topic_metadata_from_cache(cache, topic);

    return t_meta;
}

static int connect_leader_broker(char *topic, int part_id) {
    int leader_id;
    struct topic_metadata *t_meta;
    struct broker_metadata *b_meta;
    struct metadata_cache *cache;

    cache = get_metacache();
    // get topic-partition leader info from cache or metadata request.
    TIME_START();
    t_meta = get_topic_metadata(topic);
    TIME_END();
    logger(DEBUG, "Total time cost %lldus in fetch meta", TIME_COST());
    if (!t_meta || part_id >= t_meta->partitions) {
        logger(DEBUG, "Topic metadata not found."); 
        return K_ERR;
    }
    leader_id = t_meta->part_metas[part_id]->leader_id;
    if (leader_id < 0) {
        logger(DEBUG, "leader id not found."); 
        return K_ERR;
    }
    b_meta = get_broker_metadata(cache, leader_id);
    if (!b_meta) {
        logger(DEBUG, "broker metadata not found."); 
        return K_ERR;
    }

    return connect_server(b_meta->host, b_meta->port);
}

static int random_connect_broker() {
    int port, rand_idx = 0;
    char *ipport, host[16], *p;
    struct client_config *conf;

    conf = get_conf();
    if (conf->broker_count <= 0 || !conf->broker_list) return K_ERR; 

    if (conf->broker_count > 1) {
        rand_idx = rand() % conf->broker_count;
    }
    ipport = conf->broker_list[rand_idx];
    p = memchr(ipport, ':', strlen(ipport));
    memcpy(host, ipport, p - ipport);
    host[p - ipport] = '\0';
    errno = 0;
    port = atoi(p + 1);
    if(port <= 0 || errno) {
        if (port <= 0) {
            logger(DEBUG, "random connect error, as port <= 0");
        } else {
            logger(DEBUG, "random connect error, as %s!", strerror(errno));
        }
        return K_ERR;
    }

    return connect_server(host, port);
}

int send_metadata_request(const char *topics, int is_dump) {
    int i, count, cfd, ret = K_ERR;
    char **topic_arr;
    struct buffer *metadata_req, *meta_resp;

    if (!topics) return K_ERR;
    if ((cfd = random_connect_broker()) <= 0) {
        logger(DEBUG, "random connect failed");
        return K_ERR;
    }

    metadata_req = alloc_request_buffer(METADATA_KEY);
    topic_arr = split_string(topics, strlen(topics), ",", 1, &count);
    write_int32_buffer(metadata_req, count);
    for (i = 0; i < count; i++) {
        write_short_string_buffer(metadata_req, topic_arr[i], strlen(topic_arr[i])); 
    }

    if (send_request(cfd, metadata_req) != K_OK) goto cleanup;
    meta_resp = wait_response(cfd);
    if (is_dump) {
        dump_metadata(meta_resp); 
    } else {
        parse_and_store_metadata(meta_resp);
    }
    dealloc_buffer(meta_resp);
    ret = K_OK;

cleanup:
    close(cfd);
    dealloc_buffer(metadata_req);
    free_split_res(topic_arr, count);
    return ret;
}


static struct buffer *gen_message_buffer(const char *key, const char *value) {
    struct buffer *msg_buf;

    msg_buf = alloc_buffer(32);
    write_int8_buffer(msg_buf, CURRENT_MAGIC); // magic
    write_int8_buffer(msg_buf, 0); // attr
    write_string_buffer(msg_buf, key); // key
    write_string_buffer(msg_buf, value); //value
    return msg_buf;
}

int send_produce_request(char *topic, int part_id, const char *key, const char *value) {
    int cfd, messageset_size = 0;
    int key_size, value_size, message_size;
    struct client_config *conf;
    struct buffer *produce_req, *msg_buf, *produce_resp;

    cfd = connect_leader_broker(topic, part_id);
    if (cfd <= 0) return K_ERR;

    conf = get_conf();
    msg_buf = gen_message_buffer(key, value); // construct message body
    produce_req = alloc_request_buffer(PRODUCE_KEY); // request type
    write_int16_buffer(produce_req, conf->required_acks); // required_acks
    write_int32_buffer(produce_req, conf->ack_timeout); // ack_timeout
    write_int32_buffer(produce_req, 1); // topic count
    write_short_string_buffer(produce_req, topic, strlen(topic)); // topic
    write_int32_buffer(produce_req, 1); // partition count
    write_int32_buffer(produce_req, part_id); // partition id
    key_size = key ? strlen(key) : 0;
    value_size = value ? strlen(value) : 0;
    // crc(4bytes) + magic (1byte) + attr (1byte) + key + value
    message_size = 4 + 1 + 1 + key_size + 4 + value_size + 4;
    messageset_size = message_size + MSG_OVERHEAD;
    write_int32_buffer(produce_req, messageset_size); // message set size
    write_int64_buffer(produce_req, 0); // offset
    write_int32_buffer(produce_req, message_size); // message size
    write_int32_buffer(produce_req, get_buffer_crc32(msg_buf)); // crc
    write_raw_string_buffer(produce_req, get_buffer_data(msg_buf), get_buffer_used(msg_buf)); // message body

    if (send_request(cfd, produce_req) == K_ERR) goto cleanup;
    if (conf->required_acks == 0) goto cleanup; // do nothing when required_acks = 0
    produce_resp = wait_response(cfd);
    dump_produce_response(produce_resp);
    dealloc_buffer(produce_resp);

cleanup:
    close(cfd);
    dealloc_buffer(msg_buf);
    dealloc_buffer(produce_req);
    return K_OK;
}

int send_offsets_request(char *topic, int part_id, int64_t timestamp, int max_num_offsets) {
    int cfd;
    struct buffer *offsets_req, *offsets_resp;

    // connect to leader
    cfd = connect_leader_broker(topic, part_id);
    if (cfd <= 0) return K_ERR;

    offsets_req = alloc_request_buffer(OFFSET_KEY); // request key
    write_int32_buffer(offsets_req, -1); // replica id
    write_int32_buffer(offsets_req, 1); // topic count
    write_short_string_buffer(offsets_req, topic, strlen(topic)); // topic
    write_int32_buffer(offsets_req, 1); // partition count
    write_int32_buffer(offsets_req, part_id); // partition id 
    write_int64_buffer(offsets_req, timestamp); // timestamp
    write_int32_buffer(offsets_req, max_num_offsets); // max num offsets

    if(send_request(cfd, offsets_req) != K_OK) goto cleanup;
    offsets_resp = wait_response(cfd);
    dump_offsets_response(offsets_resp);
    dealloc_buffer(offsets_resp);

cleanup:
    close(cfd);
    dealloc_buffer(offsets_req);
    return K_OK;
}

int send_fetch_request(char *topic, int part_id, int64_t offset, int fetch_size) {
    int cfd;
    struct client_config *conf;
    struct buffer *fetch_req, *fetch_resp;

    // connect to leader
    cfd = connect_leader_broker(topic, part_id);
    if (cfd <= 0) return K_ERR;

    conf = get_conf();
    fetch_req = alloc_request_buffer(FETCH_KEY); // request key
    write_int32_buffer(fetch_req, -1); // replica id
    write_int32_buffer(fetch_req, conf->max_wait); // max wait
    write_int32_buffer(fetch_req, conf->min_bytes); // min bytes
    write_int32_buffer(fetch_req, 1); // topic count
    write_short_string_buffer(fetch_req, topic, strlen(topic)); // topic
    write_int32_buffer(fetch_req, 1); // partition count
    write_int32_buffer(fetch_req, part_id); // partition id
    write_int64_buffer(fetch_req, offset); // offset
    write_int32_buffer(fetch_req, fetch_size); // fetch siz3

    if(send_request(cfd, fetch_req) != K_OK) goto cleanup;
    fetch_resp = wait_response(cfd);
    dump_fetch_response(fetch_resp);
    dealloc_buffer(fetch_resp);

cleanup:
    close(cfd);
    dealloc_buffer(fetch_req);
    return K_OK;
}
