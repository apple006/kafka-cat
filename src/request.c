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

void dump_topic_list() {
    int i;
    struct metadata_response *r;
    // set topic = NULL, will get all topic metedata in broker.
    r = send_metadata_request(NULL);
    if (!r) {
        logger(INFO, "dump topic failed.");
        return;
    }

    printf("topics: [\n");
    for (i = 0; i < r->topic_count; i++) {
        printf("\t%s\n", r->t_metas[i]->topic);
    }
    printf("]\n");
    dealloc_metadata_response(r);
}

struct topic_metadata *get_topic_metadata(const char *topic) {
    int i;
    struct topic_metadata *t_meta;
    struct metadata_cache *cache;
    struct metadata_response *r;
    
    if (!topic) return NULL;
    cache = get_metacache();
    if ((t_meta = get_topic_metadata_from_cache(cache, topic)) != NULL) {
        return t_meta;
    }
    r = send_metadata_request(topic);
    if (!r) return NULL;

    // set to cache
    update_broker_metadata(cache, r->broker_count, r->b_metas);

    t_meta = NULL;
    for (i = 0; i < r->topic_count; i++) {
        if (!r->t_metas[i]) continue;
        update_topic_metadata(cache, r->t_metas[i]);
    }

    t_meta = get_topic_metadata_from_cache(cache, topic);
    dealloc_metadata_response(r);
    return t_meta;
}

static int connect_leader_broker(const char *topic, int part_id) {
    int i, leader_id = -1, rc;
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
    for (i = 0; i < t_meta->partitions; i++) {
        if (t_meta->part_metas[i]->part_id == part_id) {
            leader_id = t_meta->part_metas[i]->leader_id;
        }
    }
    if (leader_id < 0) {
        logger(DEBUG, "leader id not found."); 
        return K_ERR;
    }
    b_meta = get_broker_metadata(cache, leader_id);
    if (!b_meta) {
        logger(DEBUG, "broker metadata not found."); 
        return K_ERR;
    }

    rc = connect_server(b_meta->host, b_meta->port);
    if (rc < 0) {
        logger(WARN, "connect to leader %s-%d failed.", b_meta->host, b_meta->port); 
    }
    return rc;
}

static int random_connect_broker() {
    int port, rand_idx = 0, host_len;
    char *ipport, host[16], *p;
    struct client_config *conf;

    conf = get_conf();
    if (conf->broker_count <= 0 || !conf->broker_list) return K_ERR; 

    if (conf->broker_count > 1) {
        rand_idx = rand() % conf->broker_count;
    }
    ipport = conf->broker_list[rand_idx];
    p = memchr(ipport, ':', strlen(ipport));
    if (!p) {
        logger(INFO, "broker list format error, should be ip:port.");
        return K_ERR;
    }
    host_len = p - ipport >= 15 ? 15 : p-ipport;
    memcpy(host, ipport, host_len);
    host[host_len] = '\0';
    errno = 0;
    if ((port = atoi(p + 1)) <= 0 || errno) {
        if (!errno) {
            logger(DEBUG, "random connect error, as port <= 0");
        } else {
            logger(DEBUG, "random connect error, as %s!", strerror(errno));
        }
        return K_ERR;
    }

    return connect_server(host, port);
}

void dump_metadata(const char *topics) {
    struct metadata_response *r;

    r = send_metadata_request(topics);
    dump_metadata_response(r);
    dealloc_metadata_response(r);
}

struct metadata_response *send_metadata_request(const char *topics) {
    int i, count, cfd;
    char **topic_arr;
    struct buffer *req, *meta_resp;
    struct metadata_response *r = NULL;

    if ((cfd = random_connect_broker()) <= 0) {
        logger(INFO, "random connect failed");
        return NULL;
    }

    req = alloc_request_buffer(METADATA_KEY);
    if (topics) {
        topic_arr = split_string(topics, strlen(topics), ",", 1, &count);
        write_int32_buffer(req, count);
        for (i = 0; i < count; i++) {
            write_short_string_buffer(req, topic_arr[i], strlen(topic_arr[i])); 
        }
    } else {
        write_int32_buffer(req, 0);
    }

    if (send_request(cfd, req) != K_OK) goto cleanup;
    meta_resp = wait_response(cfd);
    r = parse_metadata_response(meta_resp);
    dealloc_buffer(meta_resp);

cleanup:
    close(cfd);
    dealloc_buffer(req);
    if (topics) free_split_res(topic_arr, count);
    return r;
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

int64_t get_newest_offset(const char *topic, int part_id) {
    int i, j;
    int64_t ret = 0;
    struct response *r;
    struct topic_info *t_info;
    struct offsets_part_info *p_info = NULL;

    r = send_offsets_request(topic, part_id, -1, 1);
    if (!r || r->topic_count <= 0) goto RET;
    for (i = 0; i < r->topic_count; i++) {
        t_info = &r->t_infos[i];
        if (strlen(topic) != strlen(t_info->name)
                 || strncmp(topic, t_info->name, strlen(topic)) != 0) {
            continue;
        }
        for (j = 0; j < t_info->part_count; j++) {
            p_info = &t_info->p_infos[j];
            if (p_info && p_info->part_id == part_id && p_info->offset_count > 0) {
                ret = p_info->offsets[0] - 1;
                goto RET;
            }
        }
    }

RET:
    dealloc_response(r, OFFSET_KEY);
    return ret;
}

struct response *send_produce_request(const char *topic, int part_id, const char *key, const char *value) {
    int cfd, messageset_size = 0;
    int key_size, value_size, message_size;
    struct client_config *conf;
    struct buffer *req, *msg_buf, *resp_buf;
    struct response *r = NULL;

    cfd = connect_leader_broker(topic, part_id);
    if (cfd <= 0) return NULL;

    conf = get_conf();
    msg_buf = gen_message_buffer(key, value); // construct message body
    req = alloc_request_buffer(PRODUCE_KEY); // request type
    write_int16_buffer(req, conf->required_acks); // required_acks
    write_int32_buffer(req, conf->ack_timeout); // ack_timeout
    write_int32_buffer(req, 1); // topic count
    write_short_string_buffer(req, topic, strlen(topic)); // topic
    write_int32_buffer(req, 1); // partition count
    write_int32_buffer(req, part_id); // partition id
    key_size = key ? strlen(key) : 0;
    value_size = value ? strlen(value) : 0;
    // crc(4bytes) + magic (1byte) + attr (1byte) + key + value
    message_size = 4 + 1 + 1 + key_size + 4 + value_size + 4;
    messageset_size = message_size + MSG_OVERHEAD;
    write_int32_buffer(req, messageset_size); // message set size
    write_int64_buffer(req, 0); // offset
    write_int32_buffer(req, message_size); // message size
    write_int32_buffer(req, get_buffer_crc32(msg_buf)); // crc
    write_raw_string_buffer(req, get_buffer_data(msg_buf), get_buffer_used(msg_buf)); // message body

    if (send_request(cfd, req) == K_ERR) goto cleanup;
    if (conf->required_acks == 0) goto cleanup; // do nothing when required_acks = 0
    resp_buf = wait_response(cfd);
    r = parse_response(resp_buf, PRODUCE_KEY);
    dealloc_buffer(resp_buf);

cleanup:
    close(cfd);
    dealloc_buffer(msg_buf);
    dealloc_buffer(req);
    return r;
}

struct response *send_offsets_request(const char *topic, int part_id, int64_t timestamp, int max_num_offsets) {
    int cfd;
    struct buffer *req, *resp_buf;
    struct response *r = NULL;

    // connect to leader
    cfd = connect_leader_broker(topic, part_id);
    if (cfd <= 0) return NULL;

    req = alloc_request_buffer(OFFSET_KEY); // request key
    write_int32_buffer(req, -1); // replica id
    write_int32_buffer(req, 1); // topic count
    write_short_string_buffer(req, topic, strlen(topic)); // topic
    write_int32_buffer(req, 1); // partition count
    write_int32_buffer(req, part_id); // partition id 
    write_int64_buffer(req, timestamp); // timestamp
    write_int32_buffer(req, max_num_offsets); // max num offsets

    if(send_request(cfd, req) != K_OK) goto cleanup;
    resp_buf = wait_response(cfd);
    r = parse_response(resp_buf, OFFSET_KEY); 
    dealloc_buffer(resp_buf);

cleanup:
    close(cfd);
    dealloc_buffer(req);
    return r;
}

struct response *send_fetch_request(const char *topic, int part_id, int64_t offset, int fetch_size) {
    int cfd;
    struct client_config *conf;
    struct buffer *req, *resp_buf;
    struct response *r = NULL;

    // connect to leader
    cfd = connect_leader_broker(topic, part_id);
    if (cfd <= 0) return NULL;
    if (offset < 0) offset = get_newest_offset(topic, part_id);

    conf = get_conf();
    req = alloc_request_buffer(FETCH_KEY); // request key
    write_int32_buffer(req, -1); // replica id
    write_int32_buffer(req, conf->max_wait); // max wait
    write_int32_buffer(req, conf->min_bytes); // min bytes
    write_int32_buffer(req, 1); // topic count
    write_short_string_buffer(req, topic, strlen(topic)); // topic
    write_int32_buffer(req, 1); // partition count
    write_int32_buffer(req, part_id); // partition id
    write_int64_buffer(req, offset); // offset
    write_int32_buffer(req, fetch_size); // fetch siz3

    if(send_request(cfd, req) != K_OK) goto cleanup;
    resp_buf = wait_response(cfd);
    r = parse_response(resp_buf, FETCH_KEY);
    dealloc_buffer(resp_buf);

cleanup:
    close(cfd);
    dealloc_buffer(req);
    return r;
}
