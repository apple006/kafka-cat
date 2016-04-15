#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include "response.h"
#include "metadata.h"
#include "main.h"
#include "conn.h"
#include "util.h"
#include "error_map.h"
#include "cJSON/cJSON.h"

struct client_config conf;

struct buffer *wait_response(int cfd) {
    int rbytes = 0, rc, r, remain, resp_size;
    struct buffer *response;

    rc = wait_socket_data(cfd, 3000, CR_READ);
    if (rc <= 0) { // timeout or error
       logger(DEBUG, "wait response error, as %s!", strerror(errno));
        return NULL;
    }

    response = alloc_buffer(128);
    remain = get_buffer_cap(response);
    TIME_START();
    while (rbytes < 4) {
        r = read(cfd, get_buffer_data(response) + rbytes, remain);
        if (r <= 0) {
            if (r == -1 &&
                   (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK)) continue;
            if (r == 0) close(cfd);
            logger(DEBUG, "wait response error, as %s!", strerror(errno));
            goto err_cleanup;
        }
        rbytes += r;
        remain -= r;
        incr_buffer_used(response, r);
    }
    resp_size = read_int32_buffer(response) + 4; 
    need_expand(response, resp_size);
    while (rbytes < resp_size) {
        r = read(cfd, get_buffer_data(response) + rbytes, resp_size - rbytes);
        if (r <= 0) {
            if (r == -1 &&
                   (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK)) continue;
            if (r == 0) close(cfd);
            logger(DEBUG, "wait response error, as %s!", strerror(errno));
            goto err_cleanup;
        }
        rbytes += r;
        incr_buffer_used(response, r);
    }
    TIME_END();
    logger(DEBUG, "Total time cost %lldus in wait response", TIME_COST());
    return response;

err_cleanup:
    dealloc_buffer(response);
    return NULL;
}

static cJSON *parse_message_set(struct buffer *response) {
    int size, key_size, value_size;
    long offset;
    char *key, *value;
    cJSON *messages, *message_obj;

    messages = cJSON_CreateArray();
    while(!is_buffer_eof(response)) {
        if (get_buffer_unread(response) < MSG_OVERHEAD) {
            skip_buffer_bytes(response, get_buffer_unread(response));
            break;
        }
        offset = read_int64_buffer(response);
        size = read_int32_buffer(response); // message size
        if (get_buffer_unread(response) < size) {
            skip_buffer_bytes(response, get_buffer_unread(response));
            break;
        }
        message_obj = cJSON_CreateObject();
        printf("offset:%ld\n", offset);
        cJSON_AddNumberToObject(message_obj, "offset", offset);
        cJSON_AddNumberToObject(message_obj, "size", size);
        skip_buffer_bytes(response, 4 + 1 + 1); //skip crc + magic + attr
        key_size = read_int32_buffer(response); // key size
        if (key_size > 0) {
            key = malloc(key_size + 1);
            read_raw_string_buffer(response, key, key_size);
            key[key_size] = '\0';
            cJSON_AddStringToObject(message_obj, "key", key);
            free(key);
        }
        value_size = read_int32_buffer(response); // value size
        if (value_size > 0) {
            value = malloc(value_size + 1);
            read_raw_string_buffer(response, value, value_size);
            value[value_size] = '\0';
            cJSON_AddStringToObject(message_obj, "value", value);
            free(value);
        }
        cJSON_AddItemToArray(messages, message_obj);
    }
    return messages;
}

void dump_offsets_response(struct buffer *response) {
    int i, j, k, old_pos, topic_count, part_count;
    int err_code, part_id, num_offsets;
    long long offsets;
    char *topic;

    if (!response) {
        logger(ERROR, "response is null.");
    }
    old_pos = get_buffer_pos(response);
    // corelation id
    read_int32_buffer(response);
    topic_count = read_int32_buffer(response);
    for (i = 0; i < topic_count; i++) {
        topic = read_short_string_buffer(response);
        printf("{ topic: %s, partitions: \n\t[\n", topic);
        part_count = read_int32_buffer(response);
        for(j = 0; j < part_count; j++) {
            part_id = read_int32_buffer(response);
            err_code = read_int16_buffer(response);
            num_offsets = read_int32_buffer(response);
            printf("\t\t{ part_id:%d, err_code:%d, offsets: [", part_id, err_code);
            for (k = 0; k < num_offsets; k++) {
                offsets = read_int64_buffer(response);
                if(k == num_offsets-1) {
                    printf("%lld", offsets);
                } else {
                    printf("%lld, ", offsets);
                }
            }
            printf("] }\n");
        }
        printf("\t]\n}\n");
        free(topic);
    }

    reset_buffer_pos(response, old_pos);
}

void dump_fetch_response(struct buffer *response) {
    int i, j, part_count, topic_count;
    int part_id, err_code, hw, total_bytes, old_pos;
    char *topic, *json_str;
    cJSON *root, *topic_obj, *part_obj, *parts, *topics;
    cJSON *messages_obj;

    if (!response) {
        logger(ERROR, "response is null.");
    }
    old_pos = get_buffer_pos(response);
    root = cJSON_CreateObject();
    // corelation id
    cJSON_AddNumberToObject(root, "corelation_id",read_int32_buffer(response));
    topics = cJSON_CreateArray();
    topic_count = read_int32_buffer(response);
    for (i = 0; i < topic_count; i++) {
        topic_obj = cJSON_CreateObject(); 
        topic = read_short_string_buffer(response);
        cJSON_AddStringToObject(topic_obj, "name", topic);
        part_count = read_int32_buffer(response);
        parts = cJSON_CreateArray();
        for (j = 0; j < part_count; j++) {
            part_obj = cJSON_CreateObject();
            part_id = read_int32_buffer(response);
            err_code = read_int16_buffer(response);
            hw = read_int64_buffer(response);
            total_bytes = read_int32_buffer(response);
            messages_obj = parse_message_set(response);
            cJSON_AddNumberToObject(part_obj, "err_code", err_code);
            if (err_code > 0) {
                cJSON_AddStringToObject(part_obj, "err_msg", err_map[err_code]);
            }
            cJSON_AddNumberToObject(part_obj, "part_id", part_id);
            cJSON_AddNumberToObject(part_obj, "high_water", hw);
            cJSON_AddNumberToObject(part_obj, "total_bytes", total_bytes);
            cJSON_AddItemToObject(part_obj, "messages", messages_obj);
            cJSON_AddItemToArray(parts, part_obj);
        }
        cJSON_AddItemToObject(topic_obj, "partitions", parts);
        cJSON_AddItemToArray(topics, topic_obj);
        free(topic);
    }
    cJSON_AddItemToObject(root, "topics", topics);
    reset_buffer_pos(response, old_pos);

    json_str = cJSON_Print(root);
    printf("%s\n", json_str);
    free(json_str);
    cJSON_Delete(root);
}

void dump_produce_response(struct buffer *response) {
    int i, j, topic_count, part_count;
    int part_id, err_code, old_pos;
    int64_t offset;
    char *topic, *json_str;
    cJSON *root, *topic_obj, *part_obj, *parts, *topics;

    if (!response) {
        logger(ERROR, "response is null.");
    }
    old_pos = get_buffer_pos(response);
    root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "corelation_id",read_int32_buffer(response));
    topic_count = read_int32_buffer(response);
    topics = cJSON_CreateArray();
    for (i = 0; i < topic_count; i++) {
        topic_obj = cJSON_CreateObject(); 
        topic = read_short_string_buffer(response);
        cJSON_AddStringToObject(topic_obj, "name", topic);
        part_count = read_int32_buffer(response); 
        parts = cJSON_CreateArray();
        for (j = 0; j < part_count; j++) {
            part_obj = cJSON_CreateObject();
            part_id = read_int32_buffer(response); 
            err_code = read_int16_buffer(response);
            offset = read_int64_buffer(response);
            cJSON_AddNumberToObject(part_obj, "err_code", err_code);
            if (err_code > 0) {
                cJSON_AddStringToObject(part_obj, "err_msg", err_map[err_code]);
            }
            cJSON_AddNumberToObject(part_obj, "part_id", part_id);
            cJSON_AddNumberToObject(part_obj, "offset", offset);
            cJSON_AddItemToArray(parts, part_obj);
        }
        cJSON_AddItemToObject(topic_obj, "partitions", parts);
        cJSON_AddItemToArray(topics, topic_obj);
        free(topic);
    }
    cJSON_AddItemToObject(root, "topics", topics);
    reset_buffer_pos(response, old_pos);

    json_str = cJSON_Print(root);
    printf("%s\n", json_str);
    free(json_str);
    cJSON_Delete(root);
}

cJSON *parse_broker_list(struct buffer *resp) {
    int i, id, port, broker_count;
    char *host;
    cJSON *brokers, *broker_obj;

    broker_count = read_int32_buffer(resp);
    brokers = cJSON_CreateArray();
    for(i = 0; i < broker_count; i++) {
        broker_obj = cJSON_CreateObject();
        id = read_int32_buffer(resp);
        host = read_short_string_buffer(resp);
        port = read_int32_buffer(resp);
        cJSON_AddNumberToObject(broker_obj, "id", id);
        cJSON_AddStringToObject(broker_obj, "host", host);
        cJSON_AddNumberToObject(broker_obj, "port", port);
        cJSON_AddItemToArray(brokers, broker_obj);
        free(host);
    }
    return brokers;
}

cJSON *parse_topic_metadata(struct buffer *resp) {
    int i, j, err_code, part_count, part_id, leader_id;
    int replica_count, *replicas, isr_count, *isr;
    char *topic;
    cJSON *topic_obj, *part_obj, *parts;

    topic_obj = cJSON_CreateObject();
    err_code = read_int16_buffer(resp);
    topic = read_short_string_buffer(resp);
    part_count = read_int32_buffer(resp);
    cJSON_AddNumberToObject(topic_obj, "err_code", err_code);
    if (err_code > 0) {
        cJSON_AddStringToObject(topic_obj, "err_msg", err_map[err_code]);
    }
    cJSON_AddStringToObject(topic_obj, "name", topic);

    parts = cJSON_CreateArray();
    cJSON_AddItemToObject(topic_obj, "partitions",parts);
    if (part_count == 0) return topic_obj;

    for(i = 0; i < part_count; i++) {
        part_obj = cJSON_CreateObject();
        err_code = read_int16_buffer(resp);
        part_id = read_int32_buffer(resp);
        leader_id = read_int32_buffer(resp);
        cJSON_AddNumberToObject(part_obj, "err_code", err_code);
        if (err_code > 0) {
            cJSON_AddStringToObject(topic_obj, "err_msg", err_map[err_code]);
        }
        cJSON_AddNumberToObject(part_obj, "part_id", part_id);
        cJSON_AddNumberToObject(part_obj, "leader_id", leader_id);
        replica_count = read_int32_buffer(resp);
        replicas = malloc(replica_count * sizeof(int));
        for (j = 0; j < replica_count; j++) {
            replicas[j] = read_int32_buffer(resp); 
        }
        cJSON_AddItemToObject(part_obj, "replicas", cJSON_CreateIntArray(replicas, replica_count));
        isr_count = read_int32_buffer(resp);
        isr = malloc(isr_count * sizeof(int));
        for (j = 0; j < isr_count; j++) {
            isr[j] = read_int32_buffer(resp); 
        }
        cJSON_AddItemToObject(part_obj, "isr", cJSON_CreateIntArray(replicas, replica_count));
        cJSON_AddItemToArray(parts, part_obj);
        free(replicas);
        free(isr);
    }

    free(topic);
    return topic_obj;
}

void dump_metadata(struct buffer *response) {
    int i, metadata_count,old_pos;
    cJSON *root, *topics, *topic_obj;
    char *json_str;

    if (!response) {
        logger(ERROR, "response is null.");
    }
    old_pos = get_buffer_pos(response);
    root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "corelation_id",read_int32_buffer(response));
    cJSON_AddItemToObject(root, "brokers", parse_broker_list(response));
    topics = cJSON_CreateArray();
    cJSON_AddItemToObject(root, "topics", topics);
    metadata_count = read_int32_buffer(response);
    for(i = 0; i < metadata_count; i++) {
        topic_obj = parse_topic_metadata(response);
        cJSON_AddItemToArray(topics, topic_obj);
    }
    reset_buffer_pos(response, old_pos);

    json_str = cJSON_Print(root);
    printf("%s\n", json_str);
    free(json_str);
    cJSON_Delete(root);
}

void parse_and_store_metadata(struct buffer *response) {
    int i, j, k, old_pos, broker_count, metadata_count, part_count, err_code;
    char *topic;
    struct broker_metadata *b_meta;
    struct metadata_cache *cache;

    old_pos = get_buffer_pos(response);
    read_int32_buffer(response); // ignore correlation id
    broker_count = read_int32_buffer(response);
    cache = get_metacache();
    update_broker_metadata(cache, broker_count);
    for(i = 0; i < broker_count; i++) {
        b_meta = &cache->broker_metas[i]; 
        b_meta->id = read_int32_buffer(response);
        b_meta->host = read_short_string_buffer(response);
        b_meta->port = read_int32_buffer(response);
    }

    metadata_count = read_int32_buffer(response);
    for(i = 0; i < metadata_count; i++) {
        err_code = read_int16_buffer(response);
        topic = read_short_string_buffer(response);
        part_count = read_int32_buffer(response);
        if (part_count == 0) continue;
        delete_topic_metadata_from_cache(cache, topic);
        struct topic_metadata *t_meta;
        struct partition_metadata* p_meta;
        t_meta = add_topic_metadata_to_cache(cache, topic, part_count);
        for(j = 0; j < part_count; j++) {
            p_meta = alloc_partition_metadata(); 
            t_meta->part_metas[j] = p_meta;
            err_code = read_int16_buffer(response); 
            p_meta->err_code = err_code;
            p_meta->part_id = read_int32_buffer(response);
            p_meta->leader_id = read_int32_buffer(response);
            p_meta->replica_count = read_int32_buffer(response);
            p_meta->replicas = malloc(p_meta->replica_count * sizeof(int));
            for (k = 0; k < p_meta->replica_count; k++) {
                p_meta->replicas[k] = read_int32_buffer(response);
            }
            p_meta->isr_count = read_int32_buffer(response);
            p_meta->isr = malloc( p_meta->isr_count * sizeof(int));
            for (k = 0; k < p_meta->isr_count; k++) {
                p_meta->isr[k] = read_int32_buffer(response);
            }
        }
        free(topic);
    }

    reset_buffer_pos(response, old_pos);
}
