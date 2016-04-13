#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include "response.h"
#include "metadata.h"
#include "main.h"
#include "conn.h"
#include "cJSON/cJSON.h"

struct metadata_cache cache;
struct client_config conf;

struct buffer *wait_response(int cfd) {
    int rbytes = 0, rc, r, remain, resp_size;
    struct buffer *response;

    rc = wait_socket_data(cfd, 3000, CR_READ);
    if (rc <= 0) return NULL; // timeout or error

    response = alloc_buffer(128);
    remain = get_buffer_cap(response);
    while (rbytes < 4) {
        r = read(cfd, get_buffer_data(response) + rbytes, remain);
        if (r <= 0) {
            if (r == -1 &&
                   (errno == EAGAIN || errno == EINTR || errno == EWOULDBLOCK)) continue;
            if (r == 0) close(cfd);
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
            goto err_cleanup;
        }
        rbytes += r;
        incr_buffer_used(response, r);
    }
    return response;

err_cleanup:
    dealloc_buffer(response);
    return NULL;
}

static cJSON *dump_message_set(struct buffer *response) {
    int size, key_size, value_size;
    int64_t offset;
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

void dump_fetch_response(struct buffer *response) {
    int i, j, part_count, topic_count;
    int part_id, err_code, hw, total_bytes;
    char *topic, *json_str;
    cJSON *root, *topic_obj, *part_obj, *parts, *topics;
    cJSON *messages_obj;

    // correlation id
    root = cJSON_CreateObject();
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
            messages_obj = dump_message_set(response);
            cJSON_AddNumberToObject(part_obj, "err_code", err_code);
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
    json_str = cJSON_Print(root);
    printf("%s\n", json_str);
    free(json_str);
    cJSON_Delete(root);
}

void dump_produce_response(struct buffer *response) {
    int i, j, topic_count, part_count;
    int part_id, err_code;
    int64_t offset;
    char *topic, *json_str;
    cJSON *root, *topic_obj, *part_obj, *parts, *topics;

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
            cJSON_AddNumberToObject(part_obj, "part_id", part_id);
            cJSON_AddNumberToObject(part_obj, "offset", offset);
            cJSON_AddItemToArray(parts, part_obj);
        }
        cJSON_AddItemToObject(topic_obj, "partitions", parts);
        cJSON_AddItemToArray(topics, topic_obj);
        free(topic);
    }
    cJSON_AddItemToObject(root, "topics", topics);
    json_str = cJSON_Print(root);
    printf("%s\n", json_str);
    free(json_str);
    cJSON_Delete(root);
}

void parse_and_store_metadata(struct buffer *resp) {
    int i, j, k, broker_count, metadatas, partitions;
    short err_code;
    char *topic;
    struct broker_metadata *b_meta;

    // correlatioin id
    read_int32_buffer(resp);
    broker_count = read_int32_buffer(resp);
    update_broker_metadata(&cache, broker_count);
    for(i = 0; i < broker_count; i++) {
        b_meta = &cache.broker_metas[i]; 
        b_meta->id = read_int32_buffer(resp);
        b_meta->host = read_short_string_buffer(resp);
        b_meta->port = read_int32_buffer(resp);
    }

    metadatas = read_int32_buffer(resp);
    for(i = 0; i < metadatas; i++) {
        err_code = read_int16_buffer(resp);
        topic = read_short_string_buffer(resp);
        partitions = read_int32_buffer(resp);
        if (partitions == 0) continue;
        delete_topic_metadata_from_cache(&cache, topic);
        struct topic_metadata *t_meta;
        struct partition_metadata* p_meta;
        t_meta = add_topic_metadata_to_cache(&cache, topic, partitions);
        for(j = 0; j < partitions; j++) {
            p_meta = alloc_partition_metadata(); 
            t_meta->part_metas[i] = p_meta;
            err_code = read_int16_buffer(resp); 
            p_meta->part_id = read_int32_buffer(resp);
            p_meta->leader_id = read_int32_buffer(resp);
            p_meta->replica_count = read_int32_buffer(resp);
            p_meta->replicas = malloc(p_meta->replica_count * sizeof(int));
            for (k = 0; k < p_meta->replica_count; k++) {
                p_meta->replicas[k] = read_int32_buffer(resp);
            }
            p_meta->isr_count = read_int32_buffer(resp);
            p_meta->isr = malloc( p_meta->isr_count * sizeof(int));
            for (k = 0; k < p_meta->isr_count; k++) {
                p_meta->isr[k] = read_int32_buffer(resp);
            }
        }
        free(topic);
    }
}
