#ifndef _REQUEST_H_
#define _REQUEST_H_

#define API_VERSION 0
#define CURRENT_MAGIC 0

typedef enum {
    PRODUCE_KEY = 0,
    FETCH_KEY,
    OFFSET_KEY,
    METADATA_KEY,
    LEADERANDISR_KEY,
    STOPREPLICA_KEY,
    UPDATEMETADATA_KEY,
    CONTROLLEDSHUTDOWN_KEY,
    OFFSETCOMMIT_KEY,
    OFFSETFETCH_KEY,
    CONSUMERMETADATA_KEY,
    JOINGROUP_KEY,
    HEARTBEAT_KEY
} RequestId;

void dump_metadata(const char *topics);
void dump_topic_list();
struct metadata_response *send_metadata_request(const char *topics);
struct response *send_offsets_request(const char *topic, int part_id, int64_t timestamp, int max_num_offsets); 
struct response *send_fetch_request(const char *topic, int part_id, int64_t offset, int fetch_size);
struct response *send_produce_request(const char *topic, int part_id, const char *key, const char *value);
#endif
