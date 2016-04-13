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

int send_metadata_request(const char *topics);
int send_fetch_request(char *topic, int part_id, int64_t offset, int fetch_size);
int send_produce_request(char *topic, int part_id, const char *key, const char *value);
#endif
