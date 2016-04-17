#ifndef _RESPONSE_H_
#define _RESPONSE_H_
#include "buffer.h"
#include "request.h"

#define MSG_OVERHEAD 12 /* offset(8 bytes) + size (4 bytes)*/ 

struct message {
    int64_t offset;
    char *key;
    char *value;
};

struct messageset {
    int cap;
    int used;
    struct message *msgs;
};

struct fetch_part_info {
    int part_id;
    int err_code;
    int64_t hw;
    int total_bytes;
    struct messageset *msg_set;
};

struct offsets_part_info {
    int part_id;
    int err_code;
    int offset_count;
    int64_t *offsets;
};

struct produce_part_info {
    int part_id;
    int err_code;
    int64_t offset;
};

struct topic_info {
    char *name;
    int part_count;
    void *p_infos;
};

struct response {
    int topic_count;
    struct topic_info t_infos[0];
};

struct buffer *wait_response(int cfd);
void parse_and_store_metadata(struct buffer *response);
struct response *parse_response(struct buffer *resp_buf, int type); 
void dealloc_response(struct response *r, int type); 
void dump_metadata_response(struct buffer *response);
void dump_produce_response(struct response *r);
void dump_offsets_response(struct response *r); 
void dump_fetch_response(struct response *r);
#endif
