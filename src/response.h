#ifndef _RESPONSE_H_
#define _RESPONSE_H_
#include "buffer.h"

#define MSG_OVERHEAD 12 /* offset(8 bytes) + size (4 bytes)*/ 

struct buffer *wait_response(int cfd);
void parse_and_store_metadata(struct buffer *response);
void dump_fetch_response(struct buffer *response);
void dump_produce_response(struct buffer *response);
void dump_metadata(struct buffer *response);
#endif
