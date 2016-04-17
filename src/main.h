#ifndef _MAIN_H_
#define _MAIN_H_
#include "metadata.h"

#define K_OK 0
#define K_ERR -1

struct client_config {
    char *client_id;
    int max_wait;
    int min_bytes;
    int broker_count;
    char ** broker_list;
    short required_acks;
    int ack_timeout;
};

struct client_config *get_conf(); 
struct metadata_cache *get_metacache();
#endif
