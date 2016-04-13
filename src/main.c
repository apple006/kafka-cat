#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include "conn.h"
#include "request.h"
#include "buffer.h"
#include "main.h"
#include "util.h"

struct client_config conf;
struct metadata_cache cache;

int main(int argc, char **argv) {
    int ch;
    char *broker_list = NULL;

    srand(time(0));
    while((ch = getopt(argc, argv, "b:chv")) != -1) {
        switch(ch) {
            case 'b': broker_list = strdup(optarg); break;
            case 'c': break;
            case 'v': break;
            case 'h': break;
        }
    }

    conf.client_id = "test_client";
    conf.max_wait = 1000;
    conf.min_bytes = 1;
    conf.required_acks = 1;
    conf.ack_timeout = 100000;
    char *ips = "127.0.0.1:9092";
    conf.broker_list = split_string(ips, strlen(ips), ",", 1, &conf.broker_count);

    init_metadata_cache(&cache);

    char *topics = "sync_file";
    send_metadata_request(topics);
    send_produce_request(topics, 0, "test_key", "test_value");
    send_fetch_request(topics, 0, 0, 50);

    if (broker_list) free(broker_list);
}
