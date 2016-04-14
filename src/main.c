#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include "conn.h"
#include "request.h"
#include "buffer.h"
#include "main.h"
#include "util.h"

static struct client_config *conf;
static struct metadata_cache *cache;

static void init_conf(char *client_id, char *brokers) {
    conf = malloc(sizeof(*conf));
    conf->min_bytes = 1;
    conf->max_wait = 1000;
    conf->required_acks = 1;
    conf->ack_timeout = 1000;
    conf->broker_list = NULL;
    conf->broker_count = 0;
    if (brokers) {
        conf->broker_list = split_string(brokers, strlen(brokers), ",", 1, &conf->broker_count);
    }
    if (client_id) {
        conf->client_id = strdup(client_id);
    } else {
        conf->client_id = strdup("kafka-cat-client");
    }
}

static void deinit_conf() {
    if (conf->broker_list) {
        free_split_res(conf->broker_list, conf->broker_count);
    }
    free(conf->client_id);
    free(conf);
}

struct client_config *get_conf() {
    return conf;
}

struct metadata_cache *get_metacache() {
     return cache;
 }

static void usage(const char *prog_name) {
    fprintf(stderr, "Usage: %s\n", prog_name);
    fprintf(stderr, "\t-b broker list, like localhost:9092.\n");
    fprintf(stderr, "\t-t topic name.\n");
    fprintf(stderr, "\t-c client id.\n");
    fprintf(stderr, "\t-C consumer mode.\n");
    fprintf(stderr, "\t-p partition id.\n");
    fprintf(stderr, "\t-P producer mode.\n");
    fprintf(stderr, "\t-o consumer offset.\n");
    fprintf(stderr, "\t-f consumer fetch size.\n");
    fprintf(stderr, "\t-k produce message key.\n");
    fprintf(stderr, "\t-v produce message value.\n");
    fprintf(stderr, "\t-l loglevel debug, info, warn, error .\n");
    fprintf(stderr, "\t-h help.\n");
}

void sig_handler(int signo)
{
    signal(SIGPIPE, SIG_IGN);
}

int main(int argc, char **argv) {
    int ch, part_id = 0, offset = 0, is_consumer = 0, is_producer = 0;
    int fetch_size = 0, show_usage = 0, ret;
    char *topic = NULL, *key = NULL, *value = NULL;
    char *brokers = NULL;
    char *client_id = NULL;
    char *log_level = NULL;

    srand(time(0));
    while((ch = getopt(argc, argv, "b:t:c:Cp:Po:f:k:v:l:h")) != -1) {
        switch(ch) {
            case 'b': brokers = strdup(optarg); break;
            case 't': topic = strdup(optarg); break;
            case 'c': client_id = strdup(optarg); break;
            case 'C': is_consumer = 1; break;
            case 'P': is_producer = 1; break;
            case 'p': part_id = atoi(optarg); break;
            case 'o': offset = atoi(optarg); break;
            case 'f': fetch_size = atoi(optarg); break;
            case 'k': key = strdup(optarg); break;
            case 'v': value = strdup(optarg); break;
            case 'l': log_level = strdup(optarg); break;
            case 'h': show_usage = 1; break;
        }
    }
   
    if(show_usage || argc == 1) {
        usage(argv[0]);
        exit(0);
    }
    if (!brokers) {
        logger(ERROR, "You shoud use -b to assign broker list.\n");
    }
    if(!topic) {
        logger(ERROR, "You shoud use -t to assign topic.\n");
    }
    if (is_producer && !value) {
        logger(ERROR, "You shoud use -v to assign value when mode is producer.\n");
    }
    if(is_producer && !key) {
        key = strdup("test_key");
    }

    init_conf(client_id, brokers);
    cache = alloc_metadata_cache();
    set_log_level(INFO);
    if (log_level) {
        set_loglevel_by_string(log_level);
    }
    if (signal(SIGPIPE, sig_handler) == SIG_ERR) {
        logger(ERROR, "can't catch SIGPIPE.");
    }
    if (fetch_size <= 0) fetch_size = 1024;
    if (is_consumer) {
        TIME_START();
        ret = send_fetch_request(topic, part_id, offset, fetch_size);
        TIME_END();
        logger(INFO, "Total time cost %lldus in fetch requst", TIME_COST());
    } else if(is_producer) {
        TIME_START();
        ret = send_produce_request(topic, part_id, key, value);
        TIME_END();
        logger(INFO, "Total time cost %lldus in produce requst", TIME_COST());
    } else {
        TIME_START();
        ret = send_metadata_request(topic, 1);
        TIME_END();
        logger(INFO, "Total time cost %lldus in fetch metadata requst", TIME_COST());
    }

    if (ret == K_OK) {
        logger(INFO, "Send request success!\n");
    } else {
        logger(INFO, "Send request failed!\n");
    }
    if (brokers) free(brokers);
    if (topic) free(topic);
    if (key) free(key);
    if (value) free(value);
    if (log_level) free(log_level);

    deinit_conf();
    dealloc_metadata_cache(cache);
    return 0;
}
