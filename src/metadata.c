#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "metadata.h"

struct metadata_cache *alloc_metadata_cache() {
    struct metadata_cache *cache;
    cache = malloc(sizeof(*cache));

    cache->broker_count = 0;
    cache->broker_metas = NULL;
    cache->topic_count = 0;
    cache->topic_metas = NULL;
    return cache;
}

void dealloc_metadata_cache(struct metadata_cache *cache) {
    int i;
    struct broker_metadata *b_meta;
    struct topic_metadata *cur, *next;

    for (i = 0; i < cache->broker_count; i++) {
        b_meta = &cache->broker_metas[i];
        if (b_meta->host) free(b_meta->host);
    }
    free(cache->broker_metas);
    
    cur = cache->topic_metas;
    while(cur) {
        next = cur->next;
        dealloc_topic_metadata(cur);
        cur = next;
    }

    free(cache);
}

struct broker_metadata *get_broker_metadata(struct metadata_cache *cache, int id) {
    int i;

    if (cache->broker_count <= 0) return NULL;
    for (i = 0; i < cache->broker_count; i++) {
        if (cache->broker_metas[i].id == id) {
            return &cache->broker_metas[i]; 
        }
    }

    return NULL;
}

int update_broker_metadata(struct metadata_cache *cache, int broker_count) {
    int i;
    struct broker_metadata *broker_metas;

    broker_metas = malloc(broker_count * sizeof(struct broker_metadata));
    if (!broker_metas) return -1;
    if (cache->broker_metas) {
        for (i = 0; i < cache->broker_count; i++) {
            free(cache->broker_metas[i].host);
        }
        free(cache->broker_metas);
    }

    cache->broker_metas = broker_metas;
    cache->broker_count = broker_count;
    return 0;
}

struct topic_metadata *get_topic_metadata_from_cache(struct metadata_cache *cache, const char *topic) {
    struct topic_metadata *cur;

    if (!cache || cache->topic_count == 0 || !topic) return NULL;
    cur = cache->topic_metas;
    while(cur) {
        if (strlen(cur->topic) == strlen(topic)
                && !strcmp(cur->topic, topic)) {
            return cur;
        }
        cur = cur->next;
    }
    return NULL;
}

int delete_topic_metadata_from_cache(struct metadata_cache *cache, const char *topic) {
    struct topic_metadata *t_meta;

    t_meta = get_topic_metadata_from_cache(cache, topic);
    if (!t_meta) return -1;

    if (t_meta == cache->topic_metas) {
        cache->topic_metas = t_meta->next;
        if (cache->topic_metas) cache->topic_metas->prev = NULL;
    } else {
        t_meta->prev->next = t_meta->next;
        t_meta->next->prev = t_meta->prev;
    }
    dealloc_topic_metadata(t_meta);
    cache->topic_count--;
    return 0;
}

struct partition_metadata* alloc_partition_metadata() {
    struct partition_metadata* p_meta = malloc(sizeof(*p_meta));
    p_meta->part_id = -1;
    p_meta->leader_id = -1;
    p_meta->replica_count = 0;
    p_meta->isr_count = 0;
    p_meta->replicas = NULL; 
    p_meta->isr = NULL; 
    return p_meta;
}

void dealloc_partition_metadata(struct partition_metadata* p_meta) {
    if (p_meta->replicas) free(p_meta->replicas);
    if (p_meta->isr) free(p_meta->isr);
    free(p_meta);
}

struct topic_metadata *alloc_topic_metadata(int partitions) {
    struct topic_metadata *t_meta = malloc(sizeof(*t_meta));
    if (!t_meta) return NULL;

    t_meta->topic = NULL; 
    t_meta->partitions = partitions;
    t_meta->part_metas = malloc(partitions * sizeof(struct partition_metadata*));
    if (!t_meta->part_metas) {
        free(t_meta);
        return NULL;
    }
    t_meta->next = NULL;
    t_meta->prev = NULL;

    return t_meta;
}

void dealloc_topic_metadata(struct topic_metadata *t_meta) {
    int i;
    if (t_meta->topic) free(t_meta->topic);
    for (i = 0; i < t_meta->partitions; i++) {
        dealloc_partition_metadata(t_meta->part_metas[i]);
    }
    if (t_meta->part_metas) free(t_meta->part_metas);
    free(t_meta);
}

struct topic_metadata *add_topic_metadata_to_cache(struct metadata_cache *cache, const char *topic, int partitions) {
    if (!cache || !topic) return NULL;

    struct topic_metadata *t_meta;
    // already exists.
    if ((t_meta = get_topic_metadata_from_cache(cache, topic))) return t_meta;

    t_meta = alloc_topic_metadata(partitions);
    t_meta->topic = strdup(topic);
    if (cache->topic_metas) {
        t_meta->next = cache->topic_metas;
        cache->topic_metas->prev = t_meta;
    }
    cache->topic_metas = t_meta;
    cache->topic_count++;

    return t_meta;
}

void dump_topic_metadata(struct topic_metadata *t_meta) {
    int i, j;
    if (!t_meta) return;

    struct partition_metadata *p_meta;

    printf("{ topic = %s, partitions = %d, info = [\n", t_meta->topic, t_meta->partitions);
    for ( i = 0; i < t_meta->partitions; i++) {
        p_meta = t_meta->part_metas[i];
        printf("[ part_id = %d, leader_id = %d, replicas = [", p_meta->part_id, p_meta->leader_id);
        for (j = 0; j < p_meta->replica_count; j++) {
            if (j != p_meta->replica_count - 1) {
                printf("%d,", p_meta->replicas[j]);
            } else {
                printf("%d", p_meta->replicas[j]);
            }
        }
        printf("], isr = ["); 
        for(j = 0; j < p_meta->isr_count; j++)  {
            if (j != p_meta->isr_count - 1) {
                printf("%d,", p_meta->isr[j]);
            } else {
                printf("%d", p_meta->isr[j]);
            }
        }
        printf("]\n");
    }
    printf("]}\n");
}
