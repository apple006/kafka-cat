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

    if (!cache) return;
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

int update_broker_metadata(struct metadata_cache *cache, 
        int count, struct broker_metadata *new_metas) {
    int i;

    if (cache->broker_metas) {
        for (i = 0; i < cache->broker_count; i++) {
            free(cache->broker_metas[i].host);
        }
        free(cache->broker_metas);
    }

    cache->broker_count = count;
    cache->broker_metas = malloc(count * sizeof(struct broker_metadata));
    if (!cache->broker_metas) return -1;

    for (i = 0; i < count; i++) {
        cache->broker_metas[i].id = new_metas[i].id;
        cache->broker_metas[i].host = strdup(new_metas[i].host);
        cache->broker_metas[i].port = new_metas[i].port;
    }
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
    if (!p_meta) return;
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

    if (!t_meta) return;
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

int update_topic_metadata(struct metadata_cache *cache, struct topic_metadata *t_meta) {
    int i, isr_count, replica_count;
    struct topic_metadata *new_meta;

    delete_topic_metadata_from_cache(cache, t_meta->topic);
    new_meta = add_topic_metadata_to_cache(cache, t_meta->topic, t_meta->partitions);
    new_meta->part_metas= malloc(t_meta->partitions * sizeof(void*));
    if (!new_meta->part_metas) goto cleanup; 

    for (i = 0; i < t_meta->partitions; i++) {
        new_meta->part_metas[i] = alloc_partition_metadata();
        new_meta->part_metas[i]->err_code = t_meta->part_metas[i]->err_code;
        new_meta->part_metas[i]->part_id = t_meta->part_metas[i]->part_id;
        new_meta->part_metas[i]->leader_id = t_meta->part_metas[i]->leader_id;
        isr_count = t_meta->part_metas[i]->isr_count;
        new_meta->part_metas[i]->isr_count = isr_count;
        replica_count = t_meta->part_metas[i]->replica_count;
        new_meta->part_metas[i]->replica_count = replica_count;
        new_meta->part_metas[i]->isr = malloc(isr_count * sizeof(int));
        if (!new_meta->part_metas[i]->isr) goto cleanup; 
        new_meta->part_metas[i]->replicas = malloc(replica_count * sizeof(int));
        if (!new_meta->part_metas[i]->replicas) {
            free(new_meta->part_metas[i]->isr);
            goto cleanup;
        }

        memcpy(new_meta->part_metas[i]->isr, t_meta->part_metas[i]->isr, isr_count);
        memcpy(new_meta->part_metas[i]->replicas, t_meta->part_metas[i]->replicas, replica_count);
    }
    return 0;

cleanup:
    delete_topic_metadata_from_cache(cache, t_meta->topic);
    return -1;
}
