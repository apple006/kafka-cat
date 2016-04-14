#ifndef _METADATA_H_
#define _METADATA_H_
struct partition_metadata {
    int err_code;
    int part_id;
    int leader_id;
    int replica_count;
    int isr_count;
    int *replicas;
    int *isr;
};

struct broker_metadata {
    int id;
    int port;
    char *host;
};

struct topic_metadata {
    char *topic;
    int partitions;
    struct partition_metadata **part_metas;
    struct topic_metadata *prev;
    struct topic_metadata *next;
};

struct metadata_cache {
    int broker_count;
    struct broker_metadata *broker_metas;
    int topic_count;
    struct topic_metadata *topic_metas;
};

struct metadata_cache *alloc_metadata_cache();
void dealloc_metadata_cache(struct metadata_cache *cache);
int update_broker_metadata(struct metadata_cache *cache, int broker_count);
struct broker_metadata *get_broker_metadata(struct metadata_cache *cache, int id); 
struct topic_metadata *get_topic_metadata_from_cache(struct metadata_cache *cache, const char *topic);
int delete_topic_metadata_from_cache(struct metadata_cache *cache, const char *topic);
struct partition_metadata* alloc_partition_metadata(); 
void dealloc_partition_metadata(struct partition_metadata* p_meta); 
struct topic_metadata *alloc_topic_metadata(int partitions);
void dealloc_topic_metadata(struct topic_metadata *t_meta); 
struct topic_metadata *add_topic_metadata_to_cache(struct metadata_cache *cache, const char *topic, int partitions); 
void dump_topic_metadata(struct topic_metadata *t_meta);
#endif
