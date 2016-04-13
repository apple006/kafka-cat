#ifndef _BUFFER_H_
#define _BUFFER_H_
#include <unistd.h>

struct buffer {
    int cap;
    int pos; // read pos
    int used;
    char *data;
};

struct buffer* alloc_buffer(int init_size);
struct buffer * alloc_buffer_with_init(const char *source, int size);
void dealloc_buffer(struct buffer*buf);
int8_t read_int8_buffer(struct buffer *buf);
void write_int8_buffer(struct buffer *buf, int8_t i8);
int16_t read_int16_buffer(struct buffer *buf);
void write_int16_buffer(struct buffer *buf, int16_t i16);
int32_t read_int32_buffer(struct buffer *buf);
void write_int32_buffer(struct buffer *buf, int32_t i32); 
int64_t read_int64_buffer(struct buffer *buf);
void write_int64_buffer(struct buffer *buf, int64_t i64);
int read_raw_string_buffer(struct buffer *buf, char *dst, int size);
void write_raw_string_buffer(struct buffer *buf, const char *str, int size); 
void write_string_buffer(struct buffer *buf, const char *str);
char *read_short_string_buffer(struct buffer *buf);
void write_short_string_buffer(struct buffer *buf, const char *str, int16_t size);
int get_buffer_cap(struct buffer *buf);
int get_buffer_used(struct buffer *buf);
int incr_buffer_used(struct buffer *buf, int size); 
int is_buffer_eof(struct buffer *buf);
int skip_buffer_bytes(struct buffer *buf, int bytes); 
char *get_buffer_data(struct buffer *buf); 
void need_expand(struct buffer *buf, int need_bytes); 
uint32_t get_buffer_crc32(struct buffer *buf);
int get_buffer_unread(struct buffer *buf); 
#endif
