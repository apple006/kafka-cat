#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "crc32.h"
#include "buffer.h"

struct buffer* alloc_buffer(int init_size) {
    if (init_size <= 0) init_size = 16;
    struct buffer *buf = malloc(sizeof(struct buffer));
    if (!buf) return NULL;

    buf->cap = init_size;
    buf->used = 0;
    buf->pos = 0;
    buf->data = malloc(init_size);
    if(!buf->data) {
        free(buf);
        return NULL;
    }

    return buf;
}

struct buffer * alloc_buffer_with_init(const char *source, int size) {
    struct buffer *buf = alloc_buffer(size);
    memcpy(buf->data, source, size);
    buf->used += size;
    return buf;
}

void dealloc_buffer(struct buffer*buf) {
    if (!buf) return;

    if (buf->data) free(buf->data);
    free(buf);
}

void need_expand(struct buffer *buf, int need_bytes) {
    int new_size;

    if (need_bytes <= 0) return;
    // remain bytes is enough.
    if (buf->cap - buf->used >= need_bytes) return;
    new_size = buf->cap + (need_bytes + buf->cap - buf->used); 
    if (buf->cap * 2 > new_size) new_size = buf->cap * 2;
    buf->data = realloc(buf->data, new_size);  
    buf->cap = new_size;
}

int8_t read_int8_buffer(struct buffer *buf) {
    int8_t i8;
    assert(buf->pos + 1 <= buf->used);

    i8 = (int8_t) buf->data[buf->pos++];
    return i8;
}

int16_t read_int16_buffer(struct buffer *buf) {
    int16_t i16 = 0;
    assert(buf->pos + 2 <= buf->used);

    i16 |= buf->data[buf->pos++] << 8;
    i16 |= buf->data[buf->pos++] & 0xff;
    return i16;
}

int32_t read_int32_buffer(struct buffer *buf) {
    int32_t i32 = 0;
    assert(buf->pos + 4 <= buf->used);

    i32 |= buf->data[buf->pos++] << 24;
    i32 |= buf->data[buf->pos++] << 16;
    i32 |= buf->data[buf->pos++] << 8;
    i32 |= buf->data[buf->pos++] & 0xff;
    return i32;
}

int64_t read_int64_buffer(struct buffer *buf) {
    int i;
    int64_t i64 = 0;
    assert(buf->pos + 8 <= buf->used);

    for (i = 0; i < 7; i++) {
        i64 |= buf->data[buf->pos++] << ((7-i)*8);
    }
    i64 |= buf->data[buf->pos++] & 0xff;
    return i64;
}

// NOTE: you should make sure dst space is enough to carray data.
int read_raw_string_buffer(struct buffer *buf, char *dst, int size) {
    if (size <= 0 || !dst || buf->pos + size > buf->used) return -1;

    memcpy(dst, buf->data + buf->pos, size);
    buf->pos += size;

    return 0;
}

// assume string never contain '\0'
char *read_short_string_buffer(struct buffer *buf) {
    int16_t size = read_int16_buffer(buf);
    if (size <= 0) return NULL;
    
    assert(buf->pos + size <= buf->used);
    char *str = malloc(size + 1);
    if (!str) return NULL;
    memcpy(str, buf->data+buf->pos, size);
    str[size] = '\0';
    buf->pos += size;

    return str;
}

void write_int8_buffer(struct buffer *buf, int8_t i8) {
    char *start;

    need_expand(buf, 1);
    start = &buf->data[buf->used];
    start[0] = i8;
    ++buf->used;
}

void write_int16_buffer(struct buffer *buf, int16_t i16) {
    char *start;

    need_expand(buf, 2);
    start = &buf->data[buf->used];
    start[0] = i16 >> 8;
    start[1] = i16 & 0xff;
    buf->used += 2 ;
}

void write_int32_buffer(struct buffer *buf, int32_t i32) {
    char *start;

    need_expand(buf, 4);
    start = &buf->data[buf->used];
    start[0] = i32 >> 24;
    start[1] = i32 >> 16;
    start[2] = i32 >> 8;
    start[3] = i32 & 0xff;
    buf->used += 4;
}

void write_int64_buffer(struct buffer *buf, int64_t i64) {
    int i;
    char *start;

    need_expand(buf, 8);
    start = &buf->data[buf->used];
    for (i = 0; i < 7; i++) {
        start[i] = i64 >> ((7-i)*8);
    }
    start[7] = i64 & 0xff;
    buf->used += 8;
}

void write_raw_string_buffer(struct buffer *buf, const char *str, int size) {
    if (!str || size <= 0) return;

    need_expand(buf, size);
    memcpy(&buf->data[buf->used], str, size);
    buf->used += size;
}

void write_string_buffer(struct buffer *buf, const char *str) {
    int size;

    size = str ? strlen(str) : 0;
    if (!str || size <= 0) {
        write_int32_buffer(buf, -1);
        return;
    }

    write_int32_buffer(buf, size);
    need_expand(buf, size);
    memcpy(&buf->data[buf->used], str, size);
    buf->used += size;
}

void write_short_string_buffer(struct buffer *buf, const char *str, int16_t size) {
    if(size == 0) return;

    write_int16_buffer(buf, size);
    need_expand(buf, size);
    memcpy(&buf->data[buf->used], str, size);
    buf->used += size;
}

int get_buffer_cap(struct buffer *buf) {
    return buf->cap;
}

int get_buffer_used(struct buffer *buf) {
    return buf->used;
}

int skip_buffer_bytes(struct buffer *buf, int bytes) { 
    if (buf->pos + bytes <= buf->used) {
        buf->pos += bytes;
        return 0;
    }
    return -1;
}

int incr_buffer_used(struct buffer *buf, int size) { 
    if (buf->used + size <= buf->cap) {
        buf->used += size;
        return 0;
    }
    return -1;
}

char *get_buffer_data(struct buffer *buf) {
    return buf->data;
}

int is_buffer_eof(struct buffer *buf) {
    return buf->pos >= buf->used;
}

int get_buffer_unread(struct buffer *buf) {
    return buf->used - buf->pos;
}

uint32_t get_buffer_crc32(struct buffer *buf) {
    return crc32(0, buf->data, buf->used);
}
