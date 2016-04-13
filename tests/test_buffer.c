#include <stdlib.h>
#include "ctest.h"
#include "buffer.h" 

CTEST(buffer, alloc_buffer) {
    struct buffer* buf;

    buf = alloc_buffer(0);
    ASSERT_EQUAL(16, buf->cap);
    ASSERT_EQUAL(0, buf->used);
    ASSERT_NOT_NULL(buf->data);
    dealloc_buffer(buf);

    buf = alloc_buffer(128);
    ASSERT_EQUAL(128, buf->cap);
    ASSERT_EQUAL(0, buf->used);
    ASSERT_NOT_NULL(buf->data);
    dealloc_buffer(buf);
    
}

CTEST(buffer, write_int8_buffer) {
    struct buffer* buf;
    int8_t i8 = 0xde;

    buf = alloc_buffer(16);
    write_int8_buffer(buf, i8);
    ASSERT_EQUAL(1, buf->used);
    ASSERT_EQUAL(i8, buf->data[0]);
    dealloc_buffer(buf);
}

CTEST(buffer, write_int16_buffer) {
    struct buffer* buf;
    int16_t i16 = 0xdece;

    buf = alloc_buffer(16);
    write_int16_buffer(buf, i16);
    ASSERT_EQUAL(2, buf->used);
    ASSERT_EQUAL(0xde, (uint8_t)buf->data[0]);
    ASSERT_EQUAL(0xce, (uint8_t)buf->data[1]);
    dealloc_buffer(buf);
}

CTEST(buffer, write_int32_buffer) {
    struct buffer* buf;
    int32_t i32 = 0xdecebeae;

    buf = alloc_buffer(16);
    write_int32_buffer(buf, i32);
    ASSERT_EQUAL(4, buf->used);
    ASSERT_EQUAL(0xde, (uint8_t)buf->data[0]);
    ASSERT_EQUAL(0xce, (uint8_t)buf->data[1]);
    ASSERT_EQUAL(0xbe, (uint8_t)buf->data[2]);
    ASSERT_EQUAL(0xae, (uint8_t)buf->data[3]);
    dealloc_buffer(buf);
}

CTEST(buffer, write_int64_buffer) {
    struct buffer* buf;
    int64_t i64 = 0xdecebeaedecebeae;

    buf = alloc_buffer(16);
    write_int64_buffer(buf, i64);
    ASSERT_EQUAL(8, buf->used);
    ASSERT_EQUAL(0xde, (uint8_t)buf->data[0]);
    ASSERT_EQUAL(0xce, (uint8_t)buf->data[1]);
    ASSERT_EQUAL(0xbe, (uint8_t)buf->data[2]);
    ASSERT_EQUAL(0xae, (uint8_t)buf->data[3]);
    ASSERT_EQUAL(0xde, (uint8_t)buf->data[4]);
    ASSERT_EQUAL(0xce, (uint8_t)buf->data[5]);
    ASSERT_EQUAL(0xbe, (uint8_t)buf->data[6]);
    ASSERT_EQUAL(0xae, (uint8_t)buf->data[7]);
    dealloc_buffer(buf);
}
