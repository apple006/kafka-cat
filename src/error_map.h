#ifndef _ERROR_MAP_H_
#define _ERROR_MAP_H_
const char *err_map[] = {
    "no error", /* 0 */
    "offset out of range", /* 1 */
    "invaild message", /* 2 */
    "unknown topic or partition", /* 3 */
    "invaild fetch size", /* 4 */
    "leader not available", /* 5 */
    "not leader for partition", /* 6 */
    "request timeout", /* 7 */
    "broker not available", /* 8 */
    "replica not available", /* 9 */
    "message size too large", /* 10 */
    "stale controller epoch", /* 11 */
    "offset metadata too large", /* 12 */
    "stale leader epoch", /* 13 */
    "offsets load in progress", /* 14 */
    "consumer coordinator not available", /* 15 */
    "not coordinator for consumer", /* 16 */
    "invalid topic", /* 17 */
    "message set size too large", /* 18 */
    "not enough replicas", /* 19 */
    "not enough replicas after append", /* 20 */
};
#endif
