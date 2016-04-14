#ifndef _UTIL_H_
#define _UTIL_H_

#include <time.h>
#include <sys/time.h>

#define C_RED "\033[31m"
#define C_GREEN "\033[32m"
#define C_YELLOW "\033[33m"
#define C_PURPLE "\033[35m"
#define C_NONE "\033[0m"
#define TIME_START() struct timeval _tv_start, _tv_end; do { \
    gettimeofday(&_tv_start, NULL); \
} while(0);

#define TIME_END() \
do { \
    gettimeofday(&_tv_end, NULL); \
} while(0);

#define TIME_COST() ((_tv_end.tv_sec - _tv_start.tv_sec) * 1000000 + (_tv_end.tv_usec - _tv_start.tv_usec))
 

enum LEVEL {
    DEBUG = 1,
    INFO,
    WARN,
    ERROR
}; 

void logger(enum LEVEL loglevel, char *fmt, ...);
void set_log_file(char *filename);
void set_log_level(enum LEVEL level);
void set_loglevel_by_string(const char *level); 

char **split_string(const char *s, int len, const char *sep, int seplen, int *count);
void free_split_res(char **tokens, int count); 
#endif
