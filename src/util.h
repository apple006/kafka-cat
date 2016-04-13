#ifndef _UTIL_H_
#define _UTIL_H_
char **split_string(const char *s, int len, const char *sep, int seplen, int *count);
void free_split_res(char **tokens, int count); 
#endif
