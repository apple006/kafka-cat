#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "util.h"

static char *log_file = NULL;
static enum LEVEL log_level = INFO;

// This function was copied from redis/sds.c
char **split_string(const char *s, int len, const char *sep, int seplen, int *count) {
    int elements = 0, slots = 5, start = 0, j;
    char **tokens, *tmp;

    if (seplen < 1 || len < 0) return NULL;

    tokens = malloc(sizeof(char *)*slots);
    if (tokens == NULL) return NULL;

    if (len == 0) { 
        *count = 0; 
        return tokens;
    }    
    for (j = 0; j < (len-(seplen-1)); j++) {
        /* make sure there is room for the next element and the final one */
        if (slots < elements+2) {
            char **newtokens;

            slots *= 2;
            newtokens = realloc(tokens,sizeof(char *)*slots);
            if (newtokens == NULL) goto cleanup;
            tokens = newtokens;
        }    
        /* search the separator */
        if ((seplen == 1 && *(s+j) == sep[0]) || (memcmp(s+j,sep,seplen) == 0)) {
            if (j-start > 0) {
                tmp = malloc(j-start+1);
                memcpy(tmp, s+start, j-start);
                tmp[j-start] = '\0';
                tokens[elements] = tmp;
                if (tokens[elements] == NULL) goto cleanup;
                elements++;
            }
            start = j+seplen;
            j = j+seplen-1; /* skip the separator */
        }
    }
    /* Add the final element. We are sure there is room in the tokens array. */
    if (len - start > 0) {
        tmp = malloc(len-start+1);
        memcpy(tmp, s+start, len-start);
        tmp[len-start] = '\0';
        tokens[elements] = tmp;
        if (tokens[elements] == NULL) goto cleanup;
        elements++;
    }
    *count = elements;
    return tokens;

cleanup:
    {
        int i;
        for (i = 0; i < elements; i++) free(tokens[i]);
        free(tokens);
        *count = 0;
        return NULL;
    }
}

void free_split_res(char **tokens, int count) {
    if (!tokens) return;
    while(count--)
        free(tokens[count]);
    free(tokens);
}

void
set_log_level(enum LEVEL level) {
    log_level  = level;
}

void
set_log_file(char *filename)
{
    log_file = filename;
}

void
logger(enum LEVEL loglevel,char *fmt, ...)
{
    FILE *fp;
    va_list ap;
    time_t now;
    char buf[4096];
    char t_buf[64];
    char *msg = NULL;
    const char *color = "";

    if(loglevel < log_level) {
        return;
    }

    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    switch(loglevel) {
        case DEBUG: msg = "DEBUG"; break;
        case INFO:  msg = "INFO";  color = C_YELLOW ; break;
        case WARN:  msg = "WARN";  color = C_PURPLE; break;
        case ERROR: msg = "ERROR"; color = C_RED; break;
    }

    now = time(NULL);
    strftime(t_buf,64,"%Y-%m-%d %H:%M:%S",localtime(&now));
    fp = (log_file == NULL) ? stdout : fopen(log_file,"a");
    if(log_file) {
        fprintf(fp, "[%s] [%s] %s\n", t_buf, msg, buf);
        fclose(fp);
    } else {
        fprintf(fp, "%s[%s] [%s] %s"C_NONE"\n", color, t_buf, msg, buf);
    }

    if(loglevel >= ERROR) {
        exit(1);
    }
}
