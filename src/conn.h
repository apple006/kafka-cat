#ifndef _CONN_H_
#define _CONN_H_
typedef enum {
    CR_READ = 1,
    CR_WRITE = 2,
    CR_RW = 4
} RW_MODE;

int connect_server(char *ip, int port);
int wait_socket_data(int fd, int timeout, RW_MODE rw);
#endif
