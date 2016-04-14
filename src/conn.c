#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>
#include "util.h"
#include "conn.h"

static int set_sock_flags(int fd, int flag) {
    long flags;
    flags = fcntl(fd, F_GETFL, NULL);
    if (flags < 0) {
        logger(INFO, "fcntl(..., F_GETFL) (%s)!\n", strerror(errno));
        return -1;
    }
    flags |= flag;     
    fcntl(fd, F_SETFL,flags);

    return 0;
}

int wait_socket_data(int fd, int timeout, RW_MODE rw) {
    fd_set fdset;
    socklen_t lon;
    struct timeval tv;
    int rc = -1, val_opt;

    tv.tv_sec = timeout / 1000; 
    tv.tv_usec = (timeout % 1000) * 1000;
    FD_ZERO(&fdset);
    FD_SET(fd, &fdset);

    if (rw == CR_READ) {
        // read
        rc = select(fd + 1, &fdset, NULL, NULL, &tv);
    } else if (rw == CR_WRITE) {
        // write
        rc = select(fd + 1, NULL, &fdset, NULL, &tv);
    } else if(rw == CR_RW) {
        rc = select(fd + 1, &fdset, &fdset, NULL, &tv);
    }

    if (rc > 0) {
        lon = sizeof(int); 
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (void*)(&val_opt), &lon) < 0) { 
            logger(INFO, "getsockopt() error as %s", strerror(errno)); 
            return -1;
        } 
        if (val_opt) { 
            logger(INFO, "connct error as %s", strerror(val_opt));
            return -1;
        } 
    }

    FD_ZERO(&fdset);
    FD_SET(fd, &fdset);
    return rc;
}

int connect_server(char *ip, int port) {
    int sockfd, rc;
    struct sockaddr_in srv_addr;
    
    TIME_START();
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        logger(DEBUG, "make socket() [%s:%d] error!", ip, port);
        return -1;
    }
    memset(&srv_addr, 0, sizeof(struct sockaddr_in));
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &srv_addr.sin_addr) <= 0) {
        logger(DEBUG, "inet_pton() [%s:%d] error!", ip, port);
        return -1;
    }

    set_sock_flags(sockfd, O_NDELAY);
    set_sock_flags(sockfd, O_NONBLOCK);
    rc = connect(sockfd, (struct sockaddr*)&srv_addr, sizeof(srv_addr));
    if ((rc == -1) && (errno != EINPROGRESS)) {
        logger(DEBUG, "connect server[%s:%d] error!", ip, port);
        close(sockfd);
        return -1;
    }
    rc = wait_socket_data(sockfd, 3000, CR_WRITE);
    if (rc == -1) {
        logger(DEBUG, "connect server[%s:%d] error!", ip, port);
        goto cleanup;
    } else if (rc == 0) {
        logger(DEBUG, "connect server[%s:%d] timeout!", ip, port);
        goto cleanup;
    }
    TIME_END();
    logger(DEBUG, "Total time cost %lldus in connect to server[%s:%d].", TIME_COST(), ip, port);
    return sockfd;

cleanup:
    close(sockfd);
    return -1;
}
