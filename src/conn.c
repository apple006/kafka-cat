#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/select.h>
#include "conn.h"

static int set_sock_flags(int fd, int flag) {
    long flags;
    flags = fcntl(fd, F_GETFL, NULL);
    if (flags < 0) {
        printf("Error fcntl(..., F_GETFL) (%s)\n", strerror(errno));
        return -1;
    }
    flags |= flag;     
    fcntl(fd, F_SETFL,flags);

    return 0;
}

int wait_socket_data(int fd, int timeout, RW_MODE rw) {
    int rc = -1;
    fd_set fdset;
    struct timeval tv;

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

    FD_ZERO(&fdset);
    FD_SET(fd, &fdset);
    return rc;
}

int connect_server(char *ip, int port) {
    int sockfd, rc;
    struct sockaddr_in srv_addr;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        return -1;
    }
    memset(&srv_addr, 0, sizeof(struct sockaddr_in));
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &srv_addr.sin_addr) <= 0) {
        return -1;
    }

    set_sock_flags(sockfd, O_NDELAY);
    set_sock_flags(sockfd, O_NONBLOCK);
    rc = connect(sockfd, (struct sockaddr*)&srv_addr, sizeof(srv_addr));
    if ((rc == -1) && (errno != EINPROGRESS)) {
        fprintf(stderr, "Error: %s\n", strerror(errno));
        close(sockfd);
        return -1;
    }
    rc = wait_socket_data(sockfd, 3000, CR_WRITE);
    if (rc == -1) {
        goto cleanup;
    } else if (rc == 0) {
        printf("timeout\n");
        goto cleanup;
    }
    return sockfd;

cleanup:
    close(sockfd);
    return -1;
}
