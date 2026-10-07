/* Owned Linux test fixture: exit the leader while a worker retains the socket. */
#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/un.h>
#include <unistd.h>

static int listener;

static void *serve(void *ignored) {
    (void)ignored;
    for (;;) {
        int peer = accept(listener, NULL, NULL);
        if (peer >= 0) {
            close(peer);
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 2) {
        return 2;
    }
    struct sockaddr_un address = {.sun_family = AF_UNIX};
    if (strlen(argv[1]) >= sizeof(address.sun_path)) {
        return 3;
    }
    strcpy(address.sun_path, argv[1]);
    listener = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listener < 0 || bind(listener, (void *)&address, sizeof(address)) ||
        listen(listener, 128)) {
        return 4;
    }
    pthread_t worker;
    if (pthread_create(&worker, NULL, serve, NULL)) {
        return 5;
    }
    puts("ready");
    fflush(stdout);
    char command;
    if (read(STDIN_FILENO, &command, 1) != 1) {
        return 6;
    }
    /* SYS_exit terminates this thread; returning would terminate the group. */
    syscall(SYS_exit, 0);
    abort();
}
