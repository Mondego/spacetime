#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <winsock2.h>
typedef int socklen_t;
#define close closesocket
#pragma comment(lib, "Ws2_32.lib")
#else
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#endif
#include <sys/types.h>
#ifdef __EMSCRIPTEN__
#include <emscripten.h>
#endif

#include <connectors/server_socket.h>
#include <connectors/test_sockets_msg.h>

int server_socket::accept_req(int accept_type)
{
    int res;
    fd_set fdr;
    fd_set fdw;

    // see if there are any connections to accept or read / write from
    FD_ZERO(&fdr);
    FD_ZERO(&fdw);
    FD_SET(server.fd, &fdr);
    FD_SET(server.fd, &fdw);
#if !TEST_DGRAM
    if (client.fd)
        FD_SET(client.fd, &fdr);
    if (client.fd)
        FD_SET(client.fd, &fdw);
#endif
    res = select(64, &fdr, &fdw, NULL, NULL);
    if (res == -1)
    {
        perror("select failed");
        exit(EXIT_SUCCESS);
    }

#if !TEST_DGRAM
    // for TCP sockets, we may need to accept a connection
    if (FD_ISSET(server.fd, &fdr))
    {
#if TEST_ACCEPT_ADDR
        // Do an accept with non-NULL addr and addlen parameters. This tests a fix to a bug in the implementation of
        // accept which had a parameter "addrp" but used "addr" internally if addrp was set - giving a ReferenceError.
        struct sockaddr_in addr = {0};
        addr.sin_family = AF_INET;

        socklen_t addrlen = sizeof(addr);
        client.fd = accept(server.fd, (struct sockaddr *)&addr, &addrlen);
#else
        client.fd = accept(server.fd, NULL, NULL);
#endif
        assert(client.fd != -1);
    }
#endif
    int fd = client.fd;
    if (accept_type == 0)
    {
        // read
        if (!FD_ISSET(fd, &fdr))
            return -1;
    }
    else
    {
        //write
        if (!FD_ISSET(fd, &fdw))
            return -1;
    }
    return client.fd;
}

int server_socket::read_socket(msg_t *read_msg)
{
    int fd = accept_req(0);
    int res;
    if (fd == -1)
    {
        return -1;
    }
    if (client.state == MSG_READ)
    {

        socklen_t addrlen = sizeof(client.addr);
        res = do_msg_read(fd, &client.msg, client.read, 0, (struct sockaddr *)&client.addr, &addrlen);
        if (res == -1)
        {
            return -1;
        }
        else if (res == 0)
        {
            // client disconnected
            memset(&client, 0, sizeof(client_t));
            return -1;
        }

        client.read += res;

        // once we've read the entire message, echo it back
        if (client.read >= client.msg.length)
        {
            client.read = 0;
            client.state = MSG_WRITE;
        }
    }
}

int server_socket::write_socket(msg_t *write_msg)
{
    int res;
    int fd = accept_req(1);
    if (fd == -1)
    {
        return -1;
    }

    if (client.state == MSG_WRITE)
    {
        res = do_msg_write(fd, &client.msg, client.wrote, 0, (struct sockaddr *)&client.addr, sizeof(client.addr));
        if (res == -1)
        {
            return -1;
        }
        else if (res == 0)
        {
            // client disconnected
            memset(&client, 0, sizeof(client_t));
            return -1;
        }

        client.wrote += res;

        if (client.wrote >= client.msg.length)
        {
            client.wrote = 0;
            client.state = MSG_READ;
        }
    }
}

server_socket::server_socket(std::string address, int port)
{
#ifdef _WIN32
    WSADATA wsaData = {};
    int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (result)
    {
        printf("WSAStartup failed!\n");
        exit(1);
    }
#endif

    struct sockaddr_in addr;
    int res;

    // move to someplace else
    // atexit(cleanup);
    // signal(SIGTERM, cleanup2);

    memset(&server, 0, sizeof(server_t));
    memset(&client, 0, sizeof(client_t));

    // create the socket and set to non-blocking
#if !TEST_DGRAM
    server.fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
    server.fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
#endif
    if (server.fd == -1)
    {
        perror("cannot create socket");
        exit(EXIT_FAILURE);
    }
#ifdef _WIN32
    unsigned long nonblocking = 1;
    ioctlsocket(server.fd, FIONBIO, &nonblocking);
#else
    fcntl(server.fd, F_SETFL, O_NONBLOCK);
#endif

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, &address[0], &addr.sin_addr) != 1)
    {
        perror("inet_pton failed");
        exit(EXIT_FAILURE);
    }

    res = bind(server.fd, (struct sockaddr *)&addr, sizeof(addr));
    if (res == -1)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

#if !TEST_DGRAM
    res = listen(server.fd, 500);
    if (res == -1)
    {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }
#endif

#ifdef __EMSCRIPTEN__
#if TEST_ASYNC
    // The first parameter being passed is actually an arbitrary userData pointer
    // for simplicity this test just passes a basic char*
    emscripten_set_socket_connection_callback("connection", async_main_loop);
    emscripten_set_socket_message_callback("message", async_main_loop);
    emscripten_set_socket_close_callback("close", async_main_loop);
#else
    emscripten_set_main_loop(main_loop, 60, 0);
#endif
#endif
}

server_socket::~server_socket()
{
    if (client.fd)
    {
        close(client.fd);
        client.fd = 0;
    }
    if (server.fd)
    {
        close(server.fd);
        server.fd = 0;
    }
}