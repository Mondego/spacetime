#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <assert.h>
#include <vector>
#ifdef __EMSCRIPTEN__
#include <emscripten.h>
#endif

#include "connectors/test_sockets_msg.h"
#include "connectors/client_socket.h"

// message to send to the server
#ifndef MESSAGE
#define MESSAGE "pingtothepong"
#endif

void client_socket::client_socket_h::finish()
{
    if (server.fd)
    {
        close(server.fd);
        server.fd = 0;
    }
#ifdef __EMSCRIPTEN__
#ifdef REPORT_RESULT
    REPORT_RESULT(result);
#endif
    emscripten_force_exit(0);
#else
    exit(-1);
#endif
}

int client_socket::client_socket_h::confirm_sock(std::string client_ip)
{
    static char out[1024 * 2];
    static int pos = 0;
    fd_set fdr;
    fd_set fdw;
    int res;

    // make sure that server.fd is ready to read / write
    FD_ZERO(&fdr);
    FD_ZERO(&fdw);
    FD_SET(server.fd, &fdr);
    FD_SET(server.fd, &fdw);
    res = select(64, &fdr, &fdw, NULL, NULL);
    if (res == -1)
    {
        perror("select failed");
        finish();
    }

    if (server.state == MSG_COMPLETE)
    {
        if (!FD_ISSET(server.fd, &fdw))
        {
            return -1;
        }
        memset(&echo_msg, 0, sizeof(msg_t));
        echo_msg.length = client_ip.length();
        echo_msg.buffer = (char *)malloc(echo_msg.length);
        memcpy(echo_msg.buffer, &client_ip[0], echo_msg.length);

        res = do_msg_write(server.fd, &echo_msg, echo_wrote, 0, NULL, 0);
        if (res == -1)
        {
            return -1;
        }
        else if (res == 0)
        {
            perror("server closed");
            finish();
        }

        echo_wrote += res;

        // once we're done writing the message, read it back
        if (echo_wrote >= echo_msg.length)
        {
            // std::cout << "Wrote the message" << std::endl;
            echo_wrote = 0;
            server.state = MSG_WRITE;
            return res;
        }
        return res;
    }
    return -1;
}
int client_socket::client_socket_h::read_sock()
{
    static char out[1024 * 2];
    static int pos = 0;
    fd_set fdr;
    fd_set fdw;
    int res;

    // make sure that server.fd is ready to read / write
    FD_ZERO(&fdr);
    FD_ZERO(&fdw);
    FD_SET(server.fd, &fdr);
    FD_SET(server.fd, &fdw);
    res = select(64, &fdr, &fdw, NULL, NULL);
    if (res == -1)
    {
        perror("select failed");
        finish();
    }

    if (server.state == MSG_READ)
    {
        if (!FD_ISSET(server.fd, &fdr))
        {
            return -1;
        }

#if !TEST_DGRAM
        // as a test, confirm with ioctl that we have data available
        // after selecting
        int available;
        res = ioctl(server.fd, FIONREAD, &available);
        assert(res != -1);
        assert(available);
#endif

        res = do_msg_read(server.fd, &server.msg, echo_read, 0, NULL, NULL);
        if (res == -1)
        {
            return -1;
        }
        else if (res == 0)
        {
            perror("server closed");
            // finish(EXIT_FAILURE);
        }

        echo_read += res;

        // once we've read the entire message, validate it
        if (echo_read >= server.msg.length)
        {
            // assert(!strcmp(server.msg.buffer, MESSAGE));
            // std::cout << "Read the message from server: " << server.msg.buffer << std::endl;
            echo_read = 0;
            server.state = MSG_COMPLETE;
            // server.msg.length = NULL;
            // server.msg.buffer = NULL;
            return res;
            // finish(EXIT_SUCCESS);
        }
    }
    return -1;
}

int client_socket::client_socket_h::write_sock(std::string client_ip)
{

    std::cout << "client_socket: trying to send" << client_ip << std::endl;
    static char out[1024 * 2];
    static int pos = 0;
    fd_set fdr;
    fd_set fdw;
    int res;

    // make sure that server.fd is ready to read / write
    FD_ZERO(&fdr);
    FD_ZERO(&fdw);
    FD_SET(server.fd, &fdr);
    FD_SET(server.fd, &fdw);
    res = select(64, &fdr, &fdw, NULL, NULL);
    if (res == -1)
    {
        perror("select failed");
        finish();
    }
    std::cout << "client_socket: select succeeded" << std::endl;

    if (server.state == MSG_WRITE)
    {
        std::cout << "Trying to do message write" << std::endl;
        if (!FD_ISSET(server.fd, &fdw))
        {
            std::cout << "Failing inside MSG_WRITE" << std::endl;
            return -1;
        }
        std::cout << "client_socket: setting up memory for the message" << std::endl;
        std::cout << "client_ip.c_str() = " << client_ip.c_str() << std::endl;
        std::cout << "client_ip.c_str() length = " << strlen(client_ip.c_str()) << std::endl;
        std::cout << "client_ip length: " << client_ip.length() << std::endl;
        memset(&echo_msg, 0, sizeof(msg_t));
        echo_msg.length = client_ip.length();
        echo_msg.buffer = (char *)malloc(echo_msg.length);
        //strncpy(echo_msg.buffer, client_ip.c_str(), echo_msg.length);
        memcpy(echo_msg.buffer, &client_ip[0], client_ip.length());
        // echo_msg.buffer[client_ip.length()] = '\0';

        res = do_msg_write(server.fd, &echo_msg, echo_wrote, 0, NULL, 0);
        if (res == -1)
        {
            return -1;
        }
        else if (res == 0)
        {
            perror("server closed");
            finish();
        }

        echo_wrote += res;

        // once we're done writing the message, read it back
        if (echo_wrote >= echo_msg.length)
        {
            // std::cout << "Wrote the message" << std::endl;
            echo_wrote = 0;
            server.state = MSG_READ;
            server.msg.buffer = NULL;
            server.msg.length = NULL;
            return res;
        }
        return res;
    }
    return -1;
}

client_socket::client_socket_h::client_socket_h(string address, int port)
{
    struct sockaddr_in addr;
    int res;

    memset(&server, 0, sizeof(server_conn_t));
    server.state = MSG_WRITE;

    // setup the message we're going to echo
    memset(&echo_msg, 0, sizeof(msg_t));
    echo_msg.length = strlen(MESSAGE) + 1;
    echo_msg.buffer = (char *)malloc(echo_msg.length);
    strncpy(echo_msg.buffer, MESSAGE, echo_msg.length);

    echo_read = 0;
    echo_wrote = 0;

    // create the socket and set to non-blocking
#if !TEST_DGRAM
    server.fd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
    server.fd = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
#endif
    if (server.fd == -1)
    {
        perror("cannot create socket");
        finish();
    }
    fcntl(server.fd, F_SETFL, O_NONBLOCK);

    // connect the socket
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, &address[0], &addr.sin_addr) != 1)
    {
        perror("inet_pton failed");
        finish();
    }

    res = connect(server.fd, (struct sockaddr *)&addr, sizeof(addr));
    if (res == -1 && errno != EINPROGRESS)
    {
        perror("connect failed");
        finish();
    }

    {
        int z;
        struct sockaddr_in adr_inet;
        socklen_t len_inet = sizeof adr_inet;
        z = getsockname(server.fd, (struct sockaddr *)&adr_inet, &len_inet);
        if (z != 0)
        {
            perror("getsockname");
            finish();
        }
        char buffer[1000];
        sprintf(buffer, "%s:%u", inet_ntoa(adr_inet.sin_addr), (unsigned)ntohs(adr_inet.sin_port));
        // TODO: This is not the correct result: We should have a auto-bound address
        char *correct = "0.0.0.0:0";
        printf("got (expected) socket: %s (%s), size %lu (%lu)\n", buffer, correct, strlen(buffer), strlen(correct));
        assert(strlen(buffer) == strlen(correct));
        assert(strcmp(buffer, correct) == 0);
    }

    {
        int z;
        struct sockaddr_in adr_inet;
        socklen_t len_inet = sizeof adr_inet;
        z = getpeername(server.fd, (struct sockaddr *)&adr_inet, &len_inet);
        if (z != 0)
        {
            perror("getpeername");
            finish();
        }
        char buffer[1000];
        sprintf(buffer, "%s:%u", inet_ntoa(adr_inet.sin_addr), (unsigned)ntohs(adr_inet.sin_port));
        char correct[1000];
        sprintf(correct, "127.0.0.1:%u", port);
        printf("got (expected) socket: %s (%s), size %lu (%lu)\n", buffer, correct, strlen(buffer), strlen(correct));
        assert(strlen(buffer) == strlen(correct));
        assert(strcmp(buffer, correct) == 0);
    }
}

client_socket::client_socket_h::~client_socket_h()
{
    /*
    if (server.fd)
    {
        close(server.fd);
        server.fd = 0;
    }
    
#ifdef __EMSCRIPTEN__
#ifdef REPORT_RESULT
    REPORT_RESULT(result);
#endif
    emscripten_force_exit(result);
#else
#endif
*/
    // exit(0);
}