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
#include "connectors/test_sockets_msg.h"
#include "connectors/server_socket.h"

// TODO: Refactor common code in the various
// socket functions

void server_socket::server_socket_h::cleanup(int)
{
    std::cout << "In cleanup!" << std::endl;
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

int server_socket::server_socket_h::read_sock()
{
    int res;
    fd_set fdr; // read descriptor set
    fd_set fdw; // write descriptor set

    // see if there are any connections to accept or read / write from
    FD_ZERO(&fdr);
    FD_ZERO(&fdw);
    FD_SET(server.fd, &fdr);
    FD_SET(server.fd, &fdw);

#if !TEST_DGRAM
    std::cout << "@@ fd list: ";
    std::cout << "cliend.fd = " << client.fd << std::endl;


  /* generate secret number between 1 and 10: */
     //auto iSecret = rand() % client_fd_list.size() + 1;

     
    for(auto cid : client_fd_list)
    {
        std::cout << cid << ", ";
        FD_SET(cid, &fdr);
        FD_SET(cid, &fdw);
    }
    std::cout << std::endl;

/*
    if (client.fd)
        FD_SET(client.fd, &fdr); // add existing client.fd to the read descriptor set (only one)
    if (client.fd)
        FD_SET(client.fd, &fdw); // add existing client.fd to the write descriptor set (only one)
        */
#endif
    std::cout << "Calling select" << std::endl;
    res = select(64, &fdr, &fdw, NULL, NULL); // watch for data
    if (res == -1)
    {
        perror("select failed");
        exit(EXIT_SUCCESS);
    }
    std::cout << "After select" << std::endl;



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
    } else {
        std::cout << ">> Before client.fd" << client.fd << std::endl;
        int idx = processingIndex % client_fd_list.size();
        std::set<int>::iterator it = client_fd_list.begin();
        std::advance(it, idx);
        client.fd = *it;
        std::cout << ">> After client.fd = " << client.fd << std::endl;
        processingIndex += 1;
        /*
        std::cout << ">> [Read] Other activity " << std::endl;
        std::cout << ">> Before client.fd" << client.fd << std::endl;
        for (auto cid :client_fd_list) {
            std::cout << "cid = " << cid << ", ";
            if (cid == client.fd) {
                continue;
            }
            client.fd = cid;
            break;
        }
        std::cout << std::endl;
        std::cout << ">> After client.fd = " << client.fd << std::endl;
        */
    }


    std::cout << "@@ Socket read, client.fd = " << client.fd << std::endl;
#if !TEST_DGRAM
    int fd = client.fd;
#else
    int fd = server.fd;
#endif

    client_fd_list.insert(fd);

    if (client_map.find(fd) == client_map.end())
    {
        client_t  *c = (client_t *) malloc(sizeof(client_t));
        memset(c, 0, sizeof(client_t));

        client_map[fd] = *c;
        client_map[fd].state = MSG_READ;
    }

    if (client_map[fd].state == MSG_READ)
    {
        std::cout << "Entered MSG_READ " << std::endl;
        if (!FD_ISSET(fd, &fdr))
        {
            return -1;
        }

        socklen_t addrlen = sizeof(client_map[fd].addr);
        res = do_msg_read(fd, &client_map[fd].msg, client_map[fd].read, 0, (struct sockaddr *)&client_map[fd].addr, &addrlen);
        std::cout << "Server read the message: " << client_map[fd].msg.buffer << std::endl;
        if (res == -1)
        {
            return -1;
        }
        else if (res == 0)
        {
            // client disconnected
            // memset(&client, 0, sizeof(client_t));
            return -1;
        }

        client_map[fd].read += res;

        // once we've read the entire message, echo it back
        if (client_map[fd].read >= client_map[fd].msg.length)
        {
            client_map[fd].read = 0;
            client_map[fd].state = MSG_WRITE;
            std::cout << "Set client state to write" << std::endl;
        }
        // return res;
        return fd;
    }
    return fd;
}

int server_socket::server_socket_h::write_sock(std::string server_ip)
{

    int res;
    fd_set fdr;
    fd_set fdw;

    // see if there are any connections to accept or read / write from
    FD_ZERO(&fdr);
    FD_ZERO(&fdw);
    FD_SET(server.fd, &fdr);
    FD_SET(server.fd, &fdw);
    // std::cout << "In the write_sock " << std::endl;
#if !TEST_DGRAM
    for(auto cid : client_fd_list)
    {
        FD_SET(cid, &fdr);
        FD_SET(cid, &fdw);
    }
    /*
    if (client.fd)
        FD_SET(client.fd, &fdr);
    if (client.fd)
        FD_SET(client.fd, &fdw);
        */
#endif
    res = select(64, &fdr, &fdw, NULL, NULL);
    if (res == -1)
    {
        perror("select failed");
        exit(EXIT_SUCCESS);
    }
    // std::cout << "after select in write_sock " << std::endl;


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
        // std::cout << "accept happened" << std::endl;
#endif
        assert(client.fd != -1);
    }
else {
        std::cout << ">> [Write] Other activity " << std::endl;
        std::cout << ">> Before client.fd" << client.fd << std::endl;
        for (auto cid :client_fd_list) {
            std::cout << "cid = " << cid << ", ";
            if (cid == client.fd) {
                continue;
            }
            // client.fd = cid;
            break;
        }
        std::cout << std::endl;
        std::cout << ">> After client.fd = " << client.fd << std::endl;
    }
    // std::cout << "after client.fd != -1 " << std::endl;
    std::cout << "@@ Socket write, client.fd = " << client.fd << std::endl;
#if !TEST_DGRAM
    int fd = client.fd;
#else
    int fd = server.fd;
#endif
    client_fd_list.insert(fd);
    if (client_map.find(fd) == client_map.end())
    {
        client_t  *c = (client_t *) malloc(sizeof(client_t));
        memset(c, 0, sizeof(client_t));

        client_map[fd] = *c;
    }
    // std::cout << "after fd = client.fd" << std::endl;
    if (client_map[fd].state == MSG_WRITE)
    {
        if (!FD_ISSET(fd, &fdw))
        {
            return -1;
        }
        // std::cout << "In the write branch " << std::endl;
        memset(&client_map[fd].msg, 0, sizeof(msg_t));
        client_map[fd].msg.length = server_ip.length();
        client_map[fd].msg.buffer = (char *)malloc(client_map[fd].msg.length);
        memcpy(client_map[fd].msg.buffer, &server_ip[0], client_map[fd].msg.length);

        res = do_msg_write(fd, &client_map[fd].msg, client_map[fd].wrote, 0, (struct sockaddr *)&client_map[fd].addr, sizeof(client_map[fd].addr));
        if (res == -1)
        {
            return -1;
        }
        else if (res == 0)
        {
            // client disconnected
            memset(&client_map[fd], 0, sizeof(client_t));
            return -1;
        }
        // std::cout << "Server wrote message" << client.msg.buffer << std::endl;

        client_map[fd].wrote += res;

        if (client_map[fd].wrote >= client_map[fd].msg.length)
        {
            std::cout << "Setting state to MSG_COMPLETE for fd = " << fd << std::endl;
            client_map[fd].wrote = 0;
            client_map[fd].state = MSG_COMPLETE;
            client_map[fd].msg.length = NULL;
            client_map[fd].msg.buffer = NULL;
            return fd;
            //return res;

#if CLOSE_CLIENT_AFTER_ECHO
            close(client.fd);
            memset(&client, 0, sizeof(client_t));
#endif
        }
        return -1;
    }

    return -1;
}

int server_socket::server_socket_h::confirm_sock()
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
    for(auto cid : client_fd_list)
    {
        FD_SET(cid, &fdr);
        FD_SET(cid, &fdw);
    }
    /*
    if (client.fd)
        FD_SET(client.fd, &fdr);
    if (client.fd)
        FD_SET(client.fd, &fdw);
        */
#endif
    res = select(64, &fdr, &fdw, NULL, NULL);
    if (res == -1)
    {
        perror("select failed");
        exit(EXIT_SUCCESS);
    }
    // std::cout << "confirm_sock: after select" << std::endl;


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
else {
        std::cout << ">> [Confirm] Other activity " << std::endl;
        std::cout << ">> Before client.fd" << client.fd << std::endl;
        for (auto cid :client_fd_list) {
            std::cout << "cid = " << cid << ", ";
            if (cid == client.fd) {
                continue;
            }
            // client.fd = cid;
            break;
        }
        std::cout << std::endl;
        std::cout << ">> After client.fd = " << client.fd << std::endl;
    }
    // std::cout << "confirm_sock: after accept" << std::endl;

#if !TEST_DGRAM
    int fd = client.fd;
#else
    int fd = server.fd;
#endif
    client_fd_list.insert(fd);
    // std::cout << "after fd = client.fd" << std::endl;
    // std::cout << client.state << std::endl;
    std::cout << "@@ Socket confirm, client.fd = " << client.fd << std::endl;
    if (client_map.find(fd) == client_map.end())
    {
        client_t  *c = (client_t *) malloc(sizeof(client_t));
        memset(c, 0, sizeof(client_t));

        client_map[fd] = *c;
    }
    if (client_map[fd].state == MSG_COMPLETE)
    {
        std::cout << "confirm_sock: Inside MSG_COMPLETE" << std::endl;

        if (!FD_ISSET(fd, &fdr))
        {
            std::cout << "failing in server confirm" << std::endl;
            return -1;
        }

        socklen_t addrlen = sizeof(client_map[fd].addr);
        res = do_msg_read(fd, &client_map[fd].msg, client_map[fd].read, 0, (struct sockaddr *)&client_map[fd].addr, &addrlen);
        std::cout << "Server read the message: " << client_map[fd].msg.buffer << std::endl;
        if (res == -1)
        {
            return -1;
        }
        else if (res == 0)
        {
            // client disconnected
            memset(&client_map[fd], 0, sizeof(client_t));
            return -1;
        }

        client_map[fd].read += res;

        // once we've read the entire message, echo it back
        if (client_map[fd].read >= client_map[fd].msg.length)
        {
            client_map[fd].read = 0;
            client_map[fd].state = MSG_READ;
            std::cout << "Set client[fd] state to read" << std::endl;
        }
        return fd;
        //return res;
    }
    return -1;
}

server_socket::server_socket_h::server_socket_h(std::string address, int port)
{
  /* initialize random seed: */
  srand (time(NULL));
  processingIndex = 0;
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

    // atexit(cleanup);
    // signal(SIGTERM, cleanup);

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

    res = ::bind(server.fd, (struct sockaddr *)&addr, sizeof(addr));
    if (res == -1)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

#if !TEST_DGRAM
    res = listen(server.fd, 200000);
    if (res == -1)
    {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }
#endif
}

server_socket::server_socket_h::~server_socket_h()
{

    std::cout << "In ~server_socket_h" << std::endl;

    for(auto it = client_map.begin(); it != client_map.end(); it++)
    {
        close(client_map[it->first].fd);
        client_map[it->first].fd = 0;
    }
    /*
    if (client.fd)
    {
        close(client.fd);
        client.fd = 0;
    }
    */
    if (server.fd)
    {
        close(server.fd);
        server.fd = 0;
    }
}