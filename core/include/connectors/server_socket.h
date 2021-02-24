#include <connectors/test_sockets_msg.h>
#include <iostream>
#include <map>
#include<set>

using namespace std;

namespace server_socket
{
    typedef struct
    {
        int fd;
    } server_t;

    typedef struct
    {
        int fd;
        struct sockaddr_in addr;
        msg_t msg;
        msg_state_t state;
        int read;
        int wrote;
    } client_t;
    class server_socket_h
    {
    public:
        server_socket_h(string address, int port);
        ~server_socket_h();
        int read_sock();
        int write_sock(std::string client_ip);
        int confirm_sock();

        server_t server;
        client_t client;
        std::map<int, client_t> client_map;
        std::set<int> client_fd_list;

    private:
        void cleanup(int);
        int processingIndex;
    };
}; // namespace server_socket
