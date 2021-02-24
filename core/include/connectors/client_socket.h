#include "test_sockets_msg.h"
#include <iostream>

using namespace std;

namespace client_socket
{
    typedef struct
    {
        int fd;
        msg_t msg;
        msg_state_t state;
    } server_conn_t;
    class client_socket_h
    {
    public:
        client_socket_h(string address, int port);
        ~client_socket_h();
        int read_sock();
        int write_sock(std::string client_ip);
        int confirm_sock(std::string clinet_ip);
        server_conn_t server;

    private:
        msg_t echo_msg;
        void finish();
        int echo_read;
        int echo_wrote;
    };
}; // namespace client_socket
