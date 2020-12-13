#include <connectors/test_sockets_msg.h>
#include <iostream>

using namespace std;

namespace client_socket
{
    typedef enum
    {
        MSG_READ,
        MSG_WRITE
    } msg_state_t;

    typedef struct
    {
        int fd;
        msg_t msg;
        msg_state_t state;
    } server_t;
    class client_socket_h
    {
    public:
        client_socket_h(string address, int port);
        ~client_socket_h();
        int read_socket(msg_t *read_msg);
        int write_socket(msg_t *write_msg);

    private:
        server_t server;
        msg_t echo_msg;
        int echo_read;
        int echo_wrote;
        // accept_type = 0 = read
        // accept_type = 1 = write
        int select_req(int accept_type);
    };
}; // namespace
