#include <connectors/test_sockets_msg.h>
#include <iostream>

using namespace std;

namespace
{
    typedef enum
    {
        MSG_READ,
        MSG_WRITE
    } msg_state_t;

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

    class server_socket
    {
    public:
        server_socket(string address, int port);
        ~server_socket();
        int read_socket(msg_t *read_msg);
        int write_socket(msg_t *write_msg);

    private:
        server_t server;
        client_t client;
        // accept_type = 0 = read
        // accept_type = 1 = write
        int accept_req(int accept_type); 
    };
}; // namespace
