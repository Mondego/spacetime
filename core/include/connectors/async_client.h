#ifndef DATAFRAME_CORE_ASYNC_CLIENT_H
#define DATAFRAME_CORE_ASYNC_CLIENT_H

#include <vector>
#include <asio.hpp>

#include <version_manager.h>
#include <connectors/client_socket.h>

namespace async_client {
    class connector {
    private:
        version_manager::VersionManager & manager;
        std::vector<char> m_tp_chain_cbor;

        std::string m_appname;

        std::string current_version;

        client_socket::client_socket_h c_socket;

        bool merged_pull_data;
        bool fetch_in_progress;
        bool push_in_progress;
        std::string start_version;
        std::string end_version;
        json package;




        std::array<unsigned char, 4> m_int_buffer;
        std::vector<char> m_buffer;

        unsigned int receive_buffer_length();

        void receive_fetch_response();

        void send_request_length(unsigned int length);

        void send_request();

        bool receive_ack();

        int send_ack();

    public:
        connector(version_manager::VersionManager & manager, std::string m_appname,
                  std::vector<char> && m_tp_chain_cbor);

        ~connector();

        void connect(std::string const & address, unsigned short port);

        std::tuple<json, std::string, std::string> pull_req(bool wait = false, double timeout = 0);

        bool push_req(json const & diff_data, std::string && start_v, std::string && end_v,
                      bool wait = false);

        std::string & get_current_version();

        bool is_connected();
    };
}

#endif //DATAFRAME_CORE_ASYNC_CLIENT_H
