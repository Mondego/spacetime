#ifndef DATAFRAME_CORE_ASYNC_CLIENT_H
#define DATAFRAME_CORE_ASYNC_CLIENT_H

#include <vector>
#include <asio.hpp>

#include <version_manager.h>

namespace async_client {
    class connector {
    private:
        version_manager::VersionManager & manager;
        std::vector<char> m_tp_chain_cbor;

        std::string m_appname;

        std::string current_version;

        asio::io_context m_context;
        asio::ip::tcp::socket m_socket;

        std::array<unsigned char, 4> m_int_buffer;
        std::vector<char> m_buffer;

        unsigned int receive_buffer_length();

        void receive_fetch_response();

        void send_request_length(unsigned int length);

        void send_request();

        bool receive_ack();

        void send_ack();

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
