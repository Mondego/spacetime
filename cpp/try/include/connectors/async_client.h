#ifndef LIBTRY_ASYNC_CLIENT_H
#define LIBTRY_ASYNC_CLIENT_H

#include <vector>
#include <asio.hpp>

#include <version_manager.h>

namespace {
    std::vector<char> dump_pull_req_data(std::string appname, std::string version, bool wait, double timeout, std::vector<char> const & tp_chain);

    std::vector<char> dump_push_req_data(std::string appname, std::pair<std::string, std::string> && versions, bool wait, const json & diff_data);
}

namespace async_client {
    class connector{
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

        void send_buffer_length(unsigned int length);

        bool receive_boolean();

        void send_boolean(bool value);

        void send_buffer();

        void receive_packet();

    public:
        connector(version_manager::VersionManager & manager, std::string m_appname, std::vector<char> && m_tp_chain_cbor);

        ~connector();

        void connect(std::string const & address, unsigned short port);

        std::tuple<json, std::string, std::string> pull_req(bool wait = false, double timeout = 0);

        bool push_req(json const & diff_data, std::string && start_v, std::string && end_v,
                      bool wait = false);

        std::string & get_current_version();

        bool is_connected();
    };
}

#endif //LIBTRY_ASYNC_CLIENT_H
