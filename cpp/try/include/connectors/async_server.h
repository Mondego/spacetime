#ifndef LIBTRY_ASYNC_SERVER_H
#define LIBTRY_ASYNC_SERVER_H

#include <asio.hpp>
#include <vector>
#include <memory>

#include <utils/utils.h>
#include <version_manager.h>

namespace {

    class connection_handler : public std::enable_shared_from_this<connection_handler>{
    public:
        connection_handler(version_manager::VersionManager & manager, asio::io_context & context);
        ~connection_handler();
        asio::ip::tcp::socket & socket();
        void start();
    private:
        void read_packet_length();
        void handle_pull_ack(asio::error_code const & ec, std::size_t bytes_transferred);
        void handle_packet_length(asio::error_code const & ec, std::size_t bytes_transferred);
        void read_packet(unsigned int length);
        void handle_packet(asio::error_code const & ec, std::size_t bytes_transferred,
                           unsigned long bytes_expected);

        void
        start_wait_for(double timeout, std::string && appname, std::string && start_v, json && req_types);

        void send_pull_response(const std::string & appname, const std::string & start_v, json && req_types);

        void send_timeout_response();

        void send_buffer();

        void handle_sent_buffer_length(asio::error_code const & ec, std::size_t bytes_transferred,
                                       unsigned int bytes_expected);

        void ack_push(bool success = true);

        version_manager::VersionManager & manager;
        asio::io_context & m_context;
        asio::ip::tcp::socket m_socket;
        std::vector<char> m_buffer;
        std::array<unsigned char, 4> m_int_buffer;
        std::unique_ptr<json> waiting_ack;
    };

    std::vector<char> dump_timeout_data(int port);

    std::vector<char> dump_pull_resp_data(int port, json const & data, std::pair<std::string, std::string> && new_versions);
}

namespace async_server{
    class server{
    public:
        server(version_manager::VersionManager & manager, unsigned short port, unsigned short thread_count = 1);
        ~server();

    private:

        void handle_new_connection(std::shared_ptr<connection_handler> const & handler, asio::error_code const & ec);

        version_manager::VersionManager & manager;


        asio::io_context m_context;
        unsigned int thread_count;
        std::vector<std::thread> thread_pool;
        asio::ip::tcp::acceptor m_acceptor;
    };
}

#endif //LIBTRY_ASYNC_SERVER_H
