#ifndef DATAFRAME_CORE_ASYNC_SERVER_H
#define DATAFRAME_CORE_ASYNC_SERVER_H

#include <asio.hpp>
#include <vector>
#include <memory>

#include <utils/utils.h>
#include <version_manager.h>

namespace {

    class connection_handler : public std::enable_shared_from_this<connection_handler> {
    public:
        connection_handler(
                version_manager::VersionManager & manager,
                asio::io_context & context, asio::io_context & writer_context);

        ~connection_handler();

        asio::ip::tcp::socket & socket();

        void start();

    private:
        void start_connection();

        void read_and_process_request(unsigned int length);

        void process_request(json && request);

        void ack_push(bool success = true);

        void process_fetch_request(
                std::string const & appname,
                std::string const & start_v, json && req_types);

        void start_wait_for(
                double timeout, std::string && appname,
                std::string && start_v, json && req_types);

        void send_timeout_response();

        void send_fetch_response();

        void send_fetch_bytes(
                asio::error_code const & ec, std::size_t bytes_transferred,
                unsigned int bytes_expected);

        void wait_for_ack();

        void handle_fetch_ack(
                asio::error_code const & ec, std::size_t bytes_transferred);

        version_manager::VersionManager & manager;
        asio::io_context & m_context;
        asio::io_context & m_writer_context;
        asio::ip::tcp::socket m_socket;
        std::vector<char> m_buffer;
        std::array<unsigned char, 4> m_int_buffer;
        std::unique_ptr<json> waiting_ack;
        std::string remote_appname;
    };
}

namespace async_server {
    class server {
    public:
        server(version_manager::VersionManager & manager, unsigned short port, unsigned short thread_count = 1);

        ~server();

        inline unsigned short port() {
            return m_acceptor.local_endpoint().port();
        }

    private:

        void handle_new_connection(std::shared_ptr<connection_handler> const & handler, asio::error_code const & ec);

        version_manager::VersionManager & manager;


        asio::io_context m_context;
        asio::io_context m_writer_context;
        std::unique_ptr<asio::io_context::work> writer_context_lock;
        std::vector<std::thread> writer_thread;
        unsigned int thread_count;
        std::vector<std::thread> thread_pool;
        asio::ip::tcp::acceptor m_acceptor;
    };
}

#endif //DATAFRAME_CORE_ASYNC_SERVER_H
