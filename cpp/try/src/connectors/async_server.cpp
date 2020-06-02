//#define ASIO_ENABLE_HANDLER_TRACKING

#include <connectors/async_server.h>

#include <memory>
#include <iostream>

#include <asio.hpp>
//#include <asio/detail/handler_tracking.hpp>
#include <utils/utils.h>
#include <utils/enums.h>

#include <connectors/async_utils.h>

async_server::server::server(version_manager::VersionManager & manager, unsigned short port,
                             unsigned short thread_count) :
manager(manager),
m_context(), thread_count(thread_count),
m_acceptor(m_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {
    auto handler = std::make_shared<connection_handler>(manager, m_context);
    asio::ip::tcp::socket & handler_socket = handler->socket();
    m_acceptor.async_accept(
            handler_socket,
            [this, shared_handler=std::move(handler)](asio::error_code const & ec) {
                handle_new_connection(shared_handler, ec);
            });

    for (int i=0; i<thread_count; i++) {
        thread_pool.emplace_back(
                [this]() {
                    try {
                        m_context.run();
                    } catch (std::exception & ex) {
                        std::cout << std::string("thread pool thread catches exception:").append(ex.what()) << std::endl;
                    }
                    std::cout << "thread pool thread stopped" << std::endl;
                }
                );
    }
}

void
async_server::server::handle_new_connection(std::shared_ptr<connection_handler> const & handler, asio::error_code const & ec) {
    if (ec) {
        std::cout << "new connection error " << ec.category().name() << ": " << ec.value() << std::endl;
        return;  
    }
    //handler->socket().set_option(asio::socket_base::keep_alive(true));
    handler->start();
    auto new_handler = std::make_shared<connection_handler>(manager, m_context);
    asio::ip::tcp::socket & handler_socket = new_handler->socket();
    m_acceptor.async_accept(
            handler_socket,
            [this, shared_handler = std::move(new_handler)](asio::error_code const & ec) {
                handle_new_connection(shared_handler, ec);
            });
}

async_server::server::~server() {
    std::cout << "server is being destructed\n";
    m_acceptor.cancel();
//    m_context.stop();
    for (auto & th: thread_pool)
        th.join();
    std::cout << "Here? \n";
}


connection_handler::connection_handler(version_manager::VersionManager & manager, asio::io_context & context)
        : manager(manager), m_context(context), m_socket(context), waiting_ack(nullptr) {
    std::cout << "new handler" << std::endl;
}

asio::ip::tcp::socket & connection_handler::socket() {
    return m_socket;
}

void connection_handler::start() {
    std::cout << m_socket.remote_endpoint().address().to_string() << " " << m_socket.remote_endpoint().port() << std::endl;
    read_packet_length();
}

void connection_handler::read_packet_length() {
    if (waiting_ack) {
        asio::async_read(
                m_socket,
                asio::buffer(m_int_buffer.data(), sizeof(unsigned char)),
                [me = shared_from_this()](asio::error_code const & ec, std::size_t bytes_transferred) {
                    me->handle_pull_ack(ec, bytes_transferred);
                }
                );
        return;
    }
    else {
        std::cout << "waiting for 4 bytes of packet length" << std::endl;
        asio::async_read(
                m_socket,
                asio::buffer(m_int_buffer),
                [me = shared_from_this()](asio::error_code const & ec, std::size_t bytes_transferred) {
                    me->handle_packet_length(ec, bytes_transferred);
                }
        );
        return;
    }
}

void connection_handler::handle_packet_length(asio::error_code const & ec, std::size_t bytes_transferred) {
    if (ec) {
        std::cout << "packet length error " << ec.category().name() << ": " << ec.value() << "bytes: " << bytes_transferred << std::endl;
        return;  
    } 
    if (bytes_transferred != sizeof(unsigned int)) return;
    unsigned int length = async_utils::unpack_unsigned_int(m_int_buffer);
    std::cout << "received packet length: " << length << std::endl;
    if (length)
        read_packet(length);
//    else
//        m_socket.shutdown(asio::ip::tcp::socket::shutdown_both);
}

void connection_handler::read_packet(unsigned int length) {
//    if (length > m_buffer.size() || length < (m_buffer.size() / 3))
//        m_buffer.resize(length);
    m_buffer = std::vector<char>(length);
    asio::async_read(
            m_socket,
            asio::buffer(m_buffer.data(), length),
            [me=shared_from_this(), expected_length=length](asio::error_code const & ec, std::size_t bytes_tranferred) {
                try {
                    me->handle_packet(ec, bytes_tranferred, expected_length);
                } catch (std::exception & ex) {
                    std::cout << "exception in handle packet : " << ex.what() << std::endl;
                }
            }
            );
}

void connection_handler::handle_packet(asio::error_code const & ec, std::size_t bytes_transferred,
                                       unsigned long bytes_expected) {
    if (ec) {
        std::cout << "packet error " << ec.category().name() << ": " << ec.value() << std::endl;
        return;  
    } 
    if (bytes_expected != bytes_transferred) return;

    json packet = json::from_cbor(m_buffer.data(), bytes_expected);

    std::cout << "server received packet: " << packet.dump(2) << std::endl;

    std::string appname;
    json data;
    enums::RequestType request_type;
    std::string start_v;
    std::string end_v;
    bool wait = false;
    double timeout = 0;
    json req_types;

    auto packet_it = packet.get_obj().begin();
    auto packet_end = packet.get_obj().end();

    if (packet_it != packet_end && packet_it->first[0] == enums::transfer_fields::AppName) { //appname
        appname = std::move(packet_it->second.get_ref<json::string_t &>());
        ++packet_it;
    } else
        throw std::runtime_error("No appname in packet");

    if (packet_it->first[0] == enums::transfer_fields::Data) { //data
        data = std::move(packet_it->second);
        ++packet_it;
    }

    if (packet_it != packet_end && packet_it->first[0] == enums::transfer_fields::RequestType) { //request type
        int requestInt = packet_it->second;
        if (utils::eq_in_int(requestInt, enums::RequestType::Pull))
            request_type = enums::RequestType::Pull;
        else if (utils::eq_in_int(requestInt, enums::RequestType::Push))
            request_type = enums::RequestType::Push;
        else
            throw std::runtime_error("Unknown request type.");
        ++packet_it;
    } else
        throw std::runtime_error("No request type");

    if (packet_it != packet_end && packet_it->first[0] == enums::transfer_fields::Versions) { //versions
        if (request_type == enums::RequestType::Pull) {
            start_v = packet_it->second.get_ref<json::string_t &>();
        } else {
            start_v = std::move(packet_it->second[0].get_ref<json::string_t &>());
            end_v = std::move(packet_it->second[1].get_ref<json::string_t &>());
        }
        ++packet_it;
    } else
        throw std::runtime_error("No versions");

    if (packet_it != packet_end && packet_it->first[0] == enums::transfer_fields::Wait) { //wait
        wait = packet_it->second;
        ++packet_it;
    }

    if (packet_it != packet_end && packet_it->first[0] == enums::transfer_fields::WaitTimeout) { //wait timeout
        timeout = packet_it->second;
        ++packet_it;
    }

    if (packet_it != packet_end && packet_it->first[0] == enums::transfer_fields::Types) { //req types
        req_types = std::move(packet_it->second);
        ++packet_it;
    }

    std::cout << "Im here, now start handling, request type is: " << (int)request_type << std::endl;

    if (request_type == enums::RequestType::Push) {
        if (data.is_null())
            throw std::runtime_error("No data for push request.");
//        if (!wait) {
//            ack_push();
//        }
        try {
            manager.receive_data(appname, start_v, end_v, std::move(data));
        } catch (std::exception & ex) {
            std::cout << "exception here: " << ex.what() << std::endl;
        }
//        if (wait) {
            ack_push();
//        }
    } else {
        if (req_types.is_null())
            throw std::runtime_error("No request types for pull request.");
        if (wait) {
            std::cout << "How it this possible?" << std::endl;
            std::thread waiting_thread{
                [me=shared_from_this(), appname=std::move(appname), start_v=std::move(start_v), req_types=std::move(req_types),
                 work_lock=std::make_unique<asio::io_context::work>(m_context), timeout]() mutable {
                    me->start_wait_for(timeout, std::move(appname), std::move(start_v), std::move(req_types));
                }
            };
            waiting_thread.detach();
        } else {
            try{
                send_pull_response(appname, start_v, std::move(req_types));
            }
            catch (std::exception & ex) {
                std::cout << "exception here: " << ex.what() << std::endl;
            }
        }
    }
}

void connection_handler::send_pull_response(const std::string & appname, const std::string & start_v, json && req_types) {
    auto[data_to_send, new_start_v, new_end_v] = manager.retrieve_data(appname, start_v, std::move(req_types));

    json temp_data = {appname, new_start_v, new_end_v};
    waiting_ack = std::make_unique<json>(std::move(temp_data));

    m_buffer = dump_pull_resp_data(0, data_to_send, {std::move(new_start_v), std::move(new_end_v)});
    std::cout << "data to be sent: " << json::from_cbor(m_buffer).dump() << std::endl;
    send_buffer();
}

void connection_handler::start_wait_for(double timeout, std::string && appname, std::string && start_v,
                                        json && req_types) {
    bool timeout_triggered = false;
    if (timeout != 0) {
        timeout_triggered = !manager.wait_graph_change_for(start_v, version_manager::float_sec_t(timeout));
    } else {
        manager.wait_graph_change(start_v);
    }
    if (timeout_triggered) {
        m_context.post([me=shared_from_this()](){
            me->send_timeout_response();
        });
    } else {
        m_context.post(
                [me=shared_from_this(), appname=std::move(appname), start_v=std::move(start_v), req_types=std::move(req_types)]() mutable {
                    me->send_pull_response(appname, start_v, std::move(req_types));
                });
    }
}

void connection_handler::send_timeout_response() {
    m_buffer = dump_timeout_data(0);
    send_buffer();
}

void connection_handler::ack_push(bool success) {
    m_int_buffer[0] = success;
    asio::async_write(
            m_socket,
            asio::buffer(m_int_buffer.data(), sizeof(unsigned char)),
            [me=shared_from_this()](asio::error_code const & ec, std::size_t bytes_tranferred){
                if (ec) {
                    std::cout << "ack push error " << ec.category().name() << ": " << ec.value() << std::endl;
                }
                if (!ec && bytes_tranferred == sizeof(unsigned char))
                    me->read_packet_length();
            });
}

void connection_handler::send_buffer() {
    unsigned int buffer_length = m_buffer.size();
    async_utils::pack_unsigned_int(m_int_buffer, buffer_length);
    asio::async_write(m_socket, asio::buffer(m_int_buffer),
            [me=shared_from_this(), buffer_length](asio::error_code const & ec, std::size_t bytes_transferred){
                me->handle_sent_buffer_length(ec, bytes_transferred, buffer_length);
    });
}

void connection_handler::handle_sent_buffer_length(asio::error_code const & ec, std::size_t bytes_transferred,
                                                   unsigned int bytes_expected) {
    if (ec) {
        std::cout << "buffer length sent error " << ec.category().name() << ": " << ec.value() << std::endl;
        return;  
    } 
    if (bytes_transferred != sizeof(unsigned int)) return;
    asio::async_write(m_socket, asio::buffer(m_buffer.data(), m_buffer.size()),
            [me=shared_from_this(), bytes_expected](asio::error_code const & ec, std::size_t bytes_transferred) {
                if (ec) {
                    std::cout << "write buffer error " << ec.category().name() << ": " << ec.value() << std::endl;
                }
                if (!ec && bytes_transferred == bytes_expected)
                    me->read_packet_length();
    });
}

void connection_handler::handle_pull_ack(const asio::error_code & ec, std::size_t bytes_transferred) {
    if (ec) {
        std::cout << "pull ack error " << ec.category().name() << ": " << ec.value() << std::endl;
        return;  
    }
    std::cout << "pull ack length: " << bytes_transferred << std::endl;
    if (bytes_transferred != sizeof(unsigned char)) return;
    bool pull_success = m_int_buffer[0];
    {
        json & temp_data = *waiting_ack;
        if (pull_success) {
            manager.data_sent_confirmed(temp_data[0], temp_data[1], temp_data[2]);
        }
    }
    waiting_ack = nullptr;
    read_packet_length();
}

connection_handler::~connection_handler() {
    std::cout << "How????" << std::endl;
}

namespace {

    std::vector<char> dump_pull_resp_data(int port, json const &data, std::pair<std::string, std::string> &&new_versions) {
        using namespace async_utils;
        std::vector<char> result;
        result.push_back(cbor_map_header<4>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::AppName);
        json port_json = port;
        json::to_cbor(port_json, result);


        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Data);
        json::to_cbor(data, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Versions);
        json versions_json = {std::move(new_versions.first), std::move(new_versions.second)};
        json::to_cbor(versions_json, result);


        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Status);
        json status_json = static_cast<unsigned int>(enums::StatusCode::Success);
        json::to_cbor(status_json, result);

        return result;
    }

    std::vector<char> dump_timeout_data(int port) {
        using namespace async_utils;
        std::vector<char> result;
        result.push_back(cbor_map_header<2>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::AppName);
        json port_json = port;
        json::to_cbor(port_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Status);
        json status_json = static_cast<unsigned int>(enums::StatusCode::Timeout);
        json::to_cbor(status_json, result);

        return result;
    }

}