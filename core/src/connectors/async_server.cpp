#include <connectors/async_server.h>

#include <memory>

#include <asio.hpp>
#include <utils/utils.h>
#include <utils/enums.h>
#include <utils/debug_logger.h>

#include <connectors/async_utils.h>


namespace {
    std::vector<char> construct_timeout_data(int port) {
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

    std::vector<char> construct_fetch_response(
            int port, json const & data, std::pair<std::string, std::string> && new_versions) {
        using namespace async_utils;
        std::vector<char> result;
        result.push_back(cbor_map_header<5>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::AppName);
        json port_json = port;
        json::to_cbor(port_json, result);


        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Data);
        json::to_cbor(data, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::StartVersion);
        json start_version_json = std::move(new_versions.first);
        json::to_cbor(start_version_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::EndVersion);
        json end_version_json = std::move(new_versions.second);
        json::to_cbor(end_version_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Status);
        json status_json = static_cast<unsigned int>(
                enums::StatusCode::Success);
        json::to_cbor(status_json, result);

        return result;
    }
}


async_server::server::server(version_manager::VersionManager & manager, unsigned short port,
                             unsigned short thread_count) :
        manager(manager),
        m_context(), thread_count(thread_count),
        m_writer_context(),
        writer_context_lock(std::make_unique<asio::io_context::work>(m_writer_context)),
        m_acceptor(
                m_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {
    auto handler = std::make_shared<connection_handler>(manager, m_context, m_writer_context);
    asio::ip::tcp::socket & handler_socket = handler->socket();

    m_acceptor.async_accept(
            handler_socket,
            [this, shared_handler = std::move(handler)](
                    asio::error_code const & ec) {
                handle_new_connection(shared_handler, ec);
            }
    );

    for (int i = 0; i < thread_count; i++) {
        thread_pool.emplace_back(
                [this]() {
                    try {
                        m_context.run();
                    } catch (std::exception & ex) {
                        logger::error("thread pool thread catches exception: ", ex.what());
                    }
                }
        );
    }

    writer_thread.emplace_back(
            [this]() {
                try {
                    m_writer_context.run();
                } catch (std::exception & ex) {
                    logger::error("writing thread catches exception: ", ex.what());
                }
            }
    );
}

void async_server::server::handle_new_connection(std::shared_ptr<connection_handler> const & handler,
                                                 asio::error_code const & ec) {
    if (ec) {
        if (ec.value() != asio::error::operation_aborted) {
            logger::error("new connection error with category ", ec.category().name(), " and content: ", ec.value());
        }
        return;
    }
    auto new_handler = std::make_shared<connection_handler>(manager, m_context, m_writer_context);
    asio::ip::tcp::socket & handler_socket = new_handler->socket();
    m_acceptor.async_accept(
            handler_socket,
            [this, shared_handler = std::move(new_handler)](
                    asio::error_code const & ec) {
                handle_new_connection(shared_handler, ec);
            });
    handler->start();

}

async_server::server::~server() {
    m_acceptor.cancel();
    for (auto & th: thread_pool)
        th.join();
    writer_context_lock = nullptr;
    for (auto & th: writer_thread)
        th.join();
}

connection_handler::connection_handler(
        version_manager::VersionManager & manager, asio::io_context & context, asio::io_context & writer_context) :
        manager(manager), m_context(context), m_writer_context(writer_context),
        m_socket(context), waiting_ack(nullptr) {
}

asio::ip::tcp::socket & connection_handler::socket() {
    return m_socket;
}

void connection_handler::start() {
    start_connection();
}

void connection_handler::start_connection() {
    asio::async_read(
            m_socket,
            asio::buffer(m_int_buffer),
            [me = shared_from_this()](
                    asio::error_code const & ec, std::size_t bytes_transferred) {
                if (ec) {
                    logger::error(
                            "packet length error: ", ec.category().name(),
                            ": ", ec.value(),
                            "bytes: ", bytes_transferred);
                    return;
                }
                unsigned int length = async_utils::unpack_unsigned_int(
                        me->m_int_buffer);
                if (length)
                    me->read_and_process_request(length);
            }
    );
}

void connection_handler::read_and_process_request(unsigned int length) {
    m_buffer = std::vector<char>(length);
    asio::async_read(
            m_socket,
            asio::buffer(m_buffer.data(), length),
            [me = shared_from_this(), expected_length = length](
                    asio::error_code const & ec, std::size_t bytes_transferred) {
                try {
                    if (ec) {
                        logger::error("packet error: ", ec.category().name(), ": ", ec.value());
                        return;
                    }
                    if (expected_length != bytes_transferred) return;
                    me->process_request(
                            json::from_cbor(
                                    me->m_buffer.data(), expected_length));
                } catch (std::exception & ex) {
                    logger::error("exception in handle packet : ", ex.what());
                }
            }
    );
}

void connection_handler::process_request(json && request) {
    // Name of the app sending the request (required).
    std::string appname;
    // Data sent by the request (Push: required, Fetch: absent).
    json data;
    // Type of request: Push or Fetch (required).
    enums::RequestType request_type;
    // start and end version tags
    // (Push: start and end required, Fetch: only start required).
    std::string start_v;
    std::string end_v;
    // Flag Wait (optional)
    // Push: wait for push to complete
    // Fetch: wait for new data to be present before responding.
    bool wait = false;
    // Max time to wait for fetch_await to complete. (optional)
    // Relevant only when request type is Pull and wait is True.
    double timeout = 0;
    // Types requested or sent. (optional)
    json req_types;


    bool has_appname = false;
    bool has_request_type = false;

    for (auto & it : request.get_obj()) {
        switch (it.first[0]) {
            case enums::transfer_fields::AppName:
                appname = std::move(
                        it.second.get_ref<json::string_t &>());
                if (remote_appname.empty())
                    remote_appname = appname;
                has_appname = true;
                break;
            case enums::transfer_fields::Data:
                data = std::move(it.second);
                break;
            case enums::transfer_fields::RequestType:
                if (utils::eq_in_int(it.second, enums::RequestType::Pull))
                    request_type = enums::RequestType::Pull;
                else if (utils::eq_in_int(it.second, enums::RequestType::Push))
                    request_type = enums::RequestType::Push;
                else
                    throw std::runtime_error("Unknown request type.");
                has_request_type = true;
                break;
            case enums::transfer_fields::StartVersion:
                start_v = it.second.get_ref<json::string_t &>();
                break;
            case enums::transfer_fields::EndVersion:
                end_v = it.second.get_ref<json::string_t &>();
                break;
            case enums::transfer_fields::Wait:
                wait = it.second;
                break;
            case enums::transfer_fields::WaitTimeout:
                timeout = it.second;
                break;
            case enums::transfer_fields::Types:
                req_types = std::move(it.second);
                break;
            default:
                throw std::runtime_error("Unknown TransferField in Request");
        }
    }

    if (!has_appname)
        throw std::runtime_error("No appname in request");

    if (!has_request_type)
        throw std::runtime_error("No request type");

    if (start_v.empty())
        throw std::runtime_error("Start version is needed in any request");

    if (request_type == enums::RequestType::Push) {
        if (end_v.empty())
            throw std::runtime_error("Final version needed in Push request");
        if (data.is_null())
            throw std::runtime_error("No data received on push.");
        if (!wait) {
            ack_push();
        }
        asio::post(m_writer_context,
                [me = shared_from_this(),
                        appname = std::move(appname),
                        start_v = std::move(start_v),
                        end_v = std::move(end_v),
                        data = std::move(data),
                        work_lock = std::make_unique<asio::io_context::work>(m_context),
                        wait]() mutable {
                    try {
                        me->manager.receive_data(appname, start_v, end_v, std::move(data));
                    } catch (std::exception & ex) {
                        logger::error("Exception in push into version graph: ", ex.what());
                    }
                    asio::post(me->m_context,
                            [me, wait]() {
                                if (wait) {
                                    me->ack_push();
                                }
                                me->start_connection();
                                // Do no changes after this for this node as requests
                                // from the same node can then be processed in parallel leading to
                                // undefined behavior. We do not want that.
                                // So exit this function after start_connection!
                            }
                    );
                }
        );
        std::hash<std::string>{}("abc");
        return;
    } else {
        // This is a fetch request
        if (req_types.is_null())
            throw std::runtime_error("No request types for pull request.");
        if (wait) {
            // Fetch await was called.
            // Creating a new thread that will wait for the right time to
            // invoke construct_fetch_response.
            std::thread waiting_thread{
                    [me = shared_from_this(),
                            appname = std::move(appname),
                            start_v = std::move(start_v),
                            req_types = std::move(req_types),
                            work_lock = std::make_unique<asio::io_context::work>(m_context),
                            timeout]() mutable {
                        me->start_wait_for(
                                timeout, std::move(appname),
                                std::move(start_v), std::move(req_types));
                    }
            };
            waiting_thread.detach();
        } else {
            // Normal Fetch response.
            // Send information that is present.
            try {
                process_fetch_request(appname, start_v, std::move(req_types));
            }
            catch (std::exception & ex) {
                logger::error("exception here:", ex.what());
            }
        }
    }
}

// Only socket Function required for push
void connection_handler::ack_push(bool success) {
    m_int_buffer[0] = success;
    asio::error_code ec;
    asio::write(
            m_socket,
            asio::buffer(m_int_buffer.data(), sizeof(unsigned char)),
            ec);
    if (ec) {
        logger::error("ack push error:", ec.category().name(), ": ", ec.value());
    }
}

// Socket Functions required for fetch
void connection_handler::process_fetch_request(
        std::string const & appname, std::string const & start_v,
        json && req_types) {
    auto [data_to_send, new_start_v, new_end_v] = manager.retrieve_data(
            appname, start_v, std::move(req_types));

    json temp_data = {appname, new_start_v, new_end_v};
    waiting_ack = std::make_unique<json>(std::move(temp_data));

    m_buffer = construct_fetch_response(0, data_to_send, {std::move(new_start_v), std::move(new_end_v)});

    send_fetch_response();
}

void connection_handler::start_wait_for(
        double timeout, std::string && appname, std::string && start_v,
        json && req_types) {
    bool timeout_triggered = false;
    if (timeout != 0) {
        timeout_triggered = !manager.wait_graph_change_for(
                start_v, version_manager::float_sec_t(timeout));
    } else {
        manager.wait_graph_change(start_v);
    }
    if (timeout_triggered) {
        asio::post(m_context,
                [me = shared_from_this()]() {
                    me->send_timeout_response();
                }
        );
    } else {
        asio::post(m_context,
                [me = shared_from_this(),
                        appname = std::move(appname),
                        start_v = std::move(start_v),
                        req_types = std::move(req_types)]() mutable {
                    me->process_fetch_request(
                            appname, start_v, std::move(req_types));
                }
        );
    }
}

void connection_handler::send_timeout_response() {
    m_buffer = construct_timeout_data(0);
    send_fetch_response();
}

void connection_handler::send_fetch_response() {
    unsigned int buffer_length = m_buffer.size();
    async_utils::pack_unsigned_int(m_int_buffer, buffer_length);
    asio::async_write(
            m_socket, asio::buffer(m_int_buffer),
            [me = shared_from_this(), buffer_length](
                    asio::error_code const & ec, std::size_t bytes_transferred) {
                me->send_fetch_bytes(
                        ec, bytes_transferred, buffer_length);
            }
    );
}

void connection_handler::send_fetch_bytes(
        asio::error_code const & ec, std::size_t bytes_transferred,
        unsigned int bytes_expected) {
    if (ec) {
        logger::error("buffer length sent error ", ec.category().name(), ": ", ec.value());
        return;
    }
    if (bytes_transferred != sizeof(unsigned int)) return;
    asio::async_write(
            m_socket, asio::buffer(m_buffer.data(), m_buffer.size()),
            [me = shared_from_this(), bytes_expected](
                    asio::error_code const & ec, std::size_t bytes_transferred) {
                if (ec) {
                    logger::error("write buffer error ", ec.category().name(), ": ", ec.value());
                }
                if (!ec && bytes_transferred == bytes_expected)
                    me->wait_for_ack();
            }
    );
}

void connection_handler::wait_for_ack() {
    asio::async_read(
            m_socket,
            asio::buffer(m_int_buffer.data(), sizeof(unsigned char)),
            [me = shared_from_this()](
                    asio::error_code const & ec, std::size_t bytes_transferred) {
                me->handle_fetch_ack(ec, bytes_transferred);
            }
    );
    return;
}

void connection_handler::handle_fetch_ack(
        const asio::error_code & ec, std::size_t bytes_transferred) {
    if (ec) {
        logger::error("pull ack error ", ec.category().name(), ": ", ec.value());
        return;
    }

    if (bytes_transferred != sizeof(unsigned char)) return;
    bool pull_success = m_int_buffer[0];
    {
        json & temp_data = *waiting_ack;
        if (pull_success) {
            manager.data_sent_confirmed(
                    temp_data[0], temp_data[1], temp_data[2]);
        }
    }
    waiting_ack = nullptr;
    start_connection();
}

connection_handler::~connection_handler() {
}
