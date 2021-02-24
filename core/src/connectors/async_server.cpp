#include <thread>
#include <connectors/async_server.h>

#include <memory>

#include <asio.hpp>
#include <utils/utils.h>
#include <utils/enums.h>
#include <utils/debug_logger.h>
#include <iostream>

#include <connectors/async_utils.h>

using namespace server_socket;

namespace
{
    std::vector<char> construct_timeout_data(int port)
    {
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
        int port, json const &data, std::pair<std::string, std::string> &&new_versions)
    {
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
} // namespace

void launch_wasm_server(version_manager::VersionManager &manager)
{
    auto handler = std::make_shared<connection_handler>(manager);
}

//async_server::server::server(version_manager::VersionManager &manager, unsigned short port, unsigned short thread_count) : manager(manager), m_context(), thread_count(thread_count), m_acceptor( m_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)) {
async_server::server::server(version_manager::VersionManager &manager, unsigned short port, unsigned short thread_count) : manager(manager)
{
    // TODO: Implement a round-robin thread handler her
    // to emulate boost library behavior
    // std::thread t1(launch_wasm_server, manager);
    //#####
    std::thread t1(launch_wasm_server, std::ref(manager));
    t1.detach();

}

void async_server::server::handle_new_connection(std::shared_ptr<connection_handler> const &handler,
                                                 asio::error_code const &ec)
{ 

}

async_server::server::~server()
{
    std::cout << "killing the server " << std::endl;
    t1.join();
    // m_acceptor.cancel();
    for (auto &th : thread_pool)
        th.join();
}

connection_handler::connection_handler(
    version_manager::VersionManager &manager) : manager(manager), s_socket(server_socket::server_socket_h("127.0.0.1", 59160)), fetch_in_progress(false), push_in_progress(false)
{
    // s_socket = server_socket::server_socket_h(std::string("127.0.0.1"), 59159);
    std::cout << "Created socket in server" << std::endl;
    bool processed_request = false;


    // s_socket.client.state = MSG_READ;
    while (1)
    {
        // if(s_socket.client.state == MSG_READ)
        int r_res = s_socket.read_sock();
        // if (r_res != -1)
            // std::cout << "client: Finished pull_req call" << std::endl;

        if (r_res != -1 && s_socket.client_map[r_res].state == MSG_WRITE)
        {
            std::cout << "Step 1 read request" << std::endl;
            if (!processed_request)
                process_request(
                    json::from_cbor(
                        s_socket.client_map[r_res].msg.buffer, s_socket.client_map[r_res].msg.length), r_res);
            // send_fetch_response();
        }
        if (r_res != -1 && s_socket.client_map[r_res].state == MSG_COMPLETE)
            wait_for_ack(r_res);
        // s_socket.write_socket(s);
    }
}

/*
asio::ip::tcp::socket &connection_handler::socket()
{
    return m_socket;
}
*/

void connection_handler::start()
{
    start_connection();
}

void connection_handler::start_connection()
{

}

void connection_handler::read_and_process_request(unsigned int length)
{

}

void connection_handler::process_request(json &&request, int sfd)
{
    std::cout << "$$ Entered process_request" << std::endl;
    std::cout << "$$ Request is: " << request.dump(4) << std::endl;
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

    for (auto &it : request.get_obj())
    {
        switch (it.first[0])
        {
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

    if (request_type == enums::RequestType::Push)
    {
        if (end_v.empty())
            throw std::runtime_error("Final version needed in Push request");
        if (data.is_null())
            throw std::runtime_error("No data received on push.");
        /*
        if (!wait)
        {
            ack_push();
        }
        */
        try
        {
            auto [xdata, xstart_v, xend_v] = manager.retrieve_data(appname, "ROOT");
            std::cout << "before push, retreive data: " << xdata.dump(4) << "; start, end = " << xstart_v << ", " << xend_v << std::endl;
            std::cout << "Receiving push data: "
                      << "start, end=" << start_v << ", " << end_v << std::endl;
            std::cout << "package: " << data << std::endl;
            manager.receive_data(appname, start_v, end_v, std::move(data));

            auto [fdata, fstart_v, fend_v] = manager.retrieve_data(appname, "ROOT");
            std::cout << "after push, retreive data: " << fdata.dump(4) << "; start, end = " << fstart_v << ", " << fend_v << std::endl;
        }
        catch (std::exception &ex)
        {
            logger::error("Exception in push into version graph: ", ex.what());
        }
        ack_push(true, sfd);
        /*
        if (wait)
        {
            ack_push();
        }
        */
        // start_connection();
        // Do no changes after this for this node as requests
        // from the same node can then be processed in parallel leading to
        // undefined behavior. We do not want that.
        // So exit this function after start_connection!
        return;
    }
    else
    {
        // This is a fetch request
        if (req_types.is_null())
            throw std::runtime_error("No request types for pull request.");
        if (wait)
        {
            std::cout << "Fetch await disabled" << std::endl;

        }
        else
        {
            std::cout << "Step 2 Process fetch request" << std::endl;
            // Normal Fetch response.
            // Send information that is present.
            try
            {
                process_fetch_request(appname, start_v, std::move(req_types));
            }
            catch (std::exception &ex)
            {
                logger::error("exception here:", ex.what());
            }
        }
    }
}

// Only socket Function required for push
void connection_handler::ack_push(bool success, int sfd)
{
    std::cout << "in ack_push";
    std::string msg = (success == true) ? "true" : "false";
    int w_res = s_socket.write_sock(msg);
    if (w_res != -1)
    {
        std::cout << "Sent ack for push success" << std::endl;
        s_socket.client_map[sfd].msg.buffer = NULL;
        s_socket.client_map[sfd].msg.length = NULL;
        push_in_progress = false;
        s_socket.client_map[sfd].state = MSG_READ;
    }

}

// Socket Functions required for fetch
void connection_handler::process_fetch_request(
    std::string const &appname, std::string const &start_v,
    json &&req_types)
{
    auto [data_to_send, new_start_v, new_end_v] = manager.retrieve_data(
        appname, start_v, std::move(req_types));

    json temp_data = {appname, new_start_v, new_end_v};
    waiting_ack = std::make_unique<json>(std::move(temp_data));

    m_buffer.clear();
    m_buffer = construct_fetch_response(0, data_to_send, {std::move(new_start_v), std::move(new_end_v)});

    send_fetch_response();
}

void connection_handler::start_wait_for(
    double timeout, std::string &&appname, std::string &&start_v,
    json &&req_types)
{

}

void connection_handler::send_timeout_response()
{
    m_buffer = construct_timeout_data(0);
    send_fetch_response();
}

void connection_handler::send_fetch_response()
{
    // unsigned int buffer_length = m_buffer.size();
    // async_utils::pack_unsigned_int(m_int_buffer, buffer_length);
    std::cout << "Step 3 Sending fetch response" << std::endl;
    std::string s(m_buffer.begin(), m_buffer.end());
    s_socket.write_sock(s);
    std::cout << "Step 4 Waiting for ack" << std::endl;
    m_buffer.clear();
    // bool pull_success = (bool)m_buffer[0];

}

void connection_handler::send_fetch_bytes(
    asio::error_code const &ec, std::size_t bytes_transferred,
    unsigned int bytes_expected)
{

}

void connection_handler::wait_for_ack(int sfd)
{
    // std::cout << "Calling confirm_sock" << std::endl;
    int res = s_socket.confirm_sock();
    if (res == -1)
        return;
    bool pull_success = false;
    ;
    if (strcmp(s_socket.client_map[sfd].msg.buffer, "true") == 0)
    {
        pull_success = true;
    }
    // bool pull_success = (bool)s_socket.client.msg.buffer[0];
    {
        json &temp_data = *waiting_ack;
        if (pull_success)
        {
            manager.data_sent_confirmed(
                temp_data[0], temp_data[1], temp_data[2]);
            std::cout << "Pull success " << std::endl;
        }
    }
    waiting_ack = nullptr;
    s_socket.client_map[sfd].msg.buffer = NULL;
    s_socket.client_map[sfd].msg.length = NULL;


    return;
}

void connection_handler::handle_fetch_ack(
    const asio::error_code &ec, std::size_t bytes_transferred)
{
    if (ec)
    {
        logger::error("pull ack error ", ec.category().name(), ": ", ec.value());
        return;
    }

    if (bytes_transferred != sizeof(unsigned char))
        return;
    bool pull_success = m_int_buffer[0];
    {
        json &temp_data = *waiting_ack;
        if (pull_success)
        {
            manager.data_sent_confirmed(
                temp_data[0], temp_data[1], temp_data[2]);
        }
    }
    waiting_ack = nullptr;
    start_connection();
}

connection_handler::~connection_handler()
{
}
