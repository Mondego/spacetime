#include <connectors/async_client.h>
#include <connectors/async_utils.h>

#include <utils/debug_logger.h>

using namespace client_socket;

namespace
{
    inline static const std::string root_tag = "ROOT";

    template <typename T, template <typename> class data_t = std::vector>
    inline void push_back_all(std::vector<T> &target, data_t<T> const &data)
    {
        std::size_t previous_length = target.size();
        target.resize(previous_length + data.size());
        std::copy(data.begin(), data.end(), target.begin() + previous_length);
    }

    std::vector<char>
    construct_fetch_request(
        std::string appname, std::string version, bool wait, double timeout,
        const std::vector<char> &tp_chain)
    {
        using namespace async_utils;
        std::vector<char> result;
        result.push_back(cbor_map_header<6>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::AppName);
        json appname_json = std::move(appname);
        json::to_cbor(appname_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::RequestType);
        result.push_back(cbor_small_unsigned_int<
                         static_cast<unsigned int>(
                             enums::RequestType::Pull)>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::StartVersion);
        json version_json = std::move(version);
        json::to_cbor(version_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Wait);
        result.push_back(wait ? cbor_const::cbor_true : cbor_const::cbor_false);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::WaitTimeout);
        json timeout_json = timeout;
        json::to_cbor(timeout_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Types);

        push_back_all(result, tp_chain);

        return result;
    }

    std::vector<char> construct_push_request(
        std::string appname, std::pair<std::string, std::string> &&versions,
        bool wait, json const &diff_data)
    {
        std::cout << "Original: In construct_push_request" << std::endl;
        using namespace async_utils;
        std::vector<char> result;
        result.push_back(cbor_map_header<6>());

        // 0 AppName
        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::AppName);
        json appname_json = std::move(appname);
        json::to_cbor(appname_json, result);

        // 1 Data
        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Data);
        json::to_cbor(diff_data, result);

        // 2 request type
        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::RequestType);
        result.push_back(cbor_small_unsigned_int<
                         static_cast<unsigned int>(
                             enums::RequestType::Push)>());

        // 3 - start version
        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::StartVersion);
        json::to_cbor(std::move(versions.first), result);

        // 4 - End Version
        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::EndVersion);
        json::to_cbor(std::move(versions.second), result);

        // this is 5 - Wait
        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Wait);
        result.push_back(wait ? cbor_const::cbor_true : cbor_const::cbor_false);

        // there's no 6,7,8

        return result;
    }
} // namespace

async_client::connector::connector(version_manager::VersionManager &manager, std::string m_appname, std::vector<char> &&m_tp_chain_cbor) : manager(manager), m_appname(std::move(m_appname)), current_version(root_tag), m_tp_chain_cbor(std::move(m_tp_chain_cbor)), merged_pull_data(false), fetch_in_progress(false), push_in_progress(false), c_socket(client_socket_h((char *)"127.0.0.1", 59159))
{
}

async_client::connector::~connector()
{

}

void async_client::connector::connect(std::string const &address, unsigned short port)
{

    std::cout << "Client - created socket" << std::endl;

}

std::tuple<json, std::string, std::string> async_client::connector::pull_req(
    bool wait, double timeout)
{

    if (push_in_progress)
        return std::make_tuple(json{""}, "", "");

    // std::cout << "client: [step 1] Entered pull_req" << std::endl;
    m_buffer.clear();
    m_buffer = construct_fetch_request(
        m_appname, current_version, wait, timeout, m_tp_chain_cbor);

    fetch_in_progress = true;
    std::cout << "$$" << "Fetch req " << std::endl;
    send_request();
    // if (c_socket.server.state == MSG_READ)
    receive_fetch_response();
    // std::cout << "Back in pull_req after fetch receive" << std::endl;
    if (c_socket.server.state == MSG_COMPLETE)
    {

        if (!merged_pull_data)
        {
            m_buffer.clear();
            m_buffer.insert(m_buffer.end(), c_socket.server.msg.buffer, c_socket.server.msg.buffer + c_socket.server.msg.length);
            json data = json::from_cbor(m_buffer);
            std::cout << "MSG_COMPLETE, fetched data" << data.dump(4) << std::endl;

            start_version =
                std::move(data[std::string(1, enums::transfer_fields::StartVersion)].get_ref<json::string_t &>());
            end_version =
                std::move(data[std::string(1, enums::transfer_fields::EndVersion)].get_ref<json::string_t &>());
            package =
                std::move(data[std::string(1, enums::transfer_fields::Data)]);
            merged_pull_data = true;
        }
        int res = send_ack();
        if (res != -1)
        {
            std::cout << "Sent fetch ack to server" << std::endl;
            std::cout << "s, e = " << start_version << ", " << end_version << std::endl;
            std::cout << "package: " << package.dump(4) << std::endl;
            std::cout << "merged_pull_data " << merged_pull_data << std::endl;
            current_version = end_version;
            merged_pull_data = false;

            fetch_in_progress = false;
            std::cout << "=== Almost finished a fetch cycle ===" << std::endl;

            return {std::move(package), std::move(start_version), std::move(end_version)};
        }
    }
    return std::make_tuple(json{""}, "", "");
    // if (!m_socket.is_open())
    // throw std::runtime_error("Not connected");
}

bool async_client::connector::push_req(
    json const &diff_data, std::string &&start_v, std::string &&end_v, bool wait)
{
    if (fetch_in_progress)
    {
        std::cout << "Cannot to push as pull/fetch is in progress!" << std::endl;
        return false;
    }
    std::cout << "Push is happening now" << std::endl;
    push_in_progress = true;
    // if (!m_socket.is_open()) throw std::runtime_error("Not connected");

    std::string final_version = end_v;

    if (c_socket.server.state == MSG_WRITE) {
        m_buffer.clear();
        m_buffer = construct_push_request(m_appname, std::pair(std::move(start_v), std::move(end_v)), wait, diff_data);
        std::cout << "Push req " << std::endl;
        send_request();
    }
    bool success;
    if (c_socket.server.state == MSG_READ) {
        success = receive_ack();
        if(success) {
            current_version = std::move(final_version);
            std::cout << "=== Finished a push cycle ===" << std::endl;
        }
    }
    return success;
}

std::string &async_client::connector::get_current_version()
{
    return current_version;
}

bool async_client::connector::is_connected()
{
    // return m_socket.is_open();
    return true;
}

unsigned int async_client::connector::receive_buffer_length()
{
    /*
    asio::read(m_socket, asio::buffer(m_int_buffer));
    return async_utils::unpack_unsigned_int(m_int_buffer);
    */
}

void async_client::connector::receive_fetch_response()
{
    // std::cout << "[client] step 3 receive fetch response" << std::endl;
    // std::cout << "sleeping for a minute" << std::endl;
    // m_buffer.clear();
    // std::cout << "making read request" << std::endl;
    int r_res = c_socket.read_sock();
    // std::cout << "Done with fetch" << std::endl;
    if (r_res != -1)
        std::cout << "client: Finished pull_req receive content from server" << std::endl;

    /*
    unsigned int length = receive_buffer_length();
    m_buffer.resize(length);
    asio::read(m_socket, asio::buffer(m_buffer.data(), length));
    */
}

void async_client::connector::send_request_length(unsigned int length)
{

}

void async_client::connector::send_request()
{
    // std::cout << "[client]: step 2 send request" << std::endl;
    std::string s(m_buffer.begin(), m_buffer.end());
    std::cout << "$$" << "The request is: " <<  s << std::endl;
    int w_res = c_socket.write_sock(s);
    if (w_res != -1)
        std::cout << "client: Finished request call" << std::endl;

}

bool async_client::connector::receive_ack()
{
    int r_res = c_socket.read_sock();
    // std::cout << "Done with fetch" << std::endl;
    if (r_res != -1)
    {
        std::cout << "client: Received push_req ack from server" << std::endl;
        c_socket.server.state = MSG_WRITE;
        push_in_progress = false;
        return true;
    }
    return false;


}

int async_client::connector::send_ack()
{
    std::cout << "[client] step 4 send ack" << std::endl;
    m_buffer.clear();
    // m_buffer.resize(1);
    // m_buffer.emplace_back(true);
    // std::string s(m_buffer.begin(), m_buffer.end());
    std::string s = "true";
    std::cout << "Ack content: " << s << "; Ack length" << s.length() << std::endl;
    return c_socket.confirm_sock(s);

}
