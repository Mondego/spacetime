#include <connectors/async_client.h>

#include <connectors/async_utils.h>

#include <utils/debug_logger.h>

namespace {
    inline static const std::string root_tag = "ROOT";

    template <typename T, template <typename> class data_t = std::vector>
    inline void push_back_all(std::vector<T> & target, data_t<T> const & data) {
        std::size_t previous_length = target.size();
        target.resize(previous_length + data.size());
        std::copy(data.begin(), data.end(), target.begin() + previous_length);
    }

    std::vector<char>
    construct_fetch_request(
            std::string appname, std::string version, bool wait, double timeout,
            const std::vector<char> & tp_chain) {
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
                        enums::RequestType::Pull)
        >());

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
            std::string appname, std::pair<std::string, std::string> && versions,
            bool wait, json const & diff_data) {
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
                        enums::RequestType::Push)
        >());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::StartVersion);
        json::to_cbor(std::move(versions.first), result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::EndVersion);
        json::to_cbor(std::move(versions.second), result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Data);
        json::to_cbor(diff_data, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Wait);
        result.push_back(wait ? cbor_const::cbor_true : cbor_const::cbor_false);

        return result;
    }
}

async_client::connector::connector(version_manager::VersionManager & manager, std::string m_appname,
                                   std::vector<char> && m_tp_chain_cbor) :
        manager(manager), m_appname(std::move(m_appname)),
        current_version(root_tag),
        m_tp_chain_cbor(std::move(m_tp_chain_cbor)),
        m_socket(m_context) {
}

async_client::connector::~connector() {
    if (m_socket.is_open()) {
        send_request_length(0);
    }
}

void async_client::connector::connect(std::string const & address, unsigned short port) {
    m_socket.close();
    asio::error_code ec;
    asio::ip::tcp::endpoint m_endpoint;
    auto m_address = asio::ip::address::from_string(address, ec);
    if (ec) {
        asio::ip::tcp::resolver m_resolver(m_context);
        auto query_result = m_resolver.resolve(address, std::to_string(port), ec);
        if (ec || query_result.empty())
            throw std::runtime_error("Invalid IP address or hostname: " + address);
    } else {
        m_endpoint = asio::ip::tcp::endpoint(m_address, port);
    }
    m_socket.connect(m_endpoint, ec);
    m_socket.non_blocking(false);
    if (ec) {
        throw std::runtime_error("Failed to connect to" + address + ":" + std::to_string(port));
    }

}

std::tuple<json, std::string, std::string> async_client::connector::pull_req(
        bool wait, double timeout) {
    if (!m_socket.is_open())
        throw std::runtime_error("Not connected");
    m_buffer = construct_fetch_request(
            m_appname, current_version, wait, timeout, m_tp_chain_cbor);
    send_request();
    receive_fetch_response();
    json data = json::from_cbor(m_buffer);

    if (wait && timeout > 0
        && utils::eq_in_int(
            data[std::string(1, enums::transfer_fields::Status)],
            enums::StatusCode::Timeout)) {
        send_ack();
        throw async_utils::timeout_error(
                "No new version received in time" + std::to_string(timeout));
    }
    std::string start_version =
            std::move(data[std::string(1, enums::transfer_fields::StartVersion)].get_ref<json::string_t &>());
    std::string end_version =
            std::move(data[std::string(1, enums::transfer_fields::EndVersion)].get_ref<json::string_t &>());
    json package =
            std::move(data[std::string(1, enums::transfer_fields::Data)]);
    send_ack();
    current_version = end_version;

    return {std::move(package), std::move(start_version), std::move(end_version)};
}

bool async_client::connector::push_req(
        json const & diff_data, std::string && start_v, std::string && end_v,bool wait) {
    if (!m_socket.is_open())
        throw std::runtime_error("Not connected");
    std::string final_version = end_v;
    m_buffer = construct_push_request(m_appname, std::pair(std::move(start_v), std::move(end_v)), wait, diff_data);
    send_request();
    bool success = receive_ack();
    current_version = std::move(final_version);
    return success;
}

std::string & async_client::connector::get_current_version() {
    return current_version;
}

bool async_client::connector::is_connected() {
    return m_socket.is_open();
}

unsigned int async_client::connector::receive_buffer_length() {
    asio::read(m_socket, asio::buffer(m_int_buffer));
    return async_utils::unpack_unsigned_int(m_int_buffer);
}

void async_client::connector::receive_fetch_response() {
    unsigned int length = receive_buffer_length();
    m_buffer.resize(length);
    asio::read(m_socket, asio::buffer(m_buffer.data(), length));
}

void async_client::connector::send_request_length(unsigned int length) {
    async_utils::pack_unsigned_int(m_int_buffer, length);
    try {
        std::size_t sent_len =
                asio::write(m_socket, asio::buffer(m_int_buffer));
        if (sent_len != 4)
            logger::error("sent only ", sent_len, "expected: ", 4);
    } catch (std::exception & ex) {
        logger::error("write length error: ", ex.what(), "sending: ", length);
    }
}

void async_client::connector::send_request() {
    unsigned int length = m_buffer.size();
    send_request_length(length);
    asio::write(m_socket, asio::buffer(m_buffer.data(), length));
}

bool async_client::connector::receive_ack() {
    asio::read(m_socket, asio::buffer(m_int_buffer.data(), sizeof(unsigned char)));
    return m_int_buffer[0];
}

void async_client::connector::send_ack() {
    m_int_buffer[0] = true;
    asio::write(m_socket, asio::buffer(m_int_buffer.data(), sizeof(unsigned char)));
}

