#include <connectors/async_client.h>

#include <connectors/async_utils.h>

#include <iostream>

namespace {
    inline static const std::string root_tag = "ROOT";

    template <typename T, template<typename> class data_t = std::vector>
    inline void push_back_all(std::vector<T> & target, data_t<T> const & data) {
        std::size_t previous_length = target.size();
        target.resize(previous_length + data.size());
        std::copy(data.begin(), data.end(), target.begin() + previous_length);
    }

    std::vector<char>
    dump_pull_req_data(std::string appname, std::string version, bool wait, double timeout, const std::vector<char> & tp_chain) {
        using namespace async_utils;
        std::vector<char> result;
        result.push_back(cbor_map_header<6>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::AppName);
        json appname_json = std::move(appname);
        json::to_cbor(appname_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::RequestType);
        result.push_back(cbor_small_unsigned_int<static_cast<unsigned int>(enums::RequestType::Pull)>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Versions);
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
//
//        for (char const & x : tp_chain)
//            result.push_back(x);
//        auto previous_len = result.size();
//        result.resize(previous_len + tp_chain.size());
//        std::copy(tp_chain.begin(), tp_chain.end(), result.begin() + previous_len);

        push_back_all(result, tp_chain);

        return result;
    }

    std::vector<char> dump_push_req_data(std::string appname, std::pair<std::string, std::string> &&versions, bool wait, const json & diff_data) {
        using namespace async_utils;
        std::vector<char> result;
        result.push_back(cbor_map_header<5>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::AppName);
        json appname_json = std::move(appname);
        json::to_cbor(appname_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::RequestType);
        result.push_back(cbor_small_unsigned_int<static_cast<unsigned int>(enums::RequestType::Push)>());

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Versions);
        json versions_json = {std::move(versions.first), std::move(versions.second)};
        json::to_cbor(versions_json, result);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Data);
        json::to_cbor(diff_data, result);
//        auto previous_len = result.size();
//        result.resize(previous_len + diff_data.size());
//        std::copy(diff_data.begin(), diff_data.end(), result.begin() + previous_len);
        //push_back_all(result, diff_data);

        result.push_back(cbor_const::cbor_char_header);
        result.push_back(enums::transfer_fields::Wait);
        result.push_back(wait ? cbor_const::cbor_true : cbor_const::cbor_false);

        return result;
    }
}

async_client::connector::connector(version_manager::VersionManager & manager, std::string m_appname,
                                   std::vector<char> && m_tp_chain_cbor) :
manager(manager), m_appname(std::move(m_appname)), current_version(root_tag), m_tp_chain_cbor(std::move(m_tp_chain_cbor)), m_socket(m_context) {
}

void async_client::connector::connect(const std::string & address, unsigned short port) {
    m_socket.close();
    asio::error_code ec;
    asio::ip::tcp::endpoint m_endpoint;
    auto m_address = asio::ip::address::from_string(address, ec);
    if (ec) {
        asio::ip::tcp::resolver m_resolver(m_context);
        auto query_result = m_resolver.resolve(address, std::to_string(port), ec);
        if (ec || query_result.empty())
            throw std::runtime_error("Address is not a valid ip address or hostname. Invalid address: " + address);
    } else {
        m_endpoint = asio::ip::tcp::endpoint(m_address, port);
    }
    m_socket.connect(m_endpoint, ec);
    m_socket.non_blocking(false);
    std::cout << "client local endpoint: " << m_socket.local_endpoint().address().to_string() << m_socket.local_endpoint().port() << std::endl;
    std::cout << "client connected to: " << m_socket.remote_endpoint().address().to_string() << m_socket.remote_endpoint().port() << std::endl;
    if (ec) {
        throw std::runtime_error("Failed to connect to" + address + ":" + std::to_string(port));
    }
}

unsigned int async_client::connector::receive_buffer_length() {
    asio::read(m_socket, asio::buffer(m_int_buffer));
    return async_utils::unpack_unsigned_int(m_int_buffer);
}

void async_client::connector::receive_packet() {
    unsigned int length = receive_buffer_length();
    m_buffer.resize(length);
    asio::read(m_socket, asio::buffer(m_buffer.data(), length));
}

void async_client::connector::send_buffer_length(unsigned int length) {
    async_utils::pack_unsigned_int(m_int_buffer, length);
    try {
        std::size_t sent_len = asio::write(m_socket, asio::buffer(m_int_buffer));
        if (sent_len != 4)
            std::cout << "sent only " << sent_len << std::endl;
    } catch (std::exception & ex) {
        std::cout << "write length error" << ex.what() << "sending: " << length << std::endl;
    }
}

bool async_client::connector::receive_boolean() {
    asio::read(m_socket, asio::buffer(m_int_buffer.data(), sizeof(unsigned char)));
    return m_int_buffer[0];
}

void async_client::connector::send_buffer() {
    unsigned int length = m_buffer.size();
    send_buffer_length(length);
    asio::write(m_socket, asio::buffer(m_buffer.data(), length));
}

std::tuple<json, std::string, std::string> async_client::connector::pull_req(bool wait, double timeout) {
    std::cout << "what??????" << json::from_cbor(m_tp_chain_cbor).dump(2) << std::endl;
    if (!m_socket.is_open())
        throw std::runtime_error("Not connected");
    m_buffer = dump_pull_req_data(m_appname, current_version, wait, timeout, m_tp_chain_cbor);
    std::cout << "sending request:" << json::from_cbor(m_buffer).dump(2) << std::endl;
    send_buffer();
    std::cout << "sent request" << std::endl;
    receive_packet();
    std::cout << "pull req received size: " << m_buffer.size() << std::endl;

    json data = json::from_cbor(m_buffer);
    std::cout << "pull req received content as json: " << data.dump(2) << std::endl;
    if (wait && timeout > 0 && utils::eq_in_int(data[std::string(1, enums::transfer_fields::Status)], enums::StatusCode::Timeout)) {
        throw async_utils::timeout_error("No new version received in time" + std::to_string(timeout));
    }
    json new_versions = std::move(data[std::string(1, enums::transfer_fields::Versions)]);
    json package = std::move(data[std::string(1, enums::transfer_fields::Data)]);
    send_boolean(true);
    current_version = new_versions[1].get_ref<json::string_t &>();
    return {std::move(package), std::move(new_versions[0].get_ref<json::string_t &>()), std::move(new_versions[1].get_ref<json::string_t &>())};
}

void async_client::connector::send_boolean(bool value) {
    m_int_buffer[0] = value;
    asio::write(m_socket, asio::buffer(m_int_buffer.data(), sizeof(unsigned char)));
}

bool
async_client::connector::push_req(json const & diff_data, std::string && start_v, std::string && end_v,
                                  bool wait) {
    if (!m_socket.is_open())
        throw std::runtime_error("Not connected");
    std::string new_version = end_v;
    m_buffer = dump_push_req_data(m_appname, std::pair(std::move(start_v), std::move(end_v)), wait, diff_data);
    send_buffer();
    bool success = receive_boolean();
    current_version = std::move(new_version);
    return success;
}

std::string & async_client::connector::get_current_version() {
    return current_version;
}

bool async_client::connector::is_connected() {
    return m_socket.is_open();
}

async_client::connector::~connector() {
    std::cout << "connector is destroyed" << std::endl;
    if (m_socket.is_open()) {
        send_buffer_length(0);
    }
//    m_socket.shutdown(asio::ip::tcp::socket::shutdown_both);
}
