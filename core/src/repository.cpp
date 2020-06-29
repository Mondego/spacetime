#include <repository.h>

#include <utils/debug_logger.h>

namespace {
    std::string socket_parent_tag = "SOCKETPARENT";

    version_manager::type_dim_map_t get_type_dim_map(json & data) {
        version_manager::type_dim_map_t result;

        auto & data_map = data.get_obj();

        auto data_it = data_map.begin();
        auto data_end = data_map.end();

        while (data_it != data_end) {
            std::unordered_set<std::string> dims_set;
            auto & dims_list = data_it->second[0].get_ref<json::array_t &>();

            for (json & dim: dims_list) {
                dims_set.emplace(std::move(dim.get_ref<json::string_t &>()));
            }

            result.emplace_hint(result.end(), data_it->first, std::move(dims_set));
            ++data_it;
        }
        return result;
    }

    json get_type_chains(json & data) {
        json result = json::object();

        auto & result_map = result.get_obj();
        auto & data_map = data.get_obj();

        auto data_it = data_map.begin();
        auto data_end = data_map.end();

        while (data_it != data_end) {
            json name_list = std::move(data_it->second[1]);

            auto & name_list_vec = name_list.get_ref<json::array_t &>();

            std::sort(name_list_vec.begin(), name_list_vec.end(),
                      [](json const & lhs, json const & rhs) {
                          return lhs.get_ref<json::string_t const &>() < rhs.get_ref<json::string_t const &>();
                      }
            );

            result_map.emplace_hint(result_map.end(), data_it->first, std::move(name_list));
            ++data_it;
        }

        return result;
    }
}

repository::Repository::Repository(std::string const & app_name, version_manager::have_custom_merge_t && have_merge,
                                   version_manager::custom_merge_function_t && custom_merge_function,
                                   json && type_info, enums::Autoresolve autoresolve) :
        manager(app_name, std::move(have_merge), std::move(custom_merge_function), get_type_dim_map(type_info),
                autoresolve), m_app_name(app_name), m_type_chains(get_type_chains(type_info)) {
}

unsigned int repository::Repository::start_server(unsigned short port, unsigned short thread_count) {
    repoServer = std::make_unique<async_server::server>(manager, port, thread_count);
    return repoServer->port();
}

void repository::Repository::connect_to(std::string const & address, unsigned short port) {
    std::vector<char> type_chains_cbor;
    json::to_cbor(m_type_chains, type_chains_cbor);
    repoConnector = std::make_unique<async_client::connector>(manager, m_app_name, std::move(type_chains_cbor));
    repoConnector->connect(address, port);
}

void repository::Repository::push() {
    logger::debug("pushing");

    if (!repoConnector || !repoConnector->is_connected()) return;
    logger::debug("is connected, proceeding");

    auto [json_data, start_v, end_v] =
            manager.retrieve_data(socket_parent_tag, repoConnector->get_current_version());

    if (start_v == end_v)
        return;

    if (repoConnector->push_req(json_data, std::string(start_v), std::string(end_v))) {
        manager.data_sent_confirmed(socket_parent_tag, start_v, end_v);
    }
}

void repository::Repository::push_await() {
    if (!repoConnector || !repoConnector->is_connected()) return;
    auto [json_data, start_v, end_v] =
            manager.retrieve_data(socket_parent_tag, repoConnector->get_current_version());

    if (start_v == end_v)
        return;

    if (repoConnector->push_req(json_data, std::string(start_v), std::string(end_v), true)) {
        manager.data_sent_confirmed(socket_parent_tag, start_v, end_v);
    }
}

void repository::Repository::fetch() {
    if (!repoConnector || !repoConnector->is_connected()) return;
    logger::debug("now fetching");
    auto [json_data, start_v, end_v] = repoConnector->pull_req();
    logger::debug("pull_req data: ", json_data, " ", start_v, " ", end_v);

    manager.receive_data(socket_parent_tag, start_v, end_v, std::move(json_data));
}

void repository::Repository::fetch_await(double timeout) {
    if (timeout == 0) {
        fetch();
        return;
    }
    if (!repoConnector || !repoConnector->is_connected()) return;
    auto [json_data, start_v, end_v] = repoConnector->pull_req(true, timeout);
    manager.receive_data(socket_parent_tag, start_v, end_v, std::move(json_data));
}

version_manager::VersionManager & repository::Repository::get_manager_ref() {
    return manager;
}

bool repository::Repository::is_connected() {
    return repoConnector && repoConnector->is_connected();
}

repository::Repository::~Repository() {
    logger::debug("destroying Repo cpp");
}
