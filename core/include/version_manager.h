#ifndef DATAFRAME_CORE_VERSION_MANAGER_H
#define DATAFRAME_CORE_VERSION_MANAGER_H

#include <version_graph.h>

#include <set>
#include <mutex>
#include <condition_variable>
#include <utils/utils.h>
#include <utils/enums.h>

namespace version_manager{
    using utils::json;

    using have_custom_merge_t = std::function<bool(std::string const &)>;
    using float_sec_t = std::chrono::duration<double>;
    using custom_merge_function_t =
    std::function<std::pair<json, json>(
            std::string const & dtpname,
            std::string const & oid,
            json const & original_dims,
            json const & new_dims,
            json const & conflicting_dims,
            std::vector<char> const & new_change_cbor,
            std::vector<char> const & conflicting_change_cbor
    )>;

    using type_dim_map_t = std::map<std::string, std::unordered_set<std::string>>;

    class VersionManager{
    private:
        version_graph::Graph graph;

        std::string app_name;
        std::set<std::string> type_names;
        type_dim_map_t type_dim_map;

        have_custom_merge_t have_custom_merge;
        custom_merge_function_t custom_merge_function;

        json filter_diff(json && recv_diff, std::set<std::string> const & types);

        json filter_diff(json && recv_diff);

        std::mutex write_mutex;

        std::condition_variable graph_changed;

        enums::Autoresolve autoresolve;


        std::set<std::string> process_req_types(json && req_types);

        std::tuple<json, std::string, std::string> retrieve_data_nomaintain(std::string version, json && req_types);

        void resolve_conflict(const std::string & start_v, const std::string & end_v, json && package, bool from_external);

        std::pair<json, json> operational_transform(std::string const & start, json && new_path, json const & conflicting_path, bool from_external);

        std::pair<json, json> ot_on_type(std::string const & tpname, std::string const & start, json && new_tp_change, json const & conflicting_tp_change, bool from_external);

        std::pair<json, json> ot_on_obj(std::string const & tpname, std::string const & oid, std::string const & start, json && new_obj_change, json && conflicting_obj_change, bool from_external);

        std::pair<json, json> resolve_modification(std::string const & dtpname, json && new_change, json && conflicting_change);

        std::pair<json, json> resolve_modification_dims(json && new_dims, json && conflicting_dims);

        std::pair<json, json> cherrypick_jsons(json && lhs_json, json && rhs_json);

        json dim_diff (std::string const & dtpname, json && original_dim, json && new_dim);

        std::pair<json, json> run_custom_merge(const std::string & version, const std::string & dtpname,
                                               const std::string & oid, json && original_obj, json && new_obj,
                                               json && conflicting_obj, std::vector<char> const & new_change_cbor,
                                               std::vector<char> const & conflicting_change_cbor);
    public:

        version_graph::Graph & get_g();

        VersionManager(VersionManager const &) = delete;

        VersionManager(VersionManager &&) = delete;

        VersionManager(std::string const & app_name, have_custom_merge_t && have_merge, custom_merge_function_t && custom_merge_function,
                       type_dim_map_t && type_dim_map, enums::Autoresolve autoresolve = enums::Autoresolve::FullResolve);

        bool receive_data(
                std::string const & app_name, const std::string & start_v, const std::string & end_v,
                json && recv_diff, bool from_external = true);

        std::tuple<json, std::string, std::string> retrieve_data(const std::string & app_name, std::string version, json && req_types = json());

        void data_sent_confirmed(std::string const & app_name, std::string const & start_v, std::string const & end_v);

        void wait_graph_change(std::string const & version);

        bool wait_graph_change_for(std::string const & version, const float_sec_t & period);
    };
}

#endif //DATAFRAME_CORE_VERSION_MANAGER_H
