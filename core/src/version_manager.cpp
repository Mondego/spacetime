
#include <version_manager.h>
#include <iostream>

version_manager::VersionManager::VersionManager(
        std::string const & app_name, have_custom_merge_t && have_merge,
        custom_merge_function_t && custom_merge_function,
        type_dim_map_t && type_dim_map, enums::Autoresolve autoresolve):
    app_name(app_name), have_custom_merge(std::move(have_merge)),
    custom_merge_function(std::move(custom_merge_function)),
    type_dim_map(std::move(type_dim_map)), autoresolve(autoresolve) {

    auto type_it = this->type_dim_map.begin();
    auto type_end = this->type_dim_map.end();

    while (type_it != type_end) {
        this->type_names.emplace_hint(type_names.end(), type_it->first);
        ++type_it;
    }
}

bool version_manager::VersionManager::receive_data(
        std::string const & app_name, const std::string & start_v,
        const std::string & end_v, json && recv_diff, bool from_external) {
    if (start_v == end_v) return true;
    // Pick up only the delta changes for types that are stored by this
    // dataframe. Remote dataframes can send more types if they store it.
    json package = filter_diff(std::move(recv_diff));
    {
        // Acquire write lock of reader-write lock.
        std::lock_guard write_lock(write_mutex);

        // merge changes, detect and resolve conflicts, if any.
        resolve_conflict(start_v, end_v, std::move(package), from_external);
        // Update garbage collector references.
        graph.update_app_ref(app_name, end_v);
        // Get rid of unnecessary versions.
        graph.clear_disposal();
    }

    // Notify any threads waiting for fetch_await or checkout_await that
    // changes were made.
    graph_changed.notify_all();
    return true;
}

json version_manager::VersionManager::filter_diff(
        json && recv_diff, std::set<std::string> const & types) {
    json result = json::object();
    auto & result_map = result.get_obj();
    auto & diff_map = recv_diff.get_obj();

    auto type_it = types.begin();
    auto type_end = types.end();

    auto diff_it = diff_map.begin();
    auto diff_end = diff_map.end();

    auto result_end = result_map.end();

    while (type_it != type_end && diff_it != diff_end) {
        int compare_result = type_it->compare(diff_it->first);
        if (!compare_result) {
            result_map.emplace_hint(result_end, *type_it, std::move(diff_it->second));
            ++type_it;
            ++diff_it;
            continue;
        }
        if (compare_result < 0) {
            ++type_it;
        } else {
            ++diff_it;
        }
    }

    return std::move(result);
}

json version_manager::VersionManager::filter_diff(json && recv_diff) {
    return filter_diff(std::move(recv_diff), type_names);
}

void version_manager::VersionManager::data_sent_confirmed(
        std::string const & app_name, std::string const & start_v,
        std::string const & end_v) {
    if (start_v != end_v) {
        graph.update_app_ref(app_name, end_v);
    }
}

std::set<std::string> version_manager::VersionManager::process_req_types(
        json && req_types) {
    std::set<std::string> result;
    if (req_types.is_null()) return result;

    auto & req_map = req_types.get_obj();

    auto req_it = req_map.begin();
    auto req_end = req_map.end();

    while (req_it != req_end) {

        auto & sorted_tp_list = req_it->second.get_ref<json::array_t &>(); //sorted by client code

        auto tp_list_it = sorted_tp_list.begin();
        auto tp_list_end = sorted_tp_list.end();

        auto type_names_it = type_names.begin();
        auto type_names_end = type_names.end();

        while (tp_list_it != tp_list_end && type_names_it != type_names_end) {
            int compare_result = tp_list_it->get_ref<json::string_t &>().compare(*type_names_it);
            if (!compare_result) {
                result.emplace(std::move(*tp_list_it));
                break;
            }
            if (compare_result < 0) {
                ++tp_list_it;
            } else {
                ++type_names_it;
            }
        }

        ++req_it;
    }

    return result;
}

std::tuple<json, std::string, std::string>
version_manager::VersionManager::retrieve_data_nomaintain(
        std::string version, json && req_types) {
    // Get only the types that are being requested.
    std::set<std::string> const & obtainable_types =
            req_types.is_null() ? type_names : process_req_types(
                std::move(req_types));

    // For all practical purposes this is the HEAD,
    // but concurrent writes can change the HEAD during the read
    // and therefore it is safer to explicitly obtain the version
    // uptil which deltas were read.
    std::string next_version = version;

    // The final delta that represents the change from
    // version to next_version
    json merged = json::object();

    auto graph_it = graph.begin(version);
    auto graph_end = graph.end();

    // Iterate from version to the HEAD version
    // HEAD is snapshotted at this point
    // Any concurrent writes that change will not affect graph_end.
    // The reader will receive those new updates only on next fetch/checkout
    while (graph_it && graph_it != graph_end) {
        auto[new_version, delta] = *graph_it;
        next_version = new_version;

        json package = json::object();
        auto & package_map = package.get_obj();

        auto const & delta_map = delta.get_obj();
        auto delta_it = delta_map.begin();
        auto delta_end = delta_map.end();

        auto type_it = obtainable_types.begin();
        auto type_end = obtainable_types.end();

        // Iterate over the types that are required and see if the delta
        // has the types. No need to pick up and send types that are not
        // requested.
        while (delta_it != delta_end && type_it != type_end) {
            int compare_result = delta_it->first.compare(*type_it);
            if (!compare_result) {
                package_map.emplace_hint(
                    package_map.end(), *type_it, delta_it->second);
                ++delta_it;
                ++type_it;
                continue;
            }
            if (compare_result < 0) {
                ++delta_it;
            } else {
                ++type_it;
            }
        }

        // There can be multiple deltas, but we send only one delta that is
        // a merge of all delta changes from version to HEAD.
        merged = utils::merge_state_delta(
            std::move(merged), std::move(package));

        ++graph_it;
    }

    return {std::move(merged), std::move(version), std::move(next_version)};

}

std::tuple<json, std::string, std::string>
version_manager::VersionManager::retrieve_data(
        const std::string & app_name, std::string version, json && req_types) {
    // prev_version == version (request version),
    // next_version == HEAD (final version),
    // data == delta changes from version to HEAD (the change)
    auto [data, prev_version, next_version] = retrieve_data_nomaintain(
        version, std::move(req_types));

    // Place tentative marker in references so that both
    // prev_version AND next_version are not garbage collected.
    // when data_sent_confirmed is called, prev_version is released for
    // any future garbage collection.
    graph.mark_app_ref_await(app_name, next_version);
    return {std::move(data), std::move(prev_version), std::move(next_version)};
}

void version_manager::VersionManager::resolve_conflict(
        const std::string & start_v, const std::string & end_v, json && package,
        bool from_external) {
    // Get all changes since start_v
    // If start_v is HEAD, return empty change, and old_v == new_v == start_v.
    // Note: this operation is not costly if old_v == new_v == start_v == HEAD
    auto [change, old_v, new_v] = retrieve_data_nomaintain(start_v, json());

    // if start_v == HEAD, there are no conflicts.
    if (new_v == start_v) {
        // Simply add this delta to the end of the graph.
        graph.continue_chain(start_v, end_v, std::move(package));
        return;
    }

    // Do the delta transformation and get the delta updates for the merge.
    auto [new_merge, conflict_merge] = operational_transform(
        start_v, std::move(change), package, from_external);
    // New uuid for the merged version.
    std::string merge_v = utils::get_uuid4();

    // Adding the update that was sent.
    graph.continue_chain(start_v, end_v, std::move(package));
    // Adding the update from the version sent to the merge version.
    graph.continue_chain(new_v, merge_v, std::move(new_merge));
    // Adding the update from the current HEAD to the merge version.
    graph.continue_chain(end_v, merge_v, std::move(conflict_merge));
    // Merge version becomes the end of the graph and the current HEAD.
}

std::pair<json, json> version_manager::VersionManager::operational_transform(
        std::string const & start,
        json && new_path, json const & conflicting_path,
        bool from_external) {

    json new_merge = json::object();
    json conflicting_merge = json::object();

    auto & new_merge_map = new_merge.get_obj();
    auto & conflicting_merge_map = conflicting_merge.get_obj();

    auto & new_path_map = new_path.get_obj();

    auto new_path_it = new_path_map.begin();
    auto new_path_end = new_path_map.end();

    auto const & conflicting_path_map = conflicting_path.get_obj();

    auto conflicting_path_it = conflicting_path_map.begin();
    auto conflicting_path_end = conflicting_path_map.end();

    while (new_path_it != new_path_end
               || conflicting_path_it != conflicting_path_end) {
        if (new_path_it == new_path_end) {
            new_merge_map.emplace_hint(
                new_merge_map.end(),
                conflicting_path_it->first,
                conflicting_path_it->second);
            ++conflicting_path_it;
            continue;
        }
        if (conflicting_path_it == conflicting_path_end) {
            conflicting_merge_map.emplace_hint(
                conflicting_merge_map.end(),
                new_path_it->first, std::move(new_path_it->second));
            ++new_path_it;
            continue;
        }
        std::string const & new_key = new_path_it->first;
        std::string const & conflicting_key = conflicting_path_it->first;
        int compare_result = new_key.compare(conflicting_key);
        if (!compare_result) {
            auto [tp_merge, tp_conf_merge] =
                    ot_on_type(
                            new_key,
                            start,
                            std::move(new_path_it->second),
                            conflicting_path_it->second,
                            from_external);
            new_merge_map.emplace_hint(new_merge_map.end(), new_key, std::move(tp_merge));
            conflicting_merge_map.emplace_hint(conflicting_merge_map.end(), new_key, std::move(tp_conf_merge));
            ++new_path_it;
            ++conflicting_path_it;
            continue;
        }
        if (compare_result < 0) {
            conflicting_merge_map.emplace_hint(conflicting_merge_map.end(), new_path_it->first, std::move(new_path_it->second));
            ++new_path_it;
            continue;
        } else {
            new_merge_map.emplace_hint(new_merge_map.end(), conflicting_path_it->first, conflicting_path_it->second);
            ++conflicting_path_it;
            continue;
        }
    }

    return {std::move(new_merge), std::move(conflicting_merge)};
}

std::pair<json, json> version_manager::VersionManager::ot_on_type(
        std::string const & tpname, std::string const & start,
        json && new_tp_change, json const & conflicting_tp_change,
        bool from_external) {

    json tp_new_merge = json::object();
    json tp_conflicting_merge = json::object();

    auto & new_merge_map = tp_new_merge.get_obj();
    auto & conflicting_merge_map = tp_conflicting_merge.get_obj();

    auto & new_tp_change_map = new_tp_change.get_obj();

    auto new_tp_change_it = new_tp_change_map.begin();
    auto new_tp_change_end = new_tp_change_map.end();

    auto const & conflicting_tp_change_map = conflicting_tp_change.get_obj();

    auto conflicting_tp_change_it = conflicting_tp_change_map.begin();
    auto conflicting_tp_change_end = conflicting_tp_change_map.end();

    while (new_tp_change_it != new_tp_change_end || conflicting_tp_change_it != conflicting_tp_change_end) {
        if (new_tp_change_it == new_tp_change_end) {
            new_merge_map.emplace_hint(new_merge_map.end(), conflicting_tp_change_it->first, conflicting_tp_change_it->second);
            ++conflicting_tp_change_it;
            continue;
        }
        if (conflicting_tp_change_it == conflicting_tp_change_end) {
            conflicting_merge_map.emplace_hint(conflicting_merge_map.end(), new_tp_change_it->first, std::move(new_tp_change_it->second));
            ++new_tp_change_it;
            continue;
        }
        std::string const & new_key = new_tp_change_it->first;
        std::string const & conflicting_key = conflicting_tp_change_it->first;
        int compare_result = new_key.compare(conflicting_key);
        if (!compare_result) {
            auto [obj_merge, obj_conf_merge] =
            ot_on_obj(
                    tpname,
                    new_key,
                    start,
                    std::move(new_tp_change_it->second),
                    json(conflicting_tp_change_it->second),
                    from_external);
            new_merge_map.emplace_hint(new_merge_map.end(), new_key, std::move(obj_merge));
            conflicting_merge_map.emplace_hint(conflicting_merge_map.end(), new_key, std::move(obj_conf_merge));
            ++new_tp_change_it;
            ++conflicting_tp_change_it;
            continue;
        }
        if (compare_result < 0) {
            conflicting_merge_map.emplace_hint(conflicting_merge_map.end(), new_tp_change_it->first, std::move(new_tp_change_it->second));
            ++new_tp_change_it;
            continue;
        } else {
            new_merge_map.emplace_hint(new_merge_map.end(), conflicting_tp_change_it->first, conflicting_tp_change_it->second);
            ++conflicting_tp_change_it;
            continue;
        }
    }

    return {std::move(tp_new_merge), std::move(tp_conflicting_merge)};
}

std::pair<json, json> version_manager::VersionManager::ot_on_obj(
        std::string const & tpname, std::string const & oid,
        std::string const & start, json && new_obj_change,
        json && conflicting_obj_change,
        bool from_external) {
    json obj_merge = json::object();
    json obj_conf_merge = json::object();

    int event_new = new_obj_change[rtype_tags::types][tpname];
    int event_conf = conflicting_obj_change[rtype_tags::types][tpname];
    using enums::Event;
    using utils::eq_in_int;

    if (eq_in_int(event_new, Event::New) && eq_in_int(event_conf, Event::New)) {
        if (have_custom_merge && have_custom_merge(tpname)) {
            std::vector<char> new_change_cbor;
            std::vector<char> conflicting_change_cbor;
            json::to_cbor(new_obj_change, new_change_cbor);
            json::to_cbor(conflicting_obj_change, conflicting_change_cbor);

            auto [obj_merge_temp, obj_conf_merge_temp] =
                    from_external ?
                    run_custom_merge(
                        start, tpname, oid, json(), std::move(new_obj_change),
                        std::move(conflicting_obj_change),
                        new_change_cbor, conflicting_change_cbor) :
                    run_custom_merge(
                        start, tpname, oid, json(),
                        std::move(conflicting_obj_change),
                        std::move(new_obj_change), new_change_cbor,
                        conflicting_change_cbor);
            obj_merge = std::move(obj_merge_temp);
            obj_conf_merge = std::move(obj_conf_merge_temp);
        } else {
            obj_conf_merge = dim_diff(
                tpname, std::move(conflicting_obj_change),
                std::move(new_obj_change));
        }
    } else if (eq_in_int(event_new, Event::New) && eq_in_int(event_conf, Event::Modification)) {
        throw std::runtime_error("Divergent modification received when object was created in the main line.");
    } else if (eq_in_int(event_new, Event::New) && eq_in_int(event_conf, Event::Delete)) {
        throw std::runtime_error("Divergent deletion received when object was created in the main line.");
    }else if (eq_in_int(event_new, Event::Modification) && eq_in_int(event_conf, Event::New)) {
        throw std::runtime_error("Divergent new received when object was modified in the main line.");
    }else if (eq_in_int(event_new, Event::Modification) && eq_in_int(event_conf, Event::Modification)) {
        if (have_custom_merge && have_custom_merge(tpname)) {
            std::vector<char> new_change_cbor;
            std::vector<char> conflicting_change_cbor;
            json::to_cbor(new_obj_change, new_change_cbor);
            json::to_cbor(conflicting_obj_change, conflicting_change_cbor);

            auto [obj_merge_temp, obj_conf_merge_temp] =
                    from_external ?
                    run_custom_merge(start, tpname, oid, json::object(), std::move(new_obj_change),
                                     std::move(conflicting_obj_change), new_change_cbor, conflicting_change_cbor) :
                    run_custom_merge(start, tpname, oid, json::object(), std::move(conflicting_obj_change),
                                     std::move(new_obj_change), new_change_cbor, conflicting_change_cbor);
            obj_merge = std::move(obj_merge_temp);
            obj_conf_merge = std::move(obj_conf_merge_temp);
        } else {
            auto [obj_merge_temp, obj_conf_merge_temp] =
                    resolve_modification(tpname, std::move(new_obj_change), std::move(conflicting_obj_change));
            obj_merge = std::move(obj_merge_temp);
            obj_conf_merge = std::move(obj_conf_merge_temp);
        }
    } else if (eq_in_int(event_new, Event::Modification) && eq_in_int(event_conf, Event::Delete)) {
        if (have_custom_merge && have_custom_merge(tpname)) {
            std::vector<char> new_change_cbor;
            std::vector<char> conflicting_change_cbor;
            json::to_cbor(new_obj_change, new_change_cbor);
            json::to_cbor(conflicting_obj_change, conflicting_change_cbor);

            auto [obj_merge_temp, obj_conf_merge_temp] =
                    from_external ?
                    run_custom_merge(start, tpname, oid, json::object(), std::move(new_obj_change), json(), new_change_cbor, conflicting_change_cbor) :
                    run_custom_merge(start, tpname, oid, json::object(), json(), std::move(new_obj_change), new_change_cbor, conflicting_change_cbor);
            obj_merge = std::move(obj_merge_temp);
            obj_conf_merge = std::move(obj_conf_merge_temp);
        } else {
            obj_merge = std::move(conflicting_obj_change);
        }
    } else if (eq_in_int(event_new, Event::Delete) && eq_in_int(event_conf, Event::New)) {
        throw std::runtime_error("Divergent new received when object was deleted in the main line.");
    } else if (eq_in_int(event_new, Event::Delete) && eq_in_int(event_conf, Event::Modification)) {
        if (have_custom_merge && have_custom_merge(tpname)) {
            std::vector<char> new_change_cbor;
            std::vector<char> conflicting_change_cbor;
            json::to_cbor(new_obj_change, new_change_cbor);
            json::to_cbor(conflicting_obj_change, conflicting_change_cbor);

            auto [obj_merge_temp, obj_conf_merge_temp] =
                    from_external ?
                    run_custom_merge(start, tpname, oid, json::object(), json(), std::move(conflicting_obj_change), new_change_cbor, conflicting_change_cbor) :
                    run_custom_merge(start, tpname, oid, json::object(), std::move(conflicting_obj_change), json(), new_change_cbor, conflicting_change_cbor);
            obj_merge = std::move(obj_merge_temp);
            obj_conf_merge = std::move(obj_conf_merge_temp);
        } else {
            obj_merge = std::move(conflicting_obj_change);
        }
    } else if (eq_in_int(event_new, Event::Delete) && eq_in_int(event_conf, Event::Delete)) {}

    return {std::move(obj_merge), std::move(obj_conf_merge)};
}

std::pair<json, json>
version_manager::VersionManager::cherrypick_jsons(json && lhs_json, json && rhs_json) {

    json lhs_result = json::object();
    json rhs_result = json::object();

    auto & lhs_result_map = lhs_result.get_obj();
    auto & rhs_result_map = rhs_result.get_obj();

    auto & lhs_json_map = lhs_json.get_obj();
    auto & rhs_json_map = rhs_json.get_obj();

    auto lhs_json_it = lhs_json_map.begin();
    auto lhs_json_end = lhs_json_map.end();

    auto rhs_json_it = rhs_json_map.begin();
    auto rhs_json_end = rhs_json_map.end();

    while (lhs_json_it != lhs_json_end || rhs_json_it != rhs_json_end) {
        if (lhs_json_it == lhs_json_end) {
            lhs_result_map.emplace_hint(lhs_result_map.end(), rhs_json_it->first, rhs_json_it->second);
            rhs_result_map.emplace_hint(rhs_result_map.end(), rhs_json_it->first, std::move(rhs_json_it->second));
            ++rhs_json_it;
            continue;
        }
        if (rhs_json_it == rhs_json_end) {
            lhs_result_map.emplace_hint(lhs_result_map.end(), lhs_json_it->first, lhs_json_it->second);
            rhs_result_map.emplace_hint(rhs_result_map.end(), lhs_json_it->first, std::move(lhs_json_it->second));
            ++lhs_json_it;
            continue;
        }
        std::string const & lhs_key = lhs_json_it->first;
        std::string const & rhs_key = rhs_json_it->first;
        int compare_result = lhs_key.compare(rhs_key);
        if (!compare_result) {
            lhs_result_map.emplace_hint(lhs_result_map.end(), lhs_key, std::move(lhs_json_it->second));
            rhs_result_map.emplace_hint(rhs_result_map.end(), rhs_key, std::move(rhs_json_it->second));
            ++lhs_json_it;
            ++rhs_json_it;
            continue;
        }
        if (compare_result < 0) {
            lhs_result_map.emplace_hint(lhs_result_map.end(), lhs_json_it->first, lhs_json_it->second);
            rhs_result_map.emplace_hint(rhs_result_map.end(), lhs_json_it->first, std::move(lhs_json_it->second));
            ++lhs_json_it;
            continue;
        } else {
            lhs_result_map.emplace_hint(lhs_result_map.end(), rhs_json_it->first, rhs_json_it->second);
            rhs_result_map.emplace_hint(rhs_result_map.end(), rhs_json_it->first, std::move(rhs_json_it->second));
            ++rhs_json_it;
            continue;
        }

    }

    return {std::move(lhs_result), std::move(rhs_result)};
}

std::pair<json, json> version_manager::VersionManager::resolve_modification_dims(json && new_dims, json && conflicting_dims) {
    json conf_np_diff = json::object();
    json new_np = json::object();

    auto & conf_np_diff_map = conf_np_diff.get_obj();
    auto & new_np_map = new_np.get_obj();

    auto & new_map = new_dims.get_obj();
    auto & conflicting_map = conflicting_dims.get_obj();

    auto new_it = new_map.begin();
    auto new_end = new_map.end();

    auto conflicting_it = conflicting_map.begin();
    auto conflicting_end = conflicting_map.end();

    while (new_it != new_end || conflicting_it != conflicting_end) {
        if (new_it == new_end) {
            conf_np_diff_map.emplace_hint(conf_np_diff_map.end(), conflicting_it->first, std::move(conflicting_it->second));
            ++conflicting_it;
            continue;
        }
        if (conflicting_it == conflicting_end) {
            new_np_map.emplace_hint(new_np_map.end(), new_it->first, std::move(new_it->second));
            ++new_it;
            continue;
        }
        std::string const & new_key = new_it->first;
        std::string const & conflicting_key = conflicting_it->first;
        int compare_result = new_key.compare(conflicting_key);
        if (!compare_result) {
            if (new_it->second != conflicting_it->second)
                conf_np_diff_map.emplace_hint(conf_np_diff_map.end(), conflicting_key, std::move(conflicting_it->second));
            ++new_it;
            ++conflicting_it;
            continue;
        }
        if (compare_result < 0) {
            new_np_map.emplace_hint(new_np_map.end(), new_it->first, std::move(new_it->second));
            ++new_it;
            continue;
        } else {
            conf_np_diff_map.emplace_hint(conf_np_diff_map.end(), conflicting_it->first, std::move(conflicting_it->second));
            ++conflicting_it;
            continue;
        }
    }

    return {std::move(conf_np_diff), std::move(new_np)};
}

std::pair<json, json>
version_manager::VersionManager::resolve_modification(std::string const & dtpname, json && new_change,
                                                      json && conflicting_change) {

    json obj_merge = json::object();
    json obj_conflicting_merge = json::object();

    auto & obj_merge_map = obj_merge.get_obj();
    auto & obj_conflicting_merge_map = obj_conflicting_merge.get_obj();

    auto [conf_np_diff, new_np] = resolve_modification_dims(std::move(new_change[rtype_tags::dims]), std::move(conflicting_change[rtype_tags::dims]));

    auto [conf_type, new_type] = cherrypick_jsons(std::move(conflicting_change[rtype_tags::types]), std::move(new_change[rtype_tags::types]));

    new_type[dtpname] = static_cast<int>(enums::Event::Modification);

    obj_merge_map.emplace_hint(obj_merge_map.end(), rtype_tags::dims, std::move(conf_np_diff));
    obj_merge_map.emplace_hint(obj_merge_map.end(), rtype_tags::types, std::move(conf_type));

    obj_conflicting_merge_map.emplace_hint(obj_conflicting_merge_map.end(), rtype_tags::dims, std::move(new_np));
    obj_conflicting_merge_map.emplace_hint(obj_conflicting_merge_map.end(), rtype_tags::types, std::move(new_type));

    return {std::move(obj_merge), std::move(obj_conflicting_merge)};
}

json version_manager::VersionManager::dim_diff(std::string const & dtpname, json && original_json, json && new_json) {
    if (original_json.is_null() || !original_json.get_obj().count(dtpname))
        return std::move(new_json);

    json change_dims = json::object();

    json change_json = json::object();

    auto & change_map = change_json.get_obj();

    auto & change_dims_map = change_dims.get_obj();

    auto const & original_dims_map = original_json[rtype_tags::dims].get_obj();
    auto & new_dims_map = new_json[rtype_tags::dims].get_obj();

    auto original_it = original_dims_map.begin();
    auto original_end = original_dims_map.end();

    auto new_it = new_dims_map.begin();
    auto new_end = new_dims_map.end();

    while (new_it != new_end) {
        if (original_it == original_end) {
            change_dims_map.emplace_hint(change_dims_map.end(), new_it->first, std::move(new_it->second));
            ++new_it;
            continue;
        }
        int compare_result = original_it->first.compare(new_it->first);
        if (!compare_result) {
            if (original_it->second != new_it->second)
                change_dims_map.emplace_hint(change_dims_map.end(), new_it->first, std::move(new_it->second));
            ++original_it;
            ++new_it;
            continue;
        }
        if (compare_result < 0) {
            ++original_it;
            continue;
        } else {
            change_dims_map.emplace_hint(change_dims_map.end(), new_it->first, std::move(new_it->second));
            ++new_it;
            continue;
        }
    }

    change_map.emplace_hint(change_map.end(), rtype_tags::dims, std::move(change_dims));
    change_map.emplace_hint(change_map.end(), rtype_tags::types,
            utils::update_json(std::move(original_json[rtype_tags::types]), std::move(new_json[rtype_tags::types])));

    return std::move(change_map);
}

version_graph::Graph & version_manager::VersionManager::get_g() {
    return graph;
}

void version_manager::VersionManager::wait_graph_change(std::string const & version) {
    std::unique_lock write_lock(write_mutex);
    graph_changed.wait(write_lock, [&version, this]{return graph.is_dead || graph.get_head_tag() != version; });
}

bool
version_manager::VersionManager::wait_graph_change_for(std::string const & version, const float_sec_t & period) {
    std::unique_lock write_lock(write_mutex);
    return graph_changed.wait_for(write_lock, period, [&version, this]{ return graph.is_dead || graph.get_head_tag() != version; });
}

//json version_manager::VersionManager::read_dimensions(std::string const & version, std::string const & dtpname,
//                                                      std::string const & oid) {
//    json result = json::object();
//
//    auto result_map = result.get_obj();
//
//    std::unordered_set<std::string> dims = type_dim_map[dtpname];
//
//    bool done = false;
//    auto graph_it = graph.rbegin(version);
//    auto graph_end = graph.rend();
//
//    while (graph_it != graph_end && !done) {
//        auto const & change = (*graph_it).second.get_obj();
//        auto type_it = change.find(dtpname);
//        if (type_it != change.end()) {
//            auto const & type_obj = type_it->second.get_obj();
//            auto obj_it = type_obj.find(oid);
//            if (obj_it != type_obj.end()) {
//                auto const& obj_obj = obj_it->second.get_obj();
//
//
//                auto obj_types_it = obj_obj.find(rtype_tags::types);
//                if (obj_types_it != obj_obj.end()) {
//                    auto const & obj_types_obj = obj_types_it->second.get_obj();
//                    auto type_change_it = obj_types_obj.find(dtpname);
//                    if (
//                            type_change_it != obj_types_obj.end() &&
//                            utils::eq_in_int(type_change_it->second, enums::Event::Delete)
//                            )
//                        done = true;
//                }
//
//                auto obj_dims_it = obj_obj.find(rtype_tags::dims);
//                if (obj_dims_it != obj_obj.end()) {
//                    auto const & obj_dims_obj = obj_dims_it->second.get_obj();
//
//                    auto dim_it = obj_dims_obj.begin();
//                    auto dim_end = obj_dims_obj.end();
//                    while (dim_it != dim_end) {
//                        auto const & dimname = dim_it->first;
//                        auto dim_set_it = dims.find(dimname);
//                        if (dim_set_it != dims.end()) {
//                            result_map.emplace_hint(result_map.end(), dimname, dim_it->second);
//                            dims.erase(dim_set_it);
//                            done = dims.empty();
//                        }
//                    }
//                }
//            }
//        }
//        ++graph_it;
//    }
//    return std::move(result);
//}

std::pair<json, json>
version_manager::VersionManager::run_custom_merge(const std::string & version, const std::string & dtpname,
                                                  const std::string & oid, json && original_obj, json && new_obj,
                                                  json && conflicting_obj, std::vector<char> const & new_change_cbor,
                                                  std::vector<char> const & conflicting_change_cbor) {

    json empty_dims = json::object();

    using dimset_t = std::unordered_set<std::string>;

    bool original_done = original_obj.is_null();
    bool new_done = new_obj.is_null();
    bool conflicting_done = conflicting_obj.is_null();

    dimset_t const & all_dims = type_dim_map[dtpname];
    dimset_t original_dims = original_done ? dimset_t() : all_dims;
    dimset_t new_dims = new_done ? dimset_t() : all_dims;
    dimset_t conflicting_dims = conflicting_done ? dimset_t() : all_dims;

    auto & original_dim_obj = original_done ? empty_dims:
                          (original_obj.count(rtype_tags::dims) ? original_obj[rtype_tags::dims] : original_obj);

    auto & new_dim_obj = new_done ? empty_dims :
                          (new_obj.count(rtype_tags::dims) ? new_obj[rtype_tags::dims] : new_obj);
    
    auto & conflicting_dim_obj = conflicting_done ? empty_dims:
                          (conflicting_obj.count(rtype_tags::dims) ? conflicting_obj[rtype_tags::dims] : conflicting_obj);

    auto & original_map = original_dim_obj.get_obj();
    auto & new_map = new_dim_obj.get_obj();
    auto & conflicting_map = conflicting_dim_obj.get_obj();

    if (!original_done) {
        auto original_it = original_map.begin();
        auto original_end = original_map.end();
        while (original_it != original_end) {
            if (!all_dims.count(original_it->first)) {
                original_it = original_map.erase(original_it);
                continue;
            }
            original_dims.erase(original_it->first);
            ++original_it;
        }
        original_done = original_dims.empty();
    }
    
    if (!new_done) {
        auto new_it = new_map.begin();
        auto new_end = new_map.end();
        while (new_it != new_end) {
            if (!all_dims.count(new_it->first)) {
                new_it = new_map.erase(new_it);
                continue;
            }
            new_dims.erase(new_it->first);
            ++new_it;
        }
        new_done = new_dims.empty();
    }
    
    if (!conflicting_done) {
        auto conflicting_it = conflicting_map.begin();
        auto conflicting_end = conflicting_map.end();
        while (conflicting_it != conflicting_end) {
            if (!all_dims.count(conflicting_it->first)) {
                conflicting_it = conflicting_map.erase(conflicting_it);
                continue;
            }
            conflicting_dims.erase(conflicting_it->first);
            ++conflicting_it;
        }
        conflicting_done = conflicting_dims.empty();
    }
    
    auto graph_it = graph.rbegin(version);
    auto graph_end = graph.rend();

    while (graph_it && graph_it != graph_end && !(original_done && new_done && conflicting_done)) {
        auto const & change = (*graph_it).second.get_obj();
        auto type_it = change.find(dtpname);
        if (type_it != change.end()) {
            auto const & type_obj = type_it->second.get_obj();
            auto obj_it = type_obj.find(oid);
            if (obj_it != type_obj.end()) {
                auto const& obj_obj = obj_it->second.get_obj();

                auto obj_dims_it = obj_obj.find(rtype_tags::dims);
                if (obj_dims_it != obj_obj.end()) {
                    auto const & obj_dims_obj = obj_dims_it->second.get_obj();

                    auto dim_it = obj_dims_obj.begin();
                    auto dim_end = obj_dims_obj.end();
                    while (dim_it != dim_end && !(original_done && new_done && conflicting_done)) {
                        auto const & dimname = dim_it->first;

                        if (!original_done) {
                            auto dim_set_it = original_dims.find(dimname);
                            if (dim_set_it != original_dims.end()) {
                                original_map.emplace_hint(original_map.end(), dimname, dim_it->second);
                                original_dims.erase(dim_set_it);
                                original_done = original_dims.empty();
                            }
                        }
                        
                        if (!new_done) {
                            auto dim_set_it = new_dims.find(dimname);
                            if (dim_set_it != new_dims.end()) {
                                new_map.emplace_hint(new_map.end(), dimname, dim_it->second);
                                new_dims.erase(dim_set_it);
                                new_done = new_dims.empty();
                            }
                        }
                        
                        if (!conflicting_done) {
                            auto dim_set_it = conflicting_dims.find(dimname);
                            if (dim_set_it != conflicting_dims.end()) {
                                conflicting_map.emplace_hint(conflicting_map.end(), dimname, dim_it->second);
                                conflicting_dims.erase(dim_set_it);
                                conflicting_done = conflicting_dims.empty();
                            }
                        }
                        
                        ++dim_it;
                    }
                }
            }
        }
        ++graph_it;
    }

    return custom_merge_function(dtpname, oid,
            original_obj.is_null() ? original_obj : original_dim_obj,
            new_obj.is_null() ? new_obj : new_dim_obj,
            conflicting_obj.is_null() ? conflicting_obj : conflicting_dim_obj,
            new_change_cbor,
            conflicting_change_cbor);
}

