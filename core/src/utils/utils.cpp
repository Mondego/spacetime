#include <utils/utils.h>
#include <utils/enums.h>

#include <sstream>

namespace {
    using utils::eq_in_int;

    json merge_object_delta(std::string const & dtpname, json && old_change, json && new_change, int old_type,
                            int new_type) {
        using enums::Event;

        if (old_change.is_null()) return std::move(new_change);
        if (
                eq_in_int(new_type, Event::Delete) ||
                (eq_in_int(old_type, Event::Delete) && eq_in_int(new_type, Event::New))
                )
            return std::move(new_change);
        if (
                !(eq_in_int(new_type, Event::Modification) ||
                  (eq_in_int(new_type, Event::New) && eq_in_int(old_type, Event::New))))
            throw std::runtime_error("Not sure why the new change does not have modification.");
        if (eq_in_int(old_type, Event::Delete)) return std::move(old_change);

        auto & old_type_obj = old_change[rtype_tags::types].get_obj();
        auto & new_type_obj = new_change[rtype_tags::types].get_obj();

        auto old_it = old_type_obj.begin();
        auto old_end = old_type_obj.end();
        auto new_it = new_type_obj.begin();
        auto new_end = new_type_obj.end();

        while (old_it != old_end || new_it != new_end) {
            if (new_it == new_end) {
                break;
            }
            if (old_it == old_end) {
                old_type_obj.emplace_hint(old_it, new_it->first, std::move(new_it->second));
                ++new_it;
                continue;
            }
            auto const & old_key = old_it->first;
            auto const & new_key = new_it->first;

            int compare_result = old_key.compare(new_key);
            if (!compare_result) {
                if (eq_in_int(new_it->second, Event::Delete) || !eq_in_int(old_it->second, Event::New))
                    old_it->second = std::move(new_it->second);
                ++new_it;
                ++old_it;
                continue;
            }
            if (compare_result < 0) {
                ++old_it;
                continue;
            } else {
                old_type_obj.emplace_hint(old_it, new_it->first, std::move(new_it->second));
                ++new_it;
            }
        }

        utils::update_json(old_change[rtype_tags::dims], std::move(new_change[rtype_tags::dims]));
        return std::move(old_change);
    }


    json merge_objectlist_delta(std::string const & dtpname, json && old_change, json && new_change, bool delete_it) {
        using enums::Event;

        auto & old_obj = old_change.get_obj();
        auto & new_obj = new_change.get_obj();

        json const & a = old_change;
        auto & ooo = a.get_obj();

        auto old_it = old_obj.begin();
        auto old_end = old_obj.end();
        auto new_it = new_obj.begin();
        auto new_end = new_obj.end();

        while (old_it != old_end || new_it != new_end) {
            if (new_it == new_end) {
                break;
            }
            int new_type = static_cast<int>(new_it->second[rtype_tags::types][dtpname]);
            if (old_it == old_end) {
                if (!(delete_it && eq_in_int(new_type, Event::Delete)))
                    old_obj.emplace_hint(old_it, new_it->first, std::move(new_it->second));
                ++new_it;
                continue;
            }
            int old_type = static_cast<int>(old_it->second[rtype_tags::types][dtpname]);
            auto const & old_key = old_it->first;
            auto const & new_key = new_it->first;
            int compare_result = old_key.compare(new_key);
            if (!compare_result) {
                if
                        (eq_in_int(new_type, Event::Delete) &&
                         (eq_in_int(old_type, Event::New) || delete_it)) {
                    old_it = old_obj.erase(old_it);
                    ++new_it;
                    continue;
                }
                old_it->second = merge_object_delta(dtpname, std::move(old_it->second), std::move(new_it->second),
                                                    old_type, new_type);
                ++old_it;
                ++new_it;
                continue;
            }
            if (compare_result < 0) {
                ++old_it;
                continue;
            } else {
                if (!(delete_it && eq_in_int(new_type, Event::Delete)))
                    old_obj.emplace_hint(old_it, new_it->first, std::move(new_it->second));
                ++new_it;
                continue;
            }
        }
        return std::move(old_change);
    }
}

namespace utils {


    json update_json(json && lhs, json && rhs) {
        update_json(lhs, std::move(rhs));
        return std::move(lhs);
    }

    void update_json(json & lhs, json && rhs) {
        if (lhs.is_null()) {
            lhs = std::move(rhs);
            return;
        }
        if (rhs.is_null()) return;

        auto & lhs_obj = lhs.get_obj();
        auto & rhs_obj = rhs.get_obj();

        auto lhs_it = lhs_obj.begin();
        auto lhs_end = lhs_obj.end();
        auto rhs_it = rhs_obj.begin();
        auto rhs_end = rhs_obj.end();

        while (lhs_it != lhs_end || rhs_it != rhs_end) {
            if (rhs_it == rhs_end) {
                break;
            }
            if (lhs_it == lhs_end) {
                lhs_obj.emplace_hint(lhs_it, rhs_it->first, std::move(rhs_it->second));
                ++rhs_it;
                continue;
            }
            auto const & lhs_key = lhs_it->first;
            auto const & rhs_key = rhs_it->first;
            int compare_result = lhs_key.compare(rhs_key);
            if (!compare_result) {
                lhs_it->second = std::move(rhs_it->second);
                ++lhs_it;
                ++rhs_it;
                continue;
            }
            if (compare_result < 0) {
                ++lhs_it;
                continue;
            } else {
                lhs_obj.emplace_hint(lhs_it, rhs_key, std::move(rhs_it->second));
                ++rhs_it;
            }
        }
    }

    json merge_state_delta(json && old_change, json && new_change, bool delete_it) {
        if (old_change.is_null()) return std::move(new_change);
        if (new_change.is_null()) return std::move(old_change);

        auto & old_obj = old_change.get_obj();
        auto & new_obj = new_change.get_obj();

        auto old_it = old_obj.begin();
        auto old_end = old_obj.end();

        auto new_it = new_obj.begin();
        auto new_end = new_obj.end();

        while (old_it != old_end || new_it != new_end) {
            if (old_it == old_end) {
                old_obj.emplace_hint(old_it, new_it->first, std::move(new_it->second));
                ++new_it;
                continue;
            }
            if (new_it == new_end) {
                break;
            }
            auto const & old_key = old_it->first;
            auto const & new_key = new_it->first;
            int compare_result = old_key.compare(new_key);
            if (!compare_result) {
                old_it->second = merge_objectlist_delta(
                        old_key,
                        std::move(old_it->second),
                        std::move(new_it->second),
                        delete_it);
                ++old_it;
                ++new_it;
                continue;
            }
            if (compare_result < 0) {
                ++old_it;
                continue;
            } else {
                old_obj.emplace_hint(old_it, new_key, std::move(new_it->second));
                ++new_it;
            }
        }

        return std::move(old_change);
    }

    std::string get_uuid4() {
        using namespace uuid;
        std::ostringstream ss;
        int i;
        ss << std::hex;
        for (i = 0; i < 8; i++) {
            ss << dis(gen);
        }
        ss << "-";
        for (i = 0; i < 4; i++) {
            ss << dis(gen);
        }
        ss << "-4";
        for (i = 0; i < 3; i++) {
            ss << dis(gen);
        }
        ss << "-";
        ss << dis2(gen);
        for (i = 0; i < 3; i++) {
            ss << dis(gen);
        }
        ss << "-";
        for (i = 0; i < 12; i++) {
            ss << dis(gen);
        }
        return ss.str();
    }
}
