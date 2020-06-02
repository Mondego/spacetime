//
// Created by zhazha on 1/8/20.
//

#ifndef LIBTRY_UTILS_H
#define LIBTRY_UTILS_H

#include <cstddef>
#include <random>
#include <functional>
#include <nlohmann/json.hpp>

namespace rtype_tags {
    inline static const std::string types = "types";
    inline static const std::string dims = "dims";
}

namespace {

    using json = nlohmann::basic_json<std::map>;

    json merge_objectlist_delta(std::string const & dtpname, json && old_change, json && new_change, bool delete_it = false);

    json merge_object_delta(std::string const &dtpname, json &&old_change, json &&new_change, int old_type, int new_type);

//    void dump_string_cbor(std::string const & value, std::vector<char> & target);

    namespace uuid {
        static std::random_device              rd;
        static std::mt19937                    gen(rd());
        static std::uniform_int_distribution<> dis(0, 15);
        static std::uniform_int_distribution<> dis2(8, 11);
    }

}

namespace utils {

    template<typename T1, typename T2>
    inline bool eq_in_int(T1 const & lhs, T2 const & rhs) {
        return static_cast<int>(lhs) == static_cast<int>(rhs);
    }

    using json = nlohmann::basic_json<std::map>;

    json update_json(json && lhs, json && rhs);

    void update_json(json & lhs, json && rhs);

    json merge_state_delta(json &&old_change, json &&new_change, bool delete_it = false);

    std::string get_uuid4();
}

#endif //LIBTRY_UTILS_H
