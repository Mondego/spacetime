#include <emscripten/bind.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include "../core/include/repository.h"
#include "../core/include/utils/utils.h"
// #include <Python.h>

using namespace emscripten;
using utils::json;

/*
bool DoNothing_custom_merge(std::string const & x) {
    return true;
}

std::pair<json, json> DoNothing_custom_merge2(
                    std::string const & dtpname,
                    std::string const & oid,
                    json const & original_dims,
                    json const & new_dims,
                    json const & conflicting_dims,
                    std::vector<char> const & new_change_cbor,
                    std::vector<char> const & conflicting_change_cbor
            ) {
                json p1 = json::parse("{}");
                json p2 = json::parse("{}");
                return std::make_pair<0, 0>
}
*/

static int Repository_init(std::string raw_app_name, std::string type_info) {
    json type_info_json = json::parse(type_info);
    std::cout << type_info_json.dump(4) << std::endl;
    enums::Autoresolve autoresolve = enums::Autoresolve::FullResolve;
    try {

        std::unique_ptr<repository::Repository> new_repo = 
            std::make_unique<repository::Repository>(
                raw_app_name, nullptr,
                nullptr, std::move(type_info_json),
                autoresolve);

        new_repo->connect_to("127.0.0.1", 9000);
        int is_connected = new_repo->is_connected();
        std::cout << "Is connected" << is_connected << std::endl;
        /*
        new_repo->fetch();
        auto [data, start_v, end_v] =
            new_repo->get_manager_ref().retrieve_data(raw_app_name, "ROOT");
        std:: cout << "data: " << data << "; start, end = " << start_v << end_v;
        
        // actual_self->m_repo = std::move(new_repo);
        */

    } catch (std::exception & ex) {
        std::cout << "In exception!" << std::endl;
        std::cout << ex.what() << std::endl;
        return -1;
    }
    return 0;
}

EMSCRIPTEN_BINDINGS(my_module) {
    function("Repository_init", &Repository_init);
}
