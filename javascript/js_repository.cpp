#include <emscripten.h>
#include <emscripten/bind.h>
#include <iostream>
#include <nlohmann/json.hpp>
#include "../core/include/repository.h"
#include "../core/include/utils/utils.h"
#include <unistd.h>
#include <random>
#include <sstream>
// #include <Python.h>

using namespace emscripten;
using utils::json;

static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);
static std::uniform_int_distribution<> dis2(8, 11);

class JsRepository
{
public:
    JsRepository() {}

    bool fetch()
    {

        new_repo->fetch();
        auto [fdata, fstart_v, fend_v] = new_repo->get_manager_ref().retrieve_data(s_raw_app_name, "ROOT");
        std::cout << "data: " << fdata.dump(4) << "; start, end = " << fstart_v << ", " << fend_v << std::endl;
        if(new_repo->wasm_repo_commitable) {
            // new_repo->connect_to("127.0.0.1", 9000);
        }
        return new_repo->wasm_repo_commitable;
    }

    void commit()
    {
        if (new_repo->wasm_repo_commitable)
        {
            // std::cout << "Finished fetch" << std::endl;
            auto [fdata, fstart_v, fend_v] = new_repo->get_manager_ref().retrieve_data(s_raw_app_name, "ROOT");
            // std::cout << "data: " << data.dump(4) << "; start, end = " << start_v << ", " << end_v;
            if (p_start_v.empty())
            {
                p_start_v = fend_v;
                p_end_v = generate_uuid_v4();
            }
            std::cout << "p_start_v: " << p_start_v << std::endl;
            std::cout << "p_end_v: " << p_end_v << std::endl;
            count_diff = "{\"datamodel.Counter\": {\"countDocument\": {\"dims\": {\"count\": {\"type\": 0, \"value\":" + to_string(iterations) + "}}, \"types\": {\"datamodel.Counter\": 1}}}}";
            // std::cout << count_diff << std::endl;
            received_diff = json::parse(count_diff);
            new_repo->get_manager_ref().receive_data(s_raw_app_name, p_start_v, p_end_v, std::move(received_diff), true);
            auto [data, start_v, end_v] = new_repo->get_manager_ref().retrieve_data(s_raw_app_name, "ROOT");
            std::cout << "Committed data: " << data.dump(4) << "; start, end = " << start_v << ", " << end_v;
            p_start_v = p_end_v;
            p_end_v = generate_uuid_v4();
            iterations += 1;
            if (iterations > 100)
                emscripten_force_exit(0);
        }
    }
    bool push()
    {
        new_repo->push();
        if(new_repo->wasm_repo_commitable) {
            // new_repo->connect_to("127.0.0.1", 9000);
        }
        return new_repo->wasm_repo_commitable;
    }

    int Repository_init(std::string raw_app_name, std::string type_info)
    {
        s_raw_app_name = raw_app_name;
        json type_info_json = json::parse(type_info);
        std::cout << type_info_json.dump(4) << std::endl;
        enums::Autoresolve autoresolve = enums::Autoresolve::FullResolve;
        try
        {

            new_repo =
                std::make_unique<repository::Repository>(
                    raw_app_name, nullptr,
                    nullptr, std::move(type_info_json),
                    autoresolve);
            std::cout << "created repo" << std::endl;
            new_repo->connect_to("127.0.0.1", 9000);
            std::cout << "Just connected" << std::endl;
            ;
            int is_connected = new_repo->is_connected();
            std::cout << "Is connected" << is_connected << std::endl;
            // emscripten_set_main_loop(&JsRepository::main_loop, 60, 0);
            // actual_self->m_repo = std::move(new_repo);
        }
        catch (std::exception &ex)
        {
            std::cout << "In exception!" << std::endl;
            std::cout << ex.what() << std::endl;
            return -1;
        }
        return 0;
    }

    bool w_is_connected()
    {
        return new_repo->is_connected();
    }

    std::string w_retreive_data(std::string appname, std::string version)
    {
        try
        {
            std::cout << "/// In w_retreive data, shouldn't happen!" << std::endl;
            auto [rtdata, rtstart_v, rtend_v] =
                new_repo->get_manager_ref().retrieve_data(appname, version);
            std::cout << "/// After retreive data, shouldn't happen!" << std::endl;
            json packed_data =
                {std::move(rtdata), std::move(rtstart_v), std::move(rtend_v)};

            std::cout << "/// After packed data!" << std::endl;
            std::cout << "js_repo: ret_data: " << packed_data.dump() << std::endl;

            return packed_data.dump();
        }
        catch (std::exception &ex)
        {
            std::cout << "Exception at retreive data: " << ex.what() << std::endl;
        }
        return "";
    }

    bool w_receive_data(std::string appname, std::string start_v, std::string end_v, std::string diff_data, bool from_external = true)
    {
        json diff_data_json;
        try
        {
            std::cout << "js_repo: diff data- " << diff_data << std::endl;
            if (diff_data.compare("{}") == 0)
            {
                diff_data_json = json({});
            }
            else
            {
                diff_data_json = json::parse(diff_data);
            }
        }
        catch (const std::exception &exc)
        {
            std::cout << "Exc from receive data1: " << exc.what() << std::endl;
        }
        try
        {
            if (new_repo->wasm_repo_commitable)
            {
                if (new_repo->get_manager_ref().receive_data(
                        appname, start_v, end_v,
                        std::move(diff_data_json), from_external))
                {
                    return true;
                }
            }
            return false;
        }
        catch (const std::exception &exc)
        {
            std::cout << "Exc from receive data2: " << exc.what() << std::endl;
        }
    }

    void w_data_sent_confirmed(std::string app_name, std::string start_v, std::string end_v)
    {

        try
        {
            new_repo->get_manager_ref().data_sent_confirmed(
                app_name, start_v, end_v);
        }
        catch (std::exception &ex)
        {
            std::cout << "w_data_sent_confirmed exception: " << ex.what() << std::endl;
        }
    }

private:
    std::unique_ptr<repository::Repository> new_repo;
    std::string s_raw_app_name;
    int iterations = 1;
    std::string p_start_v;
    std::string p_end_v;
    std::string count_diff;
    json received_diff;
    std::string generate_uuid_v4()
    {
        std::stringstream ss;
        int i;
        ss << std::hex;
        for (i = 0; i < 8; i++)
        {
            ss << dis(gen);
        }
        ss << "-";
        for (i = 0; i < 4; i++)
        {
            ss << dis(gen);
        }
        ss << "-4";
        for (i = 0; i < 3; i++)
        {
            ss << dis(gen);
        }
        ss << "-";
        ss << dis2(gen);
        for (i = 0; i < 3; i++)
        {
            ss << dis(gen);
        }
        ss << "-";
        for (i = 0; i < 12; i++)
        {
            ss << dis(gen);
        };
        return ss.str();
    }
};

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

EMSCRIPTEN_BINDINGS(my_module)
{
    class_<JsRepository>("JsRepository")
        .constructor<>()
        .function("Repository_init", &JsRepository::Repository_init)
        .function("fetch", &JsRepository::fetch)
        .function("commit", &JsRepository::commit)
        .function("push", &JsRepository::push)
        .function("w_is_connected", &JsRepository::w_is_connected)
        .function("w_retreive_data", &JsRepository::w_retreive_data)
        .function("w_receive_data", &JsRepository::w_receive_data);
        //.function("w_data_sent_confirmed", &JsRepository::w_data_sent_confirmed);
}
