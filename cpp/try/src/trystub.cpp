#include <iostream>
#include <utils/rwlock.h>
#include <utils/utils.h>
#include <thread>
#include <vector>
#include <memory>
#include <version_graph.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <fstream>

#include <asio.hpp>
#include <utils/enums.h>
#include <connectors/async_server.h>
#include <version_manager.h>
#include <repository.h>
#include <utils/debug_logger.h>

rwlock::rwlock_write rwlockobj;

void read(){
    for (int i=0; i<10; i++){
        auto rlock = rwlockobj.get_read_lock();
        std::cout << "read begin" <<std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::cout << "read end" <<std::endl;
    }
}

void write(){
    for (int i=0; i<5; i++){
        {
            auto wlock0 = rwlockobj.get_write_lock();

            rwlock::rwlock_write::write_lock wlock(std::move(wlock0));
            std::cout << "write begin" <<std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            std::cout << "write end" <<std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
}

nlohmann::json* makeJ(nlohmann::json::initializer_list_t init){
    return new nlohmann::json(init);
};

template <typename T1, typename T2>
void assertEq(T1 && lhs, T2 && rhs) {
    if (lhs != rhs)
        std::cout << "Assert fail, lhs: " << lhs <<  " rhs :" << rhs << std::endl;
}


    std::string carname = "Car_NAME";
    json package1 = {{
        carname, {{"0", {{
            "dims", {{
                "oid", {{
                    "type", 0
                }, {
                    "value", 0
                }}
                }, {
                "xvel", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }, {
                "yvel", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }, {
                "xpos", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }, {
                "ypos", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }
            }
        }, {
            "types", {{
                carname, static_cast<int>(enums::Event::New)
            }}
        }}}}
    }};

    json package2 = {{
        carname, {{"0", {{
            "dims", {{
                "xvel", {{
                    "type", 0
                    }, {
                    "value", 1
                }}
            }}
        }, {
            "types", {{
                carname, static_cast<int>(enums::Event::Modification)
            }}
        }}}}
    }};

    json package3 = {{
        carname, {{"0", {{
            "dims", {{
                "oid", {{
                    "type", 0
                }, {
                    "value", 0
                }}
                }, {
                "xvel", {{
                    "type", 0
                    }, {
                    "value", 1
                }}
                }, {
                "yvel", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }, {
                "xpos", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }, {
                "ypos", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }
            }
        }, {
            "types", {{
                carname, static_cast<int>(enums::Event::New)
            }}
        }}}}
    }};

    json package4 = {{
        carname, {{"0", {{
            "dims", {{
                "yvel", {{
                    "type", 0
                    }, {
                    "value", 1
                }}
            }}
        }, {
            "types", {{
                carname, static_cast<int>(enums::Event::Modification)
            }}
        }}}}
    }};

    json package5 = {{
        carname, {{"0", {{
            "dims", {{
                "oid", {{
                    "type", 0
                }, {
                    "value", 0
                }}
                }, {
                "xvel", {{
                    "type", 0
                    }, {
                    "value", 1
                }}
                }, {
                "yvel", {{
                    "type", 0
                    }, {
                    "value", 1
                }}
                }, {
                "xpos", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }, {
                "ypos", {{
                    "type", 0
                    }, {
                    "value", 0
                }}
                }
            }
        }, {
            "types", {{
                carname, static_cast<int>(enums::Event::New)
            }}
        }}}}
    }};

    void test1(){
        version_manager::VersionManager vm{
                "TEST",
                //version_manager::VersionManager::have_custom_merge_t(),
                nullptr,
                //version_manager::VersionManager::custom_merge_function_t(),
                nullptr,
                {{carname, {}}}
        };

        auto & g = vm.get_g();

        vm.receive_data("TEST_APP1", "ROOT", "0", json(package1));

        auto [r_package1, r_s1, r_e1] = vm.retrieve_data("TEST_APP1", "ROOT");

        assertEq(package1, r_package1);
        assertEq("ROOT", r_s1);
        assertEq("0", r_e1);

        vm.receive_data("TEST_APP2", "0", "1", json(package2));

        auto [r_package2, r_s2, r_e2] = vm.retrieve_data("TEST_APP2", "ROOT");

        assertEq(package3, r_package2);
        assertEq("ROOT", r_s2);
        assertEq("1", r_e2);

        auto [r_package3, r_s3, r_e3] = vm.retrieve_data("TEST_APP2", "0");

        assertEq(package2, r_package3);
        assertEq("0", r_s3);
        assertEq("1", r_e3);



        vm.receive_data("TEST_APP1", "0", "2", json(package4));

        auto [r_package4, r_s4, r_e4] = vm.retrieve_data("TEST_APP1", "ROOT");

        assertEq(package5, r_package4);
        assertEq("ROOT", r_s4);
        assertEq(g.get_head_tag(), r_e4);

        auto [r_package5, r_s5, r_e5] = vm.retrieve_data("TEST_APP1", "1");

        assertEq(package4, r_package5);
        assertEq("1", r_s5);
        assertEq(g.get_head_tag(), r_e5);

        auto [r_package6, r_s6, r_e6] = vm.retrieve_data("TEST_APP1", "2");

        assertEq(package2, r_package6);
        assertEq("2", r_s6);
        assertEq(g.get_head_tag(), r_e6);


        std::cout << "done" << std::endl;
    }

int main(int argc, char * argv[]){
    /*auto a  = makeJ({{"a", true}, {"b", 123}, {"c", 0.12}, {"d", {{"everything", 42}}}});
    std::vector<std::thread> ts1;
    for (int i=0; i<5; i++)
        ts1.emplace_back(std::thread(write));
    std::vector<std::thread> ts;
    for (int i=0; i<10; i++)
        ts.emplace_back(std::thread(read));

    for (auto & t: ts)
        t.join();
    for (auto & t: ts1)
        t.join();*/
/*
    version_graph::Graph g;
    std::cout << g.nodes["ROOT"]->tag << std::endl;
    g.head = g.tail = nullptr;
    std::cout << "not yet" << std::endl;
    g.nodes.clear();
    std::cout << "now?" << std::endl;
    auto & x = *a;
    std::cout << x.dump(4);*/

//
//    using nlohmann::json;
//    std::ios_base::sync_with_stdio(false);
//    std::ifstream i;
//    i.open("canada.cbor");
//
//
//    auto start0 = std::chrono::high_resolution_clock::now();
//    json j1 = /*{{"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}};*/json::from_cbor(i);
//    std::cout << std::chrono::nanoseconds(std::chrono::high_resolution_clock::now() - start0).count() << std::endl;
//
//    json j2(j1);
//    json j3(j1);
//    json j4(j2);
//    json j5(j1);
//    json j6(j1);
//
//    auto start1 = std::chrono::high_resolution_clock::now();
//    /*std::string s1 =  *///json jj = utils::te(std::move(j1), std::move(j2));//.dump();
//    utils::te1(j1, std::move(j2));
//    std::cout << std::chrono::nanoseconds(std::chrono::high_resolution_clock::now() - start1).count() << std::endl;
//
//    auto start3 = std::chrono::high_resolution_clock::now();
//    /*std::string s1 =  */json jj(utils::te(std::move(j5), std::move(j6)));//.dump();
//    std::cout << std::chrono::nanoseconds(std::chrono::high_resolution_clock::now() - start3).count() << std::endl;
//
//    //std::cout << jj;
//
//    auto start2 = std::chrono::high_resolution_clock::now();
//    j4.update(j3);
//    //std::string s2 = j3.dump();
//    std::cout << std::chrono::nanoseconds(std::chrono::high_resolution_clock::now() - start1).count() << std::endl;
//    std::cout << (j1==j4 ? "T" : "F") << std::endl;*/
    /*using nlohmann::json;
    std::string fnbase(argv[1]);
    std::ifstream lc;
    lc.open("test/" + fnbase + "l.cbor");
    json jl;
    //jl = json::from_cbor(lc);
    lc >> jl;

    std::ifstream rc;
    rc.open("test/" + fnbase + "r.cbor");
    json jr;
    //jl = json::from_cbor(lc);
    rc >> jr;


    json mym(utils::merge_state_delta(std::move(jl), std::move(jr)));

    std::ifstream mc;
    mc.open("test/" + fnbase + "m.cbor");
    json jm;
    //jl = json::from_cbor(lc);
    mc >> jm;

    if (mym == jm)
        std::cout << "YES" << std::endl;
    else
        std::cout << "FUCK" << jm << mym << std::endl;*/

    using utils::json;
    std::string aaa = "aaa";
    std::string bbb = "bbb";
    json a = {{"a", "b"}};

    json b = {{"1", {aaa, bbb}}, {"2", a}};
    std::cout << b << std::endl;
    b["2"]["a"] = "c";
    std::cout << b << std::endl;
    std::cout << a << std::endl;
    /*version_graph::Graph a;
    std::cout << "construction" << std::endl;

    a.continue_chain("ROOT", "A", {});

    a.continue_chain("A", "B", {});


    a.continue_chain("A", "C", {});

    std::cout << "c" << std::endl;

    auto b = a.rbegin("C");
    auto e = a.rend();
    for (; b!=e; ++b) {
        auto [tag, payload] = *b;
        std::cout << tag << " aaaa"<< std::endl;
    }

*/
    /*
    json str1 = "aaa";
    json str2 = "bbb";
    json str3 = "ccc";
    std::vector<char> ostr;
    ostr.push_back(utils::cbor_map_header<2>());
    json::to_cbor(str1, ostr);
    json::to_cbor(str1, ostr);
    json::to_cbor(str2, ostr);
    //json::to_cbor(str3, ostr);
    ostr.push_back(1);
    //json::to_cbor(str3, ostr);
    //json::to_cbor(str2, ostr);
    //auto sss = ostr.str();
    json res = json::from_cbor(ostr);
    std::cout << res << ostr.size() << std::endl;


    char xxx = 'a';
    std::string xxxstr;
    xxxstr = xxx;
    std::cout << xxxstr;

    json anum = xxxstr;
    auto numv = json::to_cbor(anum);

    std::cout << "aaaa" << numv.size() << "hello "<< std::hex << (int)numv[0] << (int)numv[1] <<std::endl;
     */
//
//    json tj = true;
//
//    auto tjv = json::to_cbor(tj);
//
//    std::cout << tj << std::hex << (int)tjv[0] << std::endl;
/*
    std::vector<char> cons1;
    cons1.push_back(utils::cbor_map_header<1>());
    cons1.push_back(enums::cbor_const::cbor_char_header);
    cons1.push_back(enums::transfer_fields::Types);
    cons1.push_back(enums::cbor_const::cbor_true);

    json tj = json::from_cbor(cons1);

    auto d1 = utils::dump_pull_req_data("aaa", {"v1", "v2"}, false, 1000, cons1);

    std::cout << tj << std::endl;

    json dj = json::from_cbor(d1);

    std::cout << dj << std::endl;

    auto d2 = utils::dump_pull_resp_data(1000, dj, {"v3", "v4"});

    json dj1 = json::from_cbor(d2);

    std::cout << dj1 << std::endl;

    std::cout << std::dec;
*/
    //async_server::server myserver(2048, 2);


    version_manager::VersionManager vm{
        "TEST",
        //version_manager::VersionManager::have_custom_merge_t(),
        nullptr,
        //version_manager::VersionManager::custom_merge_function_t(),
        nullptr,
        {{carname, {}}}
        };

    auto & g = vm.get_g();

    vm.receive_data("TEST_APP1", "ROOT", "0", json(package1));
    vm.receive_data("TEST_APP2", "0", "1", json(package2));
    vm.receive_data("TEST_APP1", "0", "2", json(package4));

    for (auto [v, p]: g) {
        std::cout << v << " " << p.dump(2) << std::endl;
    }
    std::cout << "conf here!" << std::endl;
    for (auto it = g.begin("2"); it != g.end(); ++it) {
        auto [v, p] = *it;
        std::cout << v << " " << p.dump(2) << std::endl;
    }

    vm.data_sent_confirmed("TEST_APP1", "2", g.get_head_tag());
    vm.data_sent_confirmed("TEST_APP2", "1", g.get_head_tag());

    auto [data, v1, v2] = vm.retrieve_data("TEST", "ROOT", json());

    std::cout << data.dump(2) << std::endl;

    json nj;
    json j1 = json::object();

    std::cout << j1.is_null() << (nj == j1) <<std::endl;
    assertEq(nj, j1);


    std::cout << "----------------------------------" << std::endl;

    test1();

    std::cout << *((&enums::transfer_fields::AppName)+2);

    repository::Repository repoa{"TEST",
            //version_manager::VersionManager::have_custom_merge_t(),
                                 nullptr,
            //version_manager::VersionManager::custom_merge_function_t(),
                                 nullptr,
                                 json::object()};
    repoa.start_server(2048, 10);

    repoa.connect_to("127.0.0.1", 2048);
    return 0;
}