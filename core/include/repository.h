#ifndef DATAFRAME_CORE_REPOSITORY_H
#define DATAFRAME_CORE__REPOSITORY_H

#include <version_manager.h>

#include <memory>

#include <connectors/async_server.h>
#include <connectors/async_client.h>


namespace repository{
    using version_manager::have_custom_merge_t;
    using version_manager::custom_merge_function_t;
    using version_manager::type_dim_map_t;
    class Repository{
    private:
        version_manager::VersionManager manager;
        std::string m_app_name;
        json m_type_chains;
        std::unique_ptr<async_server::server> repoServer;
        std::unique_ptr<async_client::connector> repoConnector;
    public:
        Repository(std::string const & app_name, have_custom_merge_t && have_merge, custom_merge_function_t && custom_merge_function,
                   json && type_info, enums::Autoresolve autoresolve = enums::Autoresolve::FullResolve);

        ~Repository();

        unsigned int start_server(unsigned short port, unsigned short thread_count = 1);

        void connect_to(std::string const & address, unsigned short port);

        bool is_connected();

        void push();

        void push_await();

        void fetch();

        void fetch_await(double timeout = 0);

        version_manager::VersionManager & get_manager_ref();

    };
}

#endif //DATAFRAME_CORE_REPOSITORY_H
