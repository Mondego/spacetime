cmake_minimum_required(VERSION 3.9)

project(dataframe_core VERSION 0.0.1 DESCRIPTION "Core Dataframe")

include_directories(include)
include_directories(libs/asio/include)
include_directories(libs)

add_library(dataframe_core STATIC
		src/utils/rwlock.cpp
		src/version_graph.cpp
		src/utils/utils.cpp
		include/connectors/async_server.h
		include/connectors/async_utils.h
		include/connectors/async_client.h
		include/utils/rwlock.h
		include/utils/debug_logger.h
		include/version_graph.h
		include/utils/utils.h
		include/version_manager.h
		include/repository.h
		src/version_manager.cpp
		src/connectors/async_server.cpp
		src/connectors/async_client.cpp
		src/connectors/async_utils.cpp
		src/repository.cpp
		)

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_VERBOSE_MAKEFILE ON)
find_package (Threads)

set_property(TARGET dataframe_core PROPERTY CXX_STANDARD 17)

set_property(TARGET dataframe_core PROPERTY POSITION_INDEPENDENT_CODE ON)

set_target_properties(dataframe_core PROPERTIES VERSION ${PROJECT_VERSION})

set_target_properties(dataframe_core PROPERTIES PUBLIC_HEADER "include/version_graph.h, include/utils/rwlock.h")

target_include_directories(dataframe_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
