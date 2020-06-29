cmake_minimum_required(VERSION 3.9)

project(dataframe_core
		VERSION 0.0.1
		DESCRIPTION "Dataframe core library, rewritten in c++"
		LANGUAGES CXX)

include_directories(include)
include_directories(libs/asio/include)
include_directories(libs)

add_library(dataframe_core STATIC
		include/utils/enums.h
		include/utils/utils.h
		include/utils/debug_logger.h
		include/connectors/async_utils.h
		include/connectors/async_server.h
		include/connectors/async_client.h
		include/version_graph.h
		include/version_manager.h
		include/repository.h
		
		src/utils/utils.cpp
		src/connectors/async_utils.cpp
		src/connectors/async_server.cpp
		src/connectors/async_client.cpp
		src/version_graph.cpp
		src/version_manager.cpp
		src/repository.cpp
		)

set(CMAKE_CXX_FLAGS_DEBUG "-g")
find_package (Threads)

set_property(TARGET dataframe_core PROPERTY CXX_STANDARD 17)

set_property(TARGET dataframe_core PROPERTY POSITION_INDEPENDENT_CODE ON)

set_target_properties(dataframe_core PROPERTIES VERSION ${PROJECT_VERSION})

target_include_directories(dataframe_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
