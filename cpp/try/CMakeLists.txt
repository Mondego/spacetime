cmake_minimum_required(VERSION 3.9)

project(libtry VERSION 0.0.1 DESCRIPTION "try lib")

include_directories(include)
include_directories(libs/asio/include)
include_directories(libs)

add_library(libtry STATIC
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

add_executable(trystub
		src/trystub.cpp
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

add_executable(
		tryasio
		src/tryasio.cpp
)


add_executable(
		tryasioclient
		src/tryasioclient.cpp
)

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_VERBOSE_MAKEFILE ON)
find_package (Threads)

target_link_libraries (trystub ${CMAKE_THREAD_LIBS_INIT})

set_property(TARGET libtry PROPERTY CXX_STANDARD 17)

set_property(TARGET libtry PROPERTY POSITION_INDEPENDENT_CODE ON)

set_property(TARGET trystub PROPERTY CXX_STANDARD 17)

set_property(TARGET tryasio PROPERTY CXX_STANDARD 17)

set_property(TARGET tryasioclient PROPERTY CXX_STANDARD 17)

target_link_libraries (tryasio ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries (tryasioclient ${CMAKE_THREAD_LIBS_INIT})

set_target_properties(libtry PROPERTIES VERSION ${PROJECT_VERSION})

#set_target_properties(libtry PROPERTIES SOVERSION 1)

set_target_properties(libtry PROPERTIES PUBLIC_HEADER "include/a.h include/version_graph.h, include/utils/rwlock.h")

target_include_directories(libtry PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
