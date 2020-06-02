#ifndef LIBTRY_DEBUG_LOGGER_H
#define LIBTRY_DEBUG_LOGGER_H

#ifdef NDEBUG
#define DEBUG_INFO(message) do { } while ( false )
#else
#include <fstream>
#include <mutex>
inline std::mutex log_mutex;
inline std::ofstream logfile("graph_log.txt");
#define DEBUG_INFO(message) do {\
    std::lock_guard lock(log_mutex);\
    logfile << "2020-05-19 14:17:09,097 - version_graph_0 - INFO - " << message << std::endl;\
} while (false)
#endif

#endif //LIBTRY_DEBUG_LOGGER_H
