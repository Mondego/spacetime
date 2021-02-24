#ifndef DATAFRAME_CORE_DEBUG_LOGGER_H
#define DATAFRAME_CORE_DEBUG_LOGGER_H

#ifdef NDEBUG
#define DEBUG_LOGGER_ENABLED false
#else
#define DEBUG_LOGGER_ENABLED true

#include <fstream>
#include <mutex>

inline std::mutex log_mutex;
inline std::ofstream logfile("df_core.log");
#endif //NDEBUG

namespace logger {
#if DEBUG_LOGGER_ENABLED
    template <typename... Types>
    void raw_write(Types... messages) {
        if constexpr(sizeof...(messages) > 0) {
            std::lock_guard lock(log_mutex);
            (logfile << ... <<  messages) << std::endl;
        }
    }
#else
    template <typename... Types>
    void raw_write(Types... messages) { }
#endif

    template <typename... Types>
    void error(Types... messages) {
        raw_write("ERROR: ", messages...);
    }

    template <typename... Types>
    void info(Types... messages) {
        raw_write("INFO: ", messages...);
    }

    template <typename... Types>
    void debug(Types... messages) {
        raw_write("DEBUG: ", messages...);
    }
}

#endif //DATAFRAME_CORE_DEBUG_LOGGER_H
