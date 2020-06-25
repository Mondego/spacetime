//
// Created by zhazha on 1/8/20.
//

#ifndef DATAFRAME_CORE_RWLOCK_H
#define DATAFRAME_CORE_RWLOCK_H

#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <condition_variable>

namespace rwlock{
    class rwlock_write{
    private:
        std::shared_mutex access_mutex;
        std::atomic<int> w_count{0};
        std::mutex w_count_mutex;
        std::condition_variable no_write_pending;
    public:
        class read_lock{
        public:
            using value_t = std::shared_lock<std::shared_mutex>;
        private:
            value_t * lock;
            rwlock_write & env;
        public:
            read_lock(read_lock const & other) = delete;
            read_lock & operator=(read_lock const & other) = delete;
            read_lock(read_lock && other) noexcept;
            read_lock & operator=(read_lock && other) = delete;
            explicit read_lock(rwlock_write & env);
            ~read_lock();
            value_t & get();
        };

        class write_lock{
        public:
            using value_t = std::unique_lock<std::shared_mutex>;
        private:
            value_t * lock;
            rwlock_write & env;
        public:
            write_lock(write_lock const & other) = delete;
            write_lock & operator=(write_lock const & other) = delete;
            write_lock(write_lock && other) noexcept;
            write_lock & operator=(write_lock && other) = delete;
            explicit write_lock(rwlock_write & env);
            ~write_lock();
            value_t & get();
        };

        write_lock get_write_lock();
        read_lock get_read_lock();
    };
}

#endif //DATAFRAME_CORE_RWLOCK_H
