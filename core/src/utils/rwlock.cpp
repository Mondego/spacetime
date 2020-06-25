#include <mutex>
#include <utils/rwlock.h>

rwlock::rwlock_write::write_lock rwlock::rwlock_write::get_write_lock() {
    return write_lock(*this);
}

rwlock::rwlock_write::read_lock rwlock::rwlock_write::get_read_lock() {
    return read_lock(*this);
}

rwlock::rwlock_write::read_lock::read_lock(rwlock::rwlock_write::read_lock &&other) noexcept
: lock(other.lock), env(other.env) {
    other.lock = nullptr;
}

rwlock::rwlock_write::read_lock::read_lock(rwlock_write & env) : env(env){
    std::unique_lock<std::mutex> w_count_lock(env.w_count_mutex);
    env.no_write_pending.wait(w_count_lock, [&env](){ return env.w_count == 0; });
    lock = new std::shared_lock(env.access_mutex);
}

rwlock::rwlock_write::read_lock::~read_lock() {
    delete lock;
}

rwlock::rwlock_write::read_lock::value_t &rwlock::rwlock_write::read_lock::get() {
    return *lock;
}

rwlock::rwlock_write::write_lock::write_lock(rwlock::rwlock_write::write_lock &&other) noexcept
: lock(other.lock), env(other.env) {
    other.lock = nullptr;
}

rwlock::rwlock_write::write_lock::write_lock(rwlock_write & env) : env(env){
    {
        std::lock_guard<std::mutex> w_count_lock(env.w_count_mutex);
        env.w_count++;
    }
    lock = new std::unique_lock(env.access_mutex);
}

rwlock::rwlock_write::write_lock::~write_lock() {
    if (lock) {
        {
            std::lock_guard<std::mutex> w_count_lock(env.w_count_mutex);
            env.w_count--;
        }
        if (env.w_count == 0) env.no_write_pending.notify_all();
    }
    delete lock;
}

rwlock::rwlock_write::write_lock::value_t &rwlock::rwlock_write::write_lock::get() {
    return *lock;
}
