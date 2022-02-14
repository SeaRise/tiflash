#pragma once

#include <mutex>
#include <shared_mutex>
#include <variant>

namespace variant_op
{
template <class... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
template <class T>
struct always_false : std::false_type
{
};
} // namespace variant_op

namespace DB
{

class MutexLockWrap
{
public:
    std::lock_guard<std::mutex> genLockGuard() const
    {
        return std::lock_guard(mutex);
    }

    std::unique_lock<std::mutex> tryToLock() const
    {
        return std::unique_lock(mutex, std::try_to_lock);
    }

    std::unique_lock<std::mutex> genUniqueLock() const
    {
        return std::unique_lock(mutex);
    }

private:
    mutable std::mutex mutex;
};

class SharedMutexLockWrap
{
public:
    std::shared_lock<std::shared_mutex> genReadLockGuard() const
    {
        return std::shared_lock(shared_mutex);
    }

    std::unique_lock<std::shared_mutex> genWriteLockGuard() const
    {
        return std::unique_lock(shared_mutex);
    }

private:
    mutable std::shared_mutex shared_mutex;
};

struct AsyncNotifier
{
    enum class Status
    {
        Timeout,
        Normal,
    };
    virtual Status blockedWaitFor(std::chrono::milliseconds) { return AsyncNotifier::Status::Timeout; }
    virtual void wake() = 0;
    virtual ~AsyncNotifier() = default;
};
} // namespace DB