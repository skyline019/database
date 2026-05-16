////
// @file thread.h
// @brief
// 线程封装
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <pthread.h>
#include <functional>
#include <type_traits>
#include <memory>
#include "error.h"

namespace wf::io {

// 线程封装类
class posix_thread
{
  private:
    pthread_t id_ = 0;     // 线程ID (0表示无效)
    static int cpu_index_; // 用于分配CPU索引的静态变量

  public:
    posix_thread() noexcept = default;
    posix_thread(const posix_thread &) = delete;
    posix_thread &operator=(const posix_thread &) = delete;
    posix_thread(posix_thread &&other) noexcept;
    posix_thread &operator=(posix_thread &&other) noexcept;
    ~posix_thread();

    // 执行异步任务，假设返回void
    template <typename Func, typename... Args>
    error_info exec(Func &&f, Args &&...args);

    // cpu亲近执行异步任务，假设返回void
    template <typename Func, typename... Args>
    error_info affinity_exec(Func &&f, Args &&...args);

    void join();
    void detach();
    ::pthread_t native_handle() const noexcept { return id_; }

    int set_affinity(int cpu_index);
    static unsigned int hardware_concurrency() noexcept;

  private:
    // 线程包装器基类
    struct thread_wrapper_base
    {
        virtual ~thread_wrapper_base() = default;
        virtual void run() = 0;
    };

    // 具体线程包装器
    struct thread_wrapper : public thread_wrapper_base
    {
        std::function<void()> func_;

        thread_wrapper(std::function<void()> func)
            : func_(std::move(func))
        {}

        void run() override { func_(); }
    };

    static void *thread_entry(void *arg);
};

// 模板函数实现
template <typename Func, typename... Args>
error_info posix_thread::exec(Func &&f, Args &&...args)
{
    // 线程运行，返回错误
    if (id_) return kErrorThreadIsRunning;

    // 绑定函数和参数
    auto *wrapper = new thread_wrapper( // 在堆上分配
        [func = std::forward<Func>(f),
         args_tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable {
            return std::apply(std::move(func), std::move(args_tuple));
        });

    // 创建线程
    if (::pthread_create(&id_, nullptr, &thread_entry, wrapper) != 0) {
        delete wrapper; // 创建失败时清理内存
        return kErrorThreadCreateFailed;
    }

    return nullptr;
}

template <typename Func, typename... Args>
error_info posix_thread::affinity_exec(Func &&f, Args &&...args)
{
    // 线程运行，返回错误
    if (id_) return kErrorThreadIsRunning;

    // 绑定函数和参数
    auto *wrapper = new thread_wrapper( // 在堆上分配
        [func = std::forward<Func>(f),
         args_tuple = std::make_tuple(std::forward<Args>(args)...)]() mutable {
            std::apply(std::move(func), std::move(args_tuple));
        });

    // 设置cpu亲和
    int index = cpu_index_++ % hardware_concurrency();

    // 设置线程属性，包括CPU亲和性
    pthread_attr_t attr;
    if (::pthread_attr_init(&attr) != 0) {
        delete wrapper;
        return kErrorThreadAttributeInitFailed;
    }

    // 设置CPU亲和性
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(index, &cpuset);
    if (::pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset) != 0) {
        ::pthread_attr_destroy(&attr);
        delete wrapper;
        return kErrorThreadAffinitySetFailed;
    }

    // 创建线程
    if (::pthread_create(&id_, &attr, &thread_entry, wrapper) != 0) {
        delete wrapper; // 创建失败时清理内存
        return kErrorThreadCreateFailed;
    }

    // 线程创建成功后销毁属性对象
    ::pthread_attr_destroy(&attr);

    return nullptr;
}

} // namespace wf::io