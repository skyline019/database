////
// @file thread_pool.h
// @brief
// 线程池服务
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <vector>
#include <functional>
#include "service.h"
#include "thread.h"

namespace wf::io {

// 线程池服务类
class thread_pool : public service_base
{
  private:
    std::vector<posix_thread> workers_;                    // 工作线程池
    std::vector<std::function<void()>> preload_functions_; // 预加载函数
    std::vector<std::function<void()>> cleanup_functions_; // 清理函数

  public:
    thread_pool() = default;
    ~thread_pool();

    // 禁止拷贝和移动
    thread_pool(const thread_pool &) = delete;
    thread_pool &operator=(const thread_pool &) = delete;
    thread_pool(thread_pool &&) = delete;
    thread_pool &operator=(thread_pool &&) = delete;

    // 直接执行任务（在当前线程中立即执行）
    template <typename Func, typename... Args>
    error_info execute(Func &&func, Args &&...args);

    // 带CPU亲和性的任务执行
    template <typename Func, typename... Args>
    error_info affinity_execute(Func &&func, Args &&...args);

    // 等待所有worker退出
    void join_all();

    // 获取线程池大小
    size_t size() const { return workers_.size(); }

    // 服务关闭函数 - 等待所有线程完成
    void shutdown() override;

    // 添加预执行函数
    template <typename Func>
    void add_preloader(Func &&func)
    {
        preload_functions_.emplace_back(std::forward<Func>(func));
    }

    // 添加后执行函数
    template <typename Func>
    void add_sweeper(Func &&func)
    {
        cleanup_functions_.emplace_back(std::forward<Func>(func));
    }

  private:
    // 执行预执行函数
    void execute_preloader();
    // 执行后执行函数
    void execute_sweeper();

    // 声明thread_wrapper为友元类，允许访问私有方法
    template <typename Func, typename... Args>
    friend class thread_wrapper;
};

// 封装线程执行函数，包含预执行和后执行
template <typename Func, typename... Args>
class thread_wrapper
{
  private:
    Func user_func_;
    std::tuple<Args...> user_args_;
    thread_pool &thread_pool_; // 保存thread_pool的引用

  public:
    // 构造函数
    thread_wrapper(thread_pool &pool, Func func, Args... args)
        : user_func_(std::move(func))
        , user_args_(std::forward<Args>(args)...)
        , thread_pool_(pool) // 初始化引用
    {}

    // 函数调用操作符 - 直接使用原始thread_pool的引用
    auto operator()() -> typename std::invoke_result<Func, Args...>::type
    {
        // 直接使用原始thread_pool执行预执行函数
        thread_pool_.execute_preloader();

        // RAII守卫，确保后执行函数在作用域退出时执行
        struct post_exec_guard
        {
            thread_pool &pool_;
            post_exec_guard(thread_pool &p)
                : pool_(p)
            {}
            ~post_exec_guard() { pool_.execute_sweeper(); }
        } guard(thread_pool_); // 使用原始thread_pool

        // 执行用户函数
        return std::apply(std::move(user_func_), std::move(user_args_));
    }
};

// 创建线程包装器的便捷函数
template <typename Func, typename... Args>
auto make_thread_wrapper(thread_pool &pool, Func &&func, Args &&...args)
{
    return thread_wrapper<std::decay_t<Func>, std::decay_t<Args>...>(
        pool, std::forward<Func>(func), std::forward<Args>(args)...);
}

// execute实现
template <typename Func, typename... Args>
error_info thread_pool::execute(Func &&func, Args &&...args)
{
    // 使用make_thread_wrapper打包函数，包含预执行和后执行逻辑
    auto wrapper = make_thread_wrapper(
        *this, std::forward<Func>(func), std::forward<Args>(args)...);

    // 创建线程并执行打包后的任务
    posix_thread thread;
    error_info ret = thread.exec(std::move(wrapper));
    if (ret == utils::OK) // 将线程添加到工作线程列表
        workers_.emplace_back(std::move(thread));
    return ret;
}

// affinity_execute实现
template <typename Func, typename... Args>
error_info thread_pool::affinity_execute(Func &&func, Args &&...args)
{
    // 使用make_thread_wrapper打包函数，包含预执行和后执行逻辑
    auto wrapper = make_thread_wrapper(
        *this, std::forward<Func>(func), std::forward<Args>(args)...);

    // 创建线程并执行打包后的任务
    posix_thread thread;
    error_info ret = thread.affinity_exec(std::move(wrapper));
    if (ret == utils::OK) // 将线程添加到工作线程列表
        workers_.emplace_back(std::move(thread));
    return ret;
}

} // namespace wf::io