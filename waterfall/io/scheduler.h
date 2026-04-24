////
// @file scheduler.h
// @brief
// 调度器
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <vector>
#include <mutex>
#include <condition_variable>
#include "service.h"
#include "error.h"
#include "service.h"
#include "task.h"
#include "executor.h"

namespace wf::io {
class reactor_service;
class scheduler_service;

// 调度器执行上下文
class scheduler_executor : public runtime_executor
{
  private:
    scheduler_service *scheduler_;

  public:
    scheduler_executor() = delete;
    scheduler_executor(const scheduler_executor &) = default;
    scheduler_executor(scheduler_executor &&) = default;
    scheduler_executor &operator=(const scheduler_executor &) = default;
    scheduler_executor &operator=(scheduler_executor &&) = default;

    void execute(async_task &task) override;
    void execute(task_queue &queue) override;

    friend bool operator==(
        const scheduler_executor &a,
        const scheduler_executor &b) noexcept
    {
        return a.scheduler_ == b.scheduler_;
    }
    friend bool operator!=(
        const scheduler_executor &a,
        const scheduler_executor &b) noexcept
    {
        return a.scheduler_ != b.scheduler_;
    }

  private:
    explicit scheduler_executor(scheduler_service *sched)
        : scheduler_(sched)
    {}
    friend class scheduler_service;
};

// 调度器服务
class scheduler_service : public service_base
{
  private:
    // 调度器哑任务
    class reactor_dummy_task : public async_task
    {
      public:
        reactor_service *reactor_;
        void run(long usec, task_queue &ops);
        void interrupt();
        reactor_dummy_task();
    };

  public:
    // worker线程信息
    struct worker_info
    {
        task_queue local_queue_; // 线程局部任务队列
        error_info *error_;      // 任务错误信息
        size_t handled_;         // 处理完成的任务数目
        unsigned short index_;   // 线程索引
    };

  private:
    std::mutex mutex_;                   // 互斥变量
    std::condition_variable cond_;       // 条件变量
    task_queue global_queue_;            // 异步操作队列
    std::vector<worker_info *> workers_; // NOTE: 指针指向线程局部信息
    reactor_dummy_task *reactor_;        // reactor指针
    service_registry *service_registry_; // 服务注册表
    scheduler_executor root_executor_;   // 执行器
    utils::call_stack<runtime_executor>::tag root_tag_; // 根tag
    uint16_t waiting_threads_;                          // 等待线程数量
    bool reactor_waiting_; // 是否有线程一直守候reactor
    bool stopped_;         // 结束请求

  public:
    explicit scheduler_service(service_registry *registry);
    ~scheduler_service();

    void shutdown() override;

    // worker线程入口
    void run(unsigned short index);

    // 投递异步操作
    void dispatch(async_task &op);
    void dispatch(task_queue &ops);

    // 停止调度器，worker线程退出
    void stop();
    void restart(); // 重启调度器
    bool stopped();

    // 获取调度器执行上下文
    runtime_executor *get_runtime_executor();

  private:
    // 创建、销毁线程局部任务队列
    void create_thread_local_queue();
    void destroy_thread_local_queue();

    // 处理一次处理循环
    size_t do_run_one(worker_info &info);

    // 唤醒一个线程，然后释放锁，该函数调用时必须持有锁
    void wakeup_one_thread_release_lock(std::unique_lock<std::mutex> &lock);

    // 唤醒reactor
    void wakeup_reactor();
    // 入队reactor
    void enqueue_reactor(reactor_dummy_task *r);

    friend struct work_cleanup;
    friend struct reactor_cleanup;
    friend class reactor_service;
    friend class scheduler_context;
};

} // namespace wf::io