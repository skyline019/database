////
// @file strand.h
// @brief
// strand服务
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <mutex>
#include <memory>
#include <vector>
#include "service.h"
#include "task.h"
#include "executor.h"

namespace wf::io {
class strand_service;

// strand实现
class strand_instance_impl
{
  private:
    strand_service *service_;  // 指向strand服务
    std::mutex mutex_;         // 互斥锁
    task_queue waiting_queue_; // 等待队列
    task_queue ready_queue_;   // 就绪队列
    bool locked_;              // 是否锁定

  public:
    ~strand_instance_impl();
    friend class strand_service;
};
using strand_instance_type = std::shared_ptr<strand_instance_impl>;

// strand执行上下文
class strand_executor : public runtime_executor
{
  private:
    strand_instance_type strand_;

  public:
    strand_executor() = delete;
    strand_executor(const strand_executor &) = default;
    strand_executor(strand_executor &&) = default;
    strand_executor &operator=(const strand_executor &) = default;
    strand_executor &operator=(strand_executor &&) = default;

    void execute(async_task &task) override;
    void execute(task_queue &queue) override;

    friend bool
    operator==(const strand_executor &a, const strand_executor &b) noexcept
    {
        return a.strand_.get() == b.strand_.get();
    }
    friend bool
    operator!=(const strand_executor &a, const strand_executor &b) noexcept
    {
        return a.strand_.get() != b.strand_.get();
    }

  private:
    explicit strand_executor(strand_service *strand);
    // 添加friend声明
    friend class strand_service;
};

// strand服务
class strand_service : public service_base
{
  public:
    struct on_execute_exit;

  public:
    explicit strand_service() = default;
    ~strand_service() = default;
    void shutdown() {}

    strand_instance_type get_runtime_executor();

  private:
    static bool enqueue(strand_instance_impl *impl, async_task &op);
    static bool enqueue(strand_instance_impl *impl, task_queue &ops);
    static bool push_waiting_to_ready(strand_instance_impl *impl);
    static void run_ready_handlers(strand_instance_impl *impl);
    static void execute(strand_instance_impl *impl);

    friend class strand_executor;
};

} // namespace wf::io