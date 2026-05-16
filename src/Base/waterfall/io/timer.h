////
// @file timer.h
// @brief
// 按system_clock实现的定时器服务
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <mutex>
#include <chrono>
#include <vector>
#include <limits>
#include <waterfall/utils/noncopyable.h>
#include "service.h"
#include "task.h"

namespace wf::io {
class scheduler_service;
class reactor_service;

// 等待操作
using wait_task = async_task;

// 叠加callback的等待操作
class callback_wait_task : public wait_task
{
  private:
    std::function<void()> callback_;

  public:
    template <typename Callback>
        requires std::is_invocable_r_v<void, Callback>
    callback_wait_task(Callback &&callback)
        : wait_task(&do_complete)
        , callback_(std::forward<Callback>(callback))
    {}

  private:
    static void do_complete(void *owner, async_task *op, void *)
    {
        callback_wait_task *self = static_cast<callback_wait_task *>(op);
        if (owner)
            self->callback_();
        else
            self->destroy();
    }
};

// 定时器队列
class timer_queue
{
  public:
    using time_point = std::chrono::system_clock::time_point;
    using duration = std::chrono::system_clock::duration;

    // 定时器堆元素指向的定时器数据，双向链表，便于删除
    class per_timer_data
    {
      private:
        per_timer_data *next_; // 后继指针
        per_timer_data *prev_; // 前驱指针
        task_queue op_queue_;  // 操作队列
        size_t heap_index_;    // 堆索引

      public:
        per_timer_data()
            : next_(nullptr)
            , prev_(nullptr)
            , heap_index_((std::numeric_limits<size_t>::max)())
        {}
        friend class timer_queue;
    };
    // 定时器堆元素
    struct heap_entry
    {
        time_point time_;       // 定时器时间
        per_timer_data *timer_; // 定时器数据
    };

  private:
    per_timer_data *timers_;       // 定时器链表
    std::vector<heap_entry> heap_; // 定时器堆

  public:
    timer_queue()
        : timers_()
        , heap_()
    {}

    bool empty() const { return timers_ == 0; }

    long wait_duration_msec(long max_duration) const;
    long wait_duration_usec(long max_duration) const;

    void get_ready_timers(task_queue &ops);
    void get_all_timers(task_queue &ops);

    bool is_enqueued(const per_timer_data &timer);

    bool enqueue_timer(
        const time_point &time,
        per_timer_data &timer,
        async_task *op);

    size_t cancel_timer(
        per_timer_data &timer,
        task_queue &ops,
        size_t max_cancelled = (std::numeric_limits<size_t>::max)());

  private:
    void up_heap(size_t index);
    void swap_heap(size_t index1, size_t index2);
    void down_heap(size_t index);

    void remove_timer(per_timer_data &timer);
};

// system_clock时钟服务
class system_timer_service : public service_base
{
  public:
    using time_point = timer_queue::time_point;
    using duration = timer_queue::duration;

    // 实例类型
    struct instance_type : private utils::noncopyable
    {
        time_point expiry; // 过期时间
        timer_queue::per_timer_data timer_data;
    };
    friend class reactor_service;

  private:
    scheduler_service *scheduler_; // 调度器
    reactor_service *reactor_;     // reactor
    std::mutex mutex_;             // 互斥变量
    timer_queue timer_queue_;      // 定时器队列
    int timer_fd_;                 // timerfd文件描述符

  public:
    system_timer_service(
        scheduler_service *scheduler,
        reactor_service *reactor);
    ~system_timer_service();

    void shutdown();

    // 构造与销毁实例
    void construct(instance_type &impl) { impl.expiry = time_point(); }
    void destroy(instance_type &impl);

    // 是否启动
    bool is_started(const instance_type &impl);

    // 取消操作
    size_t cancel(instance_type &impl);
    size_t cancel_one(instance_type &impl);

  private:
    void schedule_timer(
        const timer_queue::time_point &time,
        timer_queue::per_timer_data &timer,
        wait_task *op);
    size_t cancel_timer(
        timer_queue::per_timer_data &timer,
        size_t max_cancelled = (std::numeric_limits<size_t>::max)());

    void update_timeout();
    int get_timeout(int msec);
    int get_timeout(itimerspec &ts);
    void check_timer(task_queue &ops);

    friend class system_timer;
};

} // namespace wf::io