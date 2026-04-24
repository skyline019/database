////
// @file reactor.h
// @brief
// epoll reactor
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include "service.h"
#include "task.h"
#include "channel.h"
#include "error.h"

namespace wf::io {
class scheduler_service;
class system_timer_service;

// channel对象池，reactor内部使用
class reactor_channel_pool : private utils::noncopyable
{
  private:
    reactor_channel *live_list_; // 活跃链表
    reactor_channel *free_list_; // 空闲链表

  public:
    reactor_channel_pool()
        : live_list_(nullptr)
        , free_list_(nullptr)
    {}

    ~reactor_channel_pool()
    {
        destroy_list(live_list_);
        destroy_list(free_list_);
    }

    reactor_channel *first() { return live_list_; }
    reactor_channel *alloc();
    void free(reactor_channel *o);

  private:
    void destroy_list(reactor_channel *list);
};

// reactor服务
class reactor_service : public service_base
{
  private:
    std::mutex mutex_;                    // 互斥变量
    std::mutex pool_mutex_;               // channel池互斥变量
    service_registry *service_registry_;  // 服务注册表
    scheduler_service *scheduler_;        // 调度器指针
    async_task *dummy_;                   // 给调度器的操作
    system_timer_service *timer_service_; // 定时器服务
    reactor_channel_pool channel_pool_;   // channel分配池
    int epoll_fd_;                        // epoll文件描述符
    int interrupt_fd_;                    // eventfd文件描述符
    static const int max_events = 128;    // 每轮收割的事件数目

  public:
    // reactor仅供scheduler_service使用
    reactor_service(service_registry *registry, scheduler_service *sc);
    ~reactor_service();

    void shutdown();

    // 运行与中断
    void harvest(long usec, task_queue &ops);
    void interrupt();

    // 注册与注销文件描述符
    error_info register_descriptor(int descriptor, reactor_channel *&channel);
    void deregister_descriptor(
        int descriptor,
        reactor_channel *&channel,
        bool closing);
    error_info register_internal_descriptor(
        int op_type,
        int descriptor,
        reactor_channel *&channel,
        channel_task *op);
    void
    deregister_internal_descriptor(int descriptor, reactor_channel *&channel);

    // 清理channel对象
    void cleanup_channel(reactor_channel *&channel);

    // 异步请求，如果未完成，将操作加入到队列中，否则立即执行回调函数
    // <0     出错
    // =0     挂起
    // >0     完成
    channel_task::status async_request(
        int op_type,
        int descriptor,
        reactor_channel *&channel,
        channel_task *op);

#if 0
    // 取消操作
    void cancel_ops(reactor_channel *&channel);
    void cancel_ops_by_key(
        reactor_channel *&channel,
        int op_type,
        void *cancellation_key);
  private:

    static void call_deliver(operation *op, const void *self);
#endif

  private:
    reactor_channel *allocate_channel();
    void free_channel(reactor_channel *s);
    // dummy任务回调函数
    static void dummy_reactor_func(void *owner, async_task *op, void *);
    void register_timer(int fd, void *ptr);

    friend class scheduler_service;
    friend class system_timer_service;
    friend struct handle_event_cleanup_on_block_exit;
};

} // namespace wf::io