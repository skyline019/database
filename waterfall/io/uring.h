////
// @file uring.h
// @brief
// io_uring服务，支持文件异步操作
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <mutex>
#include <atomic>
#include <liburing.h>
#include "service.h"
#include "task.h"
#include "channel.h"

namespace wf::io {
class scheduler_service;
class reactor_service;

// io_uring异步操作
class uring_task : public async_task
{
  public:
    using prepare_func_type = void (*)(uring_task *, ::io_uring_sqe *);
    using perform_func_type = bool (*)(uring_task *, bool);
    friend class io_uring_service;

  public:
    size_t bytes_transferred_;

  private:
    prepare_func_type prepare_func_; // 用于初始化sqe
    perform_func_type perform_func_; // 用于处理完成事件

  public:
    void prepare(::io_uring_sqe *sqe) { return prepare_func_(this, sqe); }
    bool perform(bool after_completion)
    { // false在请求前动作，true在请求完成后动作
        return perform_func_(this, after_completion);
    }

  protected:
    uring_task(
        callback_type complete_func,
        prepare_func_type prepare_func,
        perform_func_type perform_func);
    void perform_io(int result);
};

// iouing服务
class io_uring_service : public service_base
{
  public:
    // 一个异步操作，用于延时提交sqe
    class submit_sqes_op : async_task
    {
        io_uring_service *service_;
        submit_sqes_op(io_uring_service *s);
        static void do_complete(void *owner, async_task *base, void *);
        friend class io_uring_service;
    };
    class event_fd_read_op;
    static const int ring_size = 16384;
    static const int submit_batch_size = 128;

  private:
    scheduler_service *scheduler_;       // 指向调度器
    reactor_service *reactor_;           // 指向reactor
    std::mutex mutex_;                   // 全局互斥锁
    struct io_uring ring_;               // io_uring结构体
    submit_sqes_op submit_sqes_op_;      // 提交操作，用于延时提交
    reactor_channel *event_channel_;     // 事件通道
    std::atomic<long> outstanding_work_; // 未完成sqe数目，包括取消的请求
    int pending_sqes_;                   // 用户分配了，但并未提交的sqe数量
    int event_fd_;                       // 注册到reactor上的事件
    bool pending_submit_sqes_op_;        // 提交任务是否被调度
    bool shutdown_;                      // 服务关闭标志

  public:
    io_uring_service(service_registry *registry);
    ~io_uring_service();
    void shutdown();

    channel_task::status async_request(uring_task *op);
    void harvest(task_queue &ops);

    void cancel_ops(uring_task *op);

  private:
    void init_ring();
    ::io_uring_sqe *get_sqe();
    void submit_sqes();
    void post_submit_sqes_op(std::unique_lock<std::mutex> &lock);
    void cancel_all(); // 取消所有未完成请求
};
} // namespace wf::io