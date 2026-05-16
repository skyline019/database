////
// @file channel.h
// @brief
// reactor注册的回调对象，称为channel
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <mutex>
#include <waterfall/utils/noncopyable.h>
#include "task.h"

namespace wf::io {
class reactor_service;

// 操作类型
enum reactor_op_types
{
    read_op = 0,
    write_op = 1,
    except_op = 2,
    max_ops = 3
};

// channel就是一个task，它管理注册到epoll文件描述符的读写。对channel的异步读写
// 请求也是task，被挂到channel上，在文件描述符读写完成后调度执行。
class channel_task : public async_task
{
  public:
    // 文件描述符以非阻塞方式读写，下面的状态表示读写尝试的结果
    enum status
    {
        not_done = 0,
        done = 1,
        done_and_exhausted = 2
    };
    using perform_func_type = status (*)(channel_task *);

  public:
    size_t bytes_transferred_; // 传输字节数

  private:
    perform_func_type perform_func_; // 读写操作函数指针

  public:
    // 读写操作，读写done后，会调度执行用户回调函数
    status perform() { return perform_func_(this); }

  protected:
    channel_task(perform_func_type perform_func, callback_type complete_func)
        : async_task(complete_func)
        , bytes_transferred_(0)
        , perform_func_(perform_func)
    {}
};

// channel封装一个文件描述符在ractor上基本的读写操作
class reactor_channel : public async_task
{
  private:
    reactor_channel *next_;         // 后继指针，object_pool使用
    reactor_channel *prev_;         // 前驱指针
    std::mutex mutex_;              // 互斥变量
    reactor_service *reactor_;      // reactor指针
    int descriptor_;                // 文件描述符
    uint32_t registered_events_;    // 注册的事件
    task_queue op_queue_[max_ops];  // reactor_op异步操作队列
    bool try_speculative_[max_ops]; // 各读写操作是否尝试
    bool shutdown_;                 // 关闭标志，NOTE: 重要

    reactor_channel();

  private:
    async_task *handle_event(uint32_t events); // 处理读写事件
    static void do_complete(void *owner, async_task *base, void *events);

    friend class reactor_channel_pool;
    friend class reactor_service;
#if 0
    friend class channel_service;

    friend class object_pool;

    friend class io_uring_service;
    friend class reactor_channel_instance;
    template <typename Handler, typename Op>
        requires std::is_invocable_v<Handler, async_task *> &&
                 is_async_task_v<Op>
    friend class reactor_handle_wrapper;
#endif
};

} // namespace wf::io