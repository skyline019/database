////
// @file async_task.h
// @brief
// 异步任务
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <assert.h>
#include "error.h"
#include "executor.h"

namespace wf::io {

// 异步任务
class async_task
{
  public:
    using callback_type = void (*)(void *, async_task *, void *);

  private:
    async_task *next_;           // 后继指针
    callback_type callback_;     // 回调函数
    runtime_executor *executor_; // 执行器

  public:
    error_info result_; // 任务错误信息

  public:
    void complete(void *owner, void *events)
    {
        callback_(owner, this, events); // 执行异步操作
    }
    void destroy() { callback_(nullptr, this, 0); }

  protected:
    explicit async_task(callback_type func)
        : next_(nullptr)
        , callback_(func)
        , executor_(nullptr)
        , result_(utils::OK)
    {
        assert(callback_ != nullptr); // 回调函数不能为空
    }
    ~async_task() = default; // null调用销毁，不需要析构函数

    // 禁止拷贝构造和移动构造
    async_task(const async_task &) = delete;
    async_task &operator=(const async_task &) = delete;
    async_task(async_task &&other) = delete;
    async_task &operator=(async_task &&other) = delete;

  public:
    // clang-format off
    void set_executor(runtime_executor *executor) noexcept { executor_ = executor; }
    runtime_executor *get_executor() const noexcept { return executor_; }
    // clang-format on

    friend class task_queue;
};

// 任务队列
class task_queue
{
  private:
    async_task *head_; // 指向队头
    async_task *tail_; // 指向队尾

  public:
    task_queue()
        : head_(nullptr)
        , tail_(nullptr)
    {}
    ~task_queue()
    {
        while (async_task *op = head_) {
            dequeue();
            op->destroy();
        }
    }

    inline async_task *head() { return head_; }
    inline void dequeue()
    {
        if (head_) {
            async_task *tmp = head_;
            head_ = head_->next_; // NOLINT(clang-diagnostic-error)
            if (head_ == nullptr) tail_ = nullptr;
            tmp->next_ = nullptr;
        }
    }
    inline void enqueue(async_task &h)
    {
        h.next_ = nullptr;
        if (tail_) {
            tail_->next_ = &h;
            tail_ = &h;
        } else {
            head_ = tail_ = &h;
        }
    }
    inline void merge(task_queue &q)
    {
        if (async_task *other_front = q.head_) {
            if (tail_)
                tail_->next_ = other_front;
            else
                head_ = other_front;
            tail_ = q.tail_;
            q.head_ = nullptr;
            q.tail_ = nullptr;
        }
    }

    inline bool empty() const { return head_ == nullptr; }

    inline bool is_queued(async_task &o) const
    {
        return o.next_ != nullptr || tail_ == &o;
    }
};

// 判定是否是async_task
template <typename T>
using is_async_task = std::is_base_of<async_task, T>;
template <typename T>
inline constexpr bool is_async_task_v = is_async_task<T>::value;

} // namespace wf::io