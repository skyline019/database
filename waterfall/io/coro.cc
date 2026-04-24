////
// @file coro.cc
// @brief
// 实现协程
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "coro.h"

namespace wf::io {

void promise_base::set_continuation(promise_base *continuation) noexcept
{
    continuation_ = continuation;
}

void promise_base::task_resume(void *owner, async_task *task, void *)
{
    if (owner) {
        promise_base *promise = static_cast<promise_base *>(task);
        std::coroutine_handle<promise_base> handle =
            std::coroutine_handle<promise_base>::from_promise(*promise);
        handle.resume();
    } // 协程销毁依赖于coroutine_type
}

void promise_base::inline_resume() noexcept
{
    std::coroutine_handle<promise_base> self =
        std::coroutine_handle<promise_base>::from_promise(*this);
    self.resume();
}

void promise_base::offline_resume() noexcept
{
    runtime_executor *executor = this->get_executor();
    if (executor) { // 协程已指定执行器
        executor->execute(*this);
    } else { // 协程未指定执行器，使用当前执行器
        current_executor()->execute(*this);
    }
}

void promise_base::resume() noexcept
{
    if (state_ & kCoroStateOffline)
        offline_resume();
    else
        inline_resume();
}

void promise_base::resume_continuation() noexcept
{
    if (continuation_) continuation_->resume();
}

yield_awaiter<void> return_base<void>::yield_value()
{
    return yield_awaiter<void>{static_cast<async_promise<void> *>(this)};
}

} // namespace wf::io