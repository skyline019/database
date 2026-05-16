////
// @file strand.cc
// @brief
// 实现strand服务
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "strand.h"

namespace wf::io {

strand_instance_impl::~strand_instance_impl()
{
    task_queue ops;
    std::unique_lock<std::mutex> lock(mutex_);
    ops.merge(waiting_queue_);
    ops.merge(ready_queue_);
    lock.unlock();
}

bool strand_service::enqueue(strand_instance_impl *impl, async_task &op)
{
    impl->mutex_.lock();
    if (impl->locked_) { // 对locked_加锁，后续执行时无需对就绪队列加锁
        // 其它线程正在处理，将op入队等待队列
        impl->waiting_queue_.enqueue(op);
        impl->mutex_.unlock();
        return false;
    } else {
        // 本线程正在处理，将op入就绪队列
        impl->locked_ = true;
        impl->mutex_.unlock();
        impl->ready_queue_.enqueue(op); // 就绪队列不加锁
        return true;
    }
}

bool strand_service::enqueue(strand_instance_impl *impl, task_queue &ops)
{
    impl->mutex_.lock();
    if (impl->locked_) { // 对locked_加锁，后续执行时无需对就绪队列加锁
        // 其它线程正在处理，将op入队等待队列
        impl->waiting_queue_.merge(ops);
        impl->mutex_.unlock();
        return false;
    } else {
        // 本线程正在处理，将op入就绪队列
        impl->locked_ = true;
        impl->mutex_.unlock();
        impl->ready_queue_.merge(ops); // 就绪队列不加锁
        return true;
    }
}

bool strand_service::push_waiting_to_ready(strand_instance_impl *impl)
{
    impl->mutex_.lock();
    impl->ready_queue_.merge(impl->waiting_queue_);
    bool more_handlers = impl->locked_ = !impl->ready_queue_.empty();
    impl->mutex_.unlock();
    return more_handlers;
}

void strand_service::run_ready_handlers(strand_instance_impl *impl)
{
    // call_stack<strand_instance_impl>::context ctx(impl.get());
    while (async_task *op = impl->ready_queue_.head()) {
        impl->ready_queue_.dequeue();
        op->complete(impl, 0);
    }
}

strand_instance_type strand_service::get_runtime_executor()
{
    strand_instance_type new_impl(new strand_instance_impl);
    new_impl->locked_ = false;
    new_impl->service_ = this;
    return new_impl;
}

void strand_service::execute(strand_instance_impl *impl)
{
    do {
        run_ready_handlers(impl);
    } while (push_waiting_to_ready(impl));
}

strand_executor::strand_executor(strand_service *strand)
    : strand_(strand->get_runtime_executor())
{}

void strand_executor::execute(async_task &op)
{
    // 在栈上创建执行上下文，确保在执行完成后自动销毁
    strand_executor executor((const strand_executor &) *this);
    utils::call_stack<runtime_executor>::tag tag(executor);

    if (strand_service::enqueue(strand_.get(), op))
        strand_service::execute(strand_.get());
}

void strand_executor::execute(task_queue &ops)
{
    // 在栈上创建执行上下文，确保在执行完成后自动销毁
    strand_executor executor((const strand_executor &) *this);
    utils::call_stack<runtime_executor>::tag tag(executor);

    if (strand_service::enqueue(strand_.get(), ops))
        strand_service::execute(strand_.get());
}

} // namespace wf::io