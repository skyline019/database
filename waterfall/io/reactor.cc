////
// @file reactor.cc
// @brief
// 实现reactor
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <errno.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include "reactor.h"
#include "scheduler.h"
#include "timer.h"

namespace wf::io {

reactor_service::reactor_service(
    service_registry *registry,
    scheduler_service *sc)
    : service_registry_(registry)
    , scheduler_(sc)
    , dummy_(nullptr)
    , timer_service_(nullptr)
    , epoll_fd_(-1)
    , interrupt_fd_(-1)
{
    // 创建文件描述符
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    assert(epoll_fd_ != -1);
    interrupt_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    assert(interrupt_fd_ != -1);

    // 创建调度器哑操作
    dummy_ = new scheduler_service::reactor_dummy_task();
    assert(dummy_ != nullptr);
    static_cast<scheduler_service::reactor_dummy_task *>(dummy_)->reactor_ =
        this;
    scheduler_->enqueue_reactor( // 将reactor入队
        static_cast<scheduler_service::reactor_dummy_task *>(dummy_));

    // 拉起timer服务
    timer_service_ =
        service_registry_->use_service<system_timer_service>(scheduler_, this);
    assert(timer_service_);

    // 添加中断
    epoll_event ev = {0, {0}};
    ev.events = EPOLLIN | EPOLLERR | EPOLLET; // 边沿触发，无需处理事件
    ev.data.ptr = &interrupt_fd_;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, interrupt_fd_, &ev);
    // 触发一个写，后续不处理
    uint64_t counter(1UL);
    int result = ::write(interrupt_fd_, &counter, sizeof(uint64_t));
    (void) result;
}

reactor_service::~reactor_service()
{
    // 关闭各文件描述符
    if (interrupt_fd_ != -1) {
        ::close(interrupt_fd_);
        interrupt_fd_ = -1;
    }
    if (epoll_fd_ != -1) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }

    // 销毁放到scheduler的异步操作，scheduler应该先被销毁
    static_cast<scheduler_service::reactor_dummy_task *>(dummy_)->destroy();
}

void reactor_service::shutdown()
{
    task_queue ops;

    while (reactor_channel *channel = channel_pool_.first()) {
        std::lock_guard<std::mutex> lock(channel->mutex_);
        for (int i = 0; i < max_ops; ++i)
            ops.merge(channel->op_queue_[i]);
        channel->shutdown_ = true;
        channel_pool_.free(channel);
    }

    // 所有遗留操作被销毁
}

error_info
reactor_service::register_descriptor(int descriptor, reactor_channel *&channel)
{
    channel = allocate_channel();

    {
        std::lock_guard<std::mutex> lock(channel->mutex_);
        channel->reactor_ = this;
        channel->descriptor_ = descriptor;
        channel->shutdown_ = false;
        for (int i = 0; i < max_ops; ++i)
            channel->try_speculative_[i] = true;
    }

    epoll_event ev = {0, {0}};
    ev.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLRDHUP | EPOLLHUP |
                EPOLLPRI | EPOLLET; // 全事件注册
    channel->registered_events_ = ev.events;
    ev.data.ptr = channel;
    int result = ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, descriptor, &ev);
    if (result != 0) {
        if (errno == EPERM) {
            // This file descriptor type is not supported by epoll. However, if
            // it is a regular file then operations on it will not block. We
            // will allow this descriptor to be used and fail later if an
            // operation on it would otherwise require a trip through the
            // reactor.
            channel->registered_events_ = 0;
            return nullptr;
        }
        return utils::system_error(errno);
    }

    return utils::OK;
}

error_info reactor_service::register_internal_descriptor(
    int op_type,
    int descriptor,
    reactor_channel *&channel,
    channel_task *op)
{
    channel = allocate_channel();

    {
        std::lock_guard<std::mutex> descriptor_lock(channel->mutex_);
        channel->reactor_ = this;
        channel->descriptor_ = descriptor;
        channel->shutdown_ = false;
        channel->op_queue_[op_type].enqueue(*op);
        for (int i = 0; i < max_ops; ++i)
            channel->try_speculative_[i] = true;
    }

    epoll_event ev = {0, {0}};
    ev.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLRDHUP | EPOLLHUP |
                EPOLLPRI | EPOLLET; // 全事件注册
    channel->registered_events_ = ev.events;
    ev.data.ptr = channel;
    int result = ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, descriptor, &ev);
    if (result != 0) return utils::system_error(errno);

    return utils::OK;
}

void reactor_service::deregister_descriptor(
    int descriptor,
    reactor_channel *&channel,
    bool closing)
{
    if (!channel) return;

    std::unique_lock<std::mutex> descriptor_lock(channel->mutex_);

    if (!channel->shutdown_) {
        if (closing) {
            // 文件关闭时，文件描述符将自动从epoll里移除，这里无需处理
        } else if (channel->registered_events_ != 0) {
            epoll_event ev = {0, {0}};
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, descriptor, &ev);
        }

        task_queue ops;
        for (int i = 0; i < max_ops; ++i) {
            while (channel_task *op = static_cast<channel_task *>(
                       channel->op_queue_[i].head())) {
                op->result_ = utils::system_error(ECANCELED);
                channel->op_queue_[i].dequeue();
                ops.enqueue(*op);
            }
        }

        channel->descriptor_ = -1;
        channel->shutdown_ = true;

        descriptor_lock.unlock();

        scheduler_->dispatch(ops);

        // Leave channel set so that it will be freed by the subsequent
        // call to cleanup_channel.
    } else {
        // We are shutting down, so prevent cleanup_channel from freeing
        // the channel object and let the destructor free it instead.
        channel = nullptr;
    }
}

void reactor_service::deregister_internal_descriptor(
    int descriptor,
    reactor_channel *&channel)
{
    if (!channel) return;

    std::unique_lock<std::mutex> descriptor_lock(channel->mutex_);

    if (!channel->shutdown_) {
        epoll_event ev = {0, {0}};
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, descriptor, &ev);

        task_queue ops;
        for (int i = 0; i < max_ops; ++i)
            ops.merge(channel->op_queue_[i]);

        channel->descriptor_ = -1;
        channel->shutdown_ = true;

        descriptor_lock.unlock();

        // Leave channel set so that it will be freed by the subsequent
        // call to cleanup_descriptor_data.
    } else {
        // We are shutting down, so prevent cleanup_descriptor_data from freeing
        // the channel object and let the destructor free it instead.
        channel = nullptr;
    }
}

reactor_channel *reactor_service::allocate_channel()
{
    std::lock_guard<std::mutex> lock(pool_mutex_);
    return channel_pool_.alloc();
}

void reactor_service::cleanup_channel(reactor_channel *&channel)
{
    if (channel) {
        free_channel(channel);
        channel = nullptr;
    }
}

void reactor_service::free_channel(reactor_channel *s)
{
    std::lock_guard<std::mutex> descriptors_lock(pool_mutex_);
    channel_pool_.free(s);
}

void reactor_service::interrupt()
{
    // EPOLL_CTL_MOD，强制epoll检查找回被ET丢失的事件
    epoll_event ev = {0, {0}};
    ev.events = EPOLLIN | EPOLLERR | EPOLLET;
    ev.data.ptr = &interrupt_fd_;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, interrupt_fd_, &ev);
}

channel_task::status reactor_service::async_request(
    int op_type,
    int descriptor,
    reactor_channel *&channel,
    channel_task *op)
{
    if (!channel) {
        op->result_ = utils::system_error(EBADF);
        op->complete(this, info_to_ptr(op->result_));
        return channel_task::done;
    }

    std::unique_lock<std::mutex> descriptor_lock(channel->mutex_);

    if (channel->shutdown_) {
        op->result_ = utils::system_error(ECANCELED);
        op->complete(this, info_to_ptr(op->result_));
        return channel_task::done;
    }

    if (channel->op_queue_[op_type].empty()) {
        if ((op_type == read_op && channel->op_queue_[except_op].empty()) ||
            op_type == write_op || op_type == except_op) {
            // write_op || except_op || (read_op && 异常队列空)
            if (channel->try_speculative_[op_type]) {
                if (channel_task::status status = op->perform()) {
                    if (status == channel_task::done_and_exhausted)
                        if (channel->registered_events_ != 0)
                            channel->try_speculative_[op_type] = false;
                    descriptor_lock.unlock();
                    // 尝试成功，直接回调
                    op->complete(this, info_to_ptr(op->result_));
                    return status;
                }
            }
        }
    }

    channel->op_queue_[op_type].enqueue(*op);
    return channel_task::not_done;
}

void reactor_service::harvest(long usec, task_queue &ops)
{
    // This code relies on the fact that the scheduler queues the reactor task
    // behind all descriptor operations generated by this function. This means,
    // that by the time we reach this point, any previously returned descriptor
    // operations have already been dequeued. Therefore it is now safe for us to
    // reuse and return them for the scheduler to queue again.

    int timeout;
    if (usec == 0)
        timeout = 0;
    else {
        timeout = (usec < 0) ? -1 : ((usec - 1) / 1000 + 1);
    }

    // Block on the epoll descriptor.
    ::epoll_event events[max_events];
    int num_events = ::epoll_wait(epoll_fd_, events, max_events, timeout);

    bool check_timers = false;

    // Dispatch the waiting events.
    for (int i = 0; i < num_events; ++i) {
        void *ptr = events[i].data.ptr;
        if (ptr == &interrupt_fd_) {
            // 中断不需要去写eventfd确认
        } else if (ptr == timer_service_) {
            check_timers = true;
            // 不在这里处理定时器，先处理其它事件
        } else {
            // 这段代码确实精彩！
            reactor_channel *channel = static_cast<reactor_channel *>(ptr);
            if (likely(!ops.is_queued(*channel))) {
                channel->result_ = int_to_info(events[i].events);
                ops.enqueue(*channel);
            } else {
                channel->result_ = utils::system_error(
                    info_to_int(channel->result_) | events[i].events);
            }
        }
    }

    // 在返回前处理定时器，而不是由队列异步处理，保证超时事件的时效性
    if (check_timers) timer_service_->check_timer(ops);
}

void reactor_service::register_timer(int fd, void *ptr)
{
    epoll_event ev = {0, {0}};
    ev.events = EPOLLIN | EPOLLERR;
    ev.data.ptr = ptr; // timer_service初始化时，timer_service_指针仍为null
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
}

// scheduler_service中reactor_dummy_task的实现
void reactor_service::dummy_reactor_func(void *owner, async_task *op, void *)
{
    scheduler_service::reactor_dummy_task *rop =
        static_cast<scheduler_service::reactor_dummy_task *>(op);
    if (!owner) delete rop;
}
scheduler_service::reactor_dummy_task::reactor_dummy_task()
    : async_task(reactor_service::dummy_reactor_func)
{}
void scheduler_service::reactor_dummy_task::run(long usec, task_queue &ops)
{
    reactor_->harvest(usec, ops);
}
void scheduler_service::reactor_dummy_task::interrupt()
{
    reactor_->interrupt();
}

// channel_pool
reactor_channel *reactor_channel_pool::alloc()
{
    reactor_channel *o = free_list_;
    if (o)
        free_list_ = free_list_->next_;
    else
        o = new reactor_channel;

    o->next_ = live_list_;
    o->prev_ = nullptr;
    if (live_list_) live_list_->prev_ = o;
    live_list_ = o;

    return o;
}

void reactor_channel_pool::free(reactor_channel *o)
{
    if (live_list_ == o) live_list_ = o->next_;
    if (o->prev_) o->prev_->next_ = o->next_;
    if (o->next_) o->next_->prev_ = o->prev_;

    o->next_ = free_list_;
    o->prev_ = nullptr;
    free_list_ = o;
}

void reactor_channel_pool::destroy_list(reactor_channel *list)
{
    while (list) {
        reactor_channel *o = list;
        list = o->next_;
        delete o;
    }
}

} // namespace wf::io