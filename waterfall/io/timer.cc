////
// @file timer.cc
// @brief
// 定时器
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <sys/timerfd.h>
#include "error.h"
#include "timer.h"
#include "scheduler.h"
#include "reactor.h"

namespace wf::io {

bool timer_queue::enqueue_timer(
    const time_point &time,
    per_timer_data &timer,
    async_task *op)
{
    if (timer.prev_ == nullptr && &timer != timers_) {
        timer.heap_index_ = heap_.size();
        heap_entry entry = {time, &timer};
        heap_.push_back(entry);
        up_heap(heap_.size() - 1);

        timer.next_ = timers_;
        timer.prev_ = nullptr;
        if (timers_) timers_->prev_ = &timer;
        timers_ = &timer;
    }

    timer.op_queue_.enqueue(*op);
    return timer.heap_index_ == 0 && timer.op_queue_.head() == op;
}

void timer_queue::up_heap(size_t index)
{
    while (index > 0) {
        size_t parent = (index - 1) / 2;
        if (heap_[index].time_ >= heap_[parent].time_) break;
        swap_heap(index, parent);
        index = parent;
    }
}

void timer_queue::down_heap(size_t index)
{
    size_t child = index * 2 + 1;
    while (child < heap_.size()) {
        size_t min_child = (child + 1 == heap_.size() ||
                            heap_[child].time_ < heap_[child + 1].time_)
                               ? child
                               : child + 1;
        if (heap_[index].time_ < heap_[min_child].time_) break;
        swap_heap(index, min_child);
        index = min_child;
        child = index * 2 + 1;
    }
}

void timer_queue::swap_heap(size_t index1, size_t index2)
{
    heap_entry tmp = heap_[index1];
    heap_[index1] = heap_[index2];
    heap_[index2] = tmp;
    heap_[index1].timer_->heap_index_ = index1;
    heap_[index2].timer_->heap_index_ = index2;
}

long timer_queue::wait_duration_msec(long max_duration) const
{
    if (heap_.empty()) return max_duration;
    auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(
                    heap_[0].time_ - std::chrono::system_clock::now())
                    .count();
    if (msec <= 0) return 0;
    if (msec > max_duration) return max_duration;
    return static_cast<long>(msec);
}

long timer_queue::wait_duration_usec(long max_duration) const
{
    if (heap_.empty()) return max_duration;
    auto msec = std::chrono::duration_cast<std::chrono::microseconds>(
                    heap_[0].time_ - std::chrono::system_clock::now())
                    .count();
    if (msec <= 0) return 0;
    if (msec > max_duration) return max_duration;
    return static_cast<long>(msec);
}

void timer_queue::remove_timer(per_timer_data &timer)
{
    // Remove the timer from the heap.
    size_t index = timer.heap_index_;
    if (!heap_.empty() && index < heap_.size()) {
        if (index == heap_.size() - 1) {
            timer.heap_index_ = (std::numeric_limits<size_t>::max)();
            heap_.pop_back();
        } else {
            swap_heap(index, heap_.size() - 1);
            timer.heap_index_ = (std::numeric_limits<size_t>::max)();
            heap_.pop_back();
            if (index > 0 && heap_[index].time_ < heap_[(index - 1) / 2].time_)
                up_heap(index);
            else
                down_heap(index);
        }
    }

    // Remove the timer from the linked list of active timers.
    if (timers_ == &timer) timers_ = timer.next_;
    if (timer.prev_) timer.prev_->next_ = timer.next_;
    if (timer.next_) timer.next_->prev_ = timer.prev_;
    timer.next_ = nullptr;
    timer.prev_ = nullptr;
}

void timer_queue::get_ready_timers(task_queue &ops)
{
    if (!heap_.empty()) {
        time_point now = std::chrono::system_clock::now();
        while (!heap_.empty() && now >= heap_[0].time_) {
            per_timer_data *timer = heap_[0].timer_;
            while (async_task *op = timer->op_queue_.head()) {
                timer->op_queue_.dequeue();
                op->result_ = utils::system_error(ETIMEDOUT);
                ops.enqueue(*op);
            }
            remove_timer(*timer);
        }
    }
}

void timer_queue::get_all_timers(task_queue &ops)
{
    while (timers_) {
        per_timer_data *timer = timers_;
        timers_ = timers_->next_;
        ops.merge(timer->op_queue_);
        timer->next_ = nullptr;
        timer->prev_ = nullptr;
    }
    heap_.clear();
}

size_t timer_queue::cancel_timer(
    per_timer_data &timer,
    task_queue &ops,
    size_t max_cancelled)
{
    size_t num_cancelled = 0;
    if (timer.prev_ != nullptr || &timer == timers_) {
        while (async_task *op = (num_cancelled != max_cancelled)
                                    ? timer.op_queue_.head()
                                    : nullptr) {
            op->result_ = utils::system_error(ECANCELED);
            timer.op_queue_.dequeue();
            ops.enqueue(*op);
            ++num_cancelled;
        }
        if (timer.op_queue_.empty()) remove_timer(timer);
    }
    return num_cancelled;
}

bool timer_queue::is_enqueued(const per_timer_data &timer)
{
    return timer.prev_ != nullptr || &timer == timers_;
}

system_timer_service::system_timer_service(
    scheduler_service *scheduler,
    reactor_service *reactor)
    : scheduler_(scheduler)
    , reactor_(reactor)
    , timer_fd_(-1)
{
    // 创建timerfd
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    assert(timer_fd_ != -1);
    // 注册timerfd
    reactor_->register_timer(timer_fd_, this);
}

system_timer_service::~system_timer_service()
{
    if (timer_fd_ != -1) {
        ::close(timer_fd_);
        timer_fd_ = -1;
    }
}

void system_timer_service::shutdown()
{
    // 清空定时器
    task_queue ops;
    timer_queue_.get_all_timers(ops);
}

void system_timer_service::destroy(instance_type &impl) { cancel(impl); }

size_t system_timer_service::cancel(instance_type &impl)
{
    return cancel_timer(impl.timer_data);
}

void system_timer_service::schedule_timer(
    const timer_queue::time_point &time,
    timer_queue::per_timer_data &timer,
    wait_task *op)
{
    std::lock_guard<std::mutex> lock(mutex_);
    bool earliest = timer_queue_.enqueue_timer(time, timer, op);
    if (earliest) update_timeout();
}

void system_timer_service::update_timeout()
{
    itimerspec new_timeout;
    itimerspec old_timeout;
    int flags = get_timeout(new_timeout);
    ::timerfd_settime(timer_fd_, flags, &new_timeout, &old_timeout);
}

int system_timer_service::get_timeout(itimerspec &ts)
{
    ts.it_interval.tv_sec = 0;
    ts.it_interval.tv_nsec = 0;

    long usec = timer_queue_.wait_duration_usec(5 * 60 * 1000 * 1000);
    ts.it_value.tv_sec = usec / 1000000;
    ts.it_value.tv_nsec = usec ? (usec % 1000000) * 1000 : 1;

    return usec ? 0 : TFD_TIMER_ABSTIME;
}

int system_timer_service::get_timeout(int msec)
{
#if 1
    // 下一拍时间，如果reactor无事件发生，会设置5分钟定时
    const int max_msec = 3 * 60 * 1000;
    return timer_queue_.wait_duration_msec(
        (msec < 0 || max_msec < msec) ? max_msec : msec);
#else
    // devlog每3m中断一次
    if (msec < 0) msec = std::numeric_limits<int>::max();
    return timer_queue_.wait_duration_msec(msec);
#endif
}

size_t system_timer_service::cancel_timer(
    timer_queue::per_timer_data &timer,
    size_t max_cancelled)
{
    std::unique_lock<std::mutex> lock(mutex_);
    task_queue ops;
    size_t n = timer_queue_.cancel_timer(timer, ops, max_cancelled);
    lock.unlock();
    scheduler_->dispatch(ops);
    return n;
}

void system_timer_service::check_timer(task_queue &ops)
{
    std::lock_guard<std::mutex> lock(mutex_);
    timer_queue_.get_ready_timers(ops);
    itimerspec new_timeout;
    itimerspec old_timeout;
    int flags = get_timeout(new_timeout);
    ::timerfd_settime(timer_fd_, flags, &new_timeout, &old_timeout);
}

bool system_timer_service::is_started(const instance_type &impl)
{
    std::lock_guard<std::mutex> lock(mutex_);
    return timer_queue_.is_enqueued(impl.timer_data);
}

} // namespace wf::io