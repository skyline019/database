////
// @file scheduler.cc
// @brief
// 实现调度器
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <assert.h>
#include "scheduler.h"
#include "thread_pool.h"
#include "service.h"
#include "reactor.h"

namespace wf::io {

scheduler_service::scheduler_service(service_registry *registry)
    : reactor_(nullptr)
    , service_registry_(registry)
    , root_executor_(this)
    , root_tag_(root_executor_)
    , waiting_threads_(0)
    , reactor_waiting_(false)
    , stopped_(false)
{
    // 必须先加载thread_pool
    thread_pool *tp = service_registry_->has_service<thread_pool>();
    assert(tp != nullptr);
    // 加载ractor注册服务
    reactor_service *rs = service_registry_->use_service<reactor_service>(
        service_registry_, this);
    assert(rs != nullptr);
    // 设置主线程根执行器
    utils::call_stack<runtime_executor>::set_top(&root_tag_);
}

scheduler_service::~scheduler_service()
{
    // 清掉主线程根执行器
    utils::call_stack<runtime_executor>::set_top(nullptr);
}

void scheduler_service::shutdown()
{
    // 线程已经退出，清空全局队列
    std::unique_lock<std::mutex> lock(mutex_);
    while (!global_queue_.empty()) {
        async_task *t = global_queue_.head();
        global_queue_.dequeue(); // 总是出队
        if (t != reactor_)       // reactor_由reactor释放
            t->destroy();        // 销毁多余的任务
    }
    reactor_ = nullptr; // 补一手，防止reactor不在队列中
}

void scheduler_service::run(unsigned short index)
{
    // 栈上构建worker_info
    worker_info worker{
        .local_queue_ = task_queue{},
        .error_ = nullptr,
        .handled_ = 0,
        .index_ = index,
    };

    // 设置worker线程根执行器
    utils::call_stack<runtime_executor>::set_top(&root_tag_);

    mutex_.lock();
    workers_.push_back(&worker);
    mutex_.unlock();

    // 进入循环
    while (do_run_one(worker)) // do_run_one返回0退出
        ++worker.handled_;
}

void scheduler_service::wakeup_one_thread_release_lock(
    std::unique_lock<std::mutex> &lock)
{
    // lock仍然加锁
    if (waiting_threads_ == 0) {
        // 没有等待线程，尝试唤醒reactor线程
        // wakeup_reactor();
    } else {
        // 有等待线程，唤醒一个线程
        cond_.notify_one();
    }
    lock.unlock();
}

// do_run_one异步操作的清扫对象
struct work_cleanup
{
    scheduler_service *scheduler_;
    std::unique_lock<std::mutex> *lock_;
    scheduler_service::worker_info *info_;

    ~work_cleanup()
    {
        // lock是释放的，异步操作完毕，将私有队列中的操作转移到全局队列中
        if (!info_->local_queue_.empty()) {
            lock_->lock(); // 重新加锁
            scheduler_->global_queue_.merge(info_->local_queue_);
        } // lock未释放，后续从do_run_one退出后，释放锁
    }
};

struct reactor_cleanup
{
    scheduler_service *scheduler_;
    std::unique_lock<std::mutex> *lock_;
    scheduler_service::worker_info *info_;

    ~reactor_cleanup()
    {
        lock_->lock();
        scheduler_->reactor_waiting_ = false;
        scheduler_->global_queue_.merge(info_->local_queue_);
        scheduler_->global_queue_.enqueue(*scheduler_->reactor_);
    }
};

size_t scheduler_service::do_run_one(worker_info &info)
{
    // lock此时加锁
    std::unique_lock<std::mutex> lock(mutex_);

    while (!stopped_) {
        if (!global_queue_.empty()) {
            // 队列非空，取出队头元素
            async_task *o = global_queue_.head();
            global_queue_.dequeue(); // 取出后，队列是非空？
            bool more_handlers = !global_queue_.empty();

            if (o == reactor_) {
                reactor_waiting_ = !more_handlers;

                if (more_handlers)
                    wakeup_one_thread_release_lock(lock);
                else
                    lock.unlock();

                reactor_cleanup on_exit = {this, &lock, &info};
                (void) on_exit;

                // 跑epoll，队列仍有操作，则只是poll
                reactor_->run(more_handlers ? 0 : -1, info.local_queue_);

                // 不需要重新加锁，task_cleanup析构会重新加锁
            } else {
                // 取出的是一个异步操作
                int result = reinterpret_cast<intptr_t>(
                    o->result_); // 在锁内取出，这个位置放到是事件

                if (more_handlers) // 唤醒下一个线程，必须在锁内
                    wakeup_one_thread_release_lock(lock);
                else
                    lock.unlock();

                // 设置清扫对象
                work_cleanup on_exit = {this, &lock, &info};
                (void) on_exit;

                // 执行异步操作
                o->complete(this, reinterpret_cast<void *>(result));

                return 1;
            }
        } else {
            // 在加锁中等待，阻塞的同时释放锁
            ++waiting_threads_;
            cond_.wait(lock);
            --waiting_threads_;
            // 从阻塞态退出，重新加锁
        }
    }

    // 锁未释放，由析构函数释放
    return 0;
}

void scheduler_service::dispatch(async_task &op)
{
    std::unique_lock<std::mutex> lock(mutex_);
    global_queue_.enqueue(op);
    wakeup_one_thread_release_lock(lock);
}

void scheduler_service::dispatch(task_queue &ops)
{
    std::unique_lock<std::mutex> lock(mutex_);
    global_queue_.merge(ops);
    wakeup_one_thread_release_lock(lock);
}

void scheduler_service::stop()
{
    std::unique_lock<std::mutex> lock(mutex_);
    stopped_ = true;
    cond_.notify_all();

    // 唤醒reactor线程
    wakeup_reactor();
    // 清空所有worker_info
    workers_.clear();

    // 停止后，reactor仍然在队
}

void scheduler_service::restart()
{
    std::unique_lock<std::mutex> lock(mutex_);
    stopped_ = false;
}

bool scheduler_service::stopped()
{
    std::unique_lock<std::mutex> lock(mutex_);
    return stopped_;
}

void scheduler_service::wakeup_reactor()
{
    // NOTE: 必须加锁
    if (reactor_waiting_ && reactor_) {
        reactor_waiting_ = false;
        reactor_->interrupt();
    }
}
void scheduler_service::enqueue_reactor(reactor_dummy_task *r)
{
    std::unique_lock<std::mutex> lock(mutex_);
    reactor_ = r;
    reactor_waiting_ = false;
    global_queue_.enqueue(*reactor_);
    wakeup_one_thread_release_lock(lock);
}

runtime_executor *scheduler_service::get_runtime_executor()
{
    return new scheduler_executor(this);
}

void scheduler_executor::execute(async_task &task)
{
    scheduler_->dispatch(task);
}
void scheduler_executor::execute(task_queue &queue)
{
    scheduler_->dispatch(queue);
}

} // namespace wf::io