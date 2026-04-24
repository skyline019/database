////
// @file channel.cc
// @brief
// channel相关实现
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <sys/epoll.h>
#include "channel.h"
#include "reactor.h"
#include "scheduler.h"

namespace wf::io {

// NOTE: reactor_channel构造不要动前后指针
reactor_channel::reactor_channel()
    : async_task(&do_complete)
{}

void reactor_channel::do_complete(void *owner, async_task *base, void *events)
{
    if (owner) {
        reactor_channel *ch = static_cast<reactor_channel *>(base);
        if (async_task *op =
                ch->handle_event(reinterpret_cast<intptr_t>(events))) {
            op->complete(owner, 0);
        }
    }
}

struct handle_event_cleanup_on_block_exit
{
    reactor_service *reactor_;
    task_queue ops_;
    async_task *first_op_;

    explicit handle_event_cleanup_on_block_exit(reactor_service *r)
        : reactor_(r)
        , first_op_(nullptr)
    {}

    ~handle_event_cleanup_on_block_exit()
    {
        if (first_op_) {
            // 将其余异步操作直接投递到全局队列上，注意异步请求未完成，不能减
            // 未完成计数，只有当用户回调完成后，才能动未完成计数。
            if (!ops_.empty()) reactor_->scheduler_->dispatch(ops_);
        }
    }
};

async_task *reactor_channel::handle_event(uint32_t events)
{
    handle_event_cleanup_on_block_exit cleanup(reactor_); // 清扫结构

    // 要访问读写队列，加锁
    std::lock_guard<std::mutex> lock(mutex_);

    // 带外数据夹杂在正常数据里面，要先处理异常事件
    static const int flag[max_ops] = {EPOLLIN, EPOLLOUT, EPOLLPRI};
    for (int j = max_ops - 1; j >= 0; --j) { // j从高到低，先处理异常
        if (events & (flag[j] | EPOLLERR | EPOLLRDHUP | EPOLLHUP)) {
            try_speculative_[j] = true;
            while (channel_task *op = // 异步请求队头开始枚举
                   static_cast<channel_task *>(op_queue_[j].head())) {
                if (channel_task::status status = op->perform()) {
                    op_queue_[j].dequeue();    // 读写done，队头出队
                    cleanup.ops_.enqueue(*op); // 将完成的异步操作入队清扫结构
                    if (status == channel_task::done_and_exhausted) {
                        try_speculative_[j] = false; // 资源耗尽，不用再试了
                        break;
                    }
                } else
                    break;
            }
        }
    }

    // 以上操作只是完成了读写，后面还有用户回调工作，本线程继续处理第1个回调操作
    cleanup.first_op_ = cleanup.ops_.head();
    cleanup.ops_.dequeue();
    return cleanup.first_op_;
}

} // namespace wf::io
