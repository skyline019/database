////
// @file uring.cc
// @brief
// 实现uring服务
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <sys/eventfd.h>
#include "uring.h"
#include "reactor.h"
#include "scheduler.h"

namespace wf::io {

uring_task::uring_task(
    callback_type complete_func,
    prepare_func_type prepare_func,
    perform_func_type perform_func)
    : async_task(complete_func)
    , bytes_transferred_(0)
    , prepare_func_(prepare_func)
    , perform_func_(perform_func)
{}

io_uring_service::io_uring_service(service_registry *registry)
    : scheduler_(registry->has_service<scheduler_service>())
    , reactor_(registry->has_service<reactor_service>())
    , submit_sqes_op_(this)
    , event_channel_(nullptr)
    , outstanding_work_(0)
    , pending_sqes_(0)
    , event_fd_(-1)
    , pending_submit_sqes_op_(false)
    , shutdown_(false)
{
    assert(scheduler_ != nullptr);
    assert(reactor_ != nullptr);
    init_ring();
}

io_uring_service::~io_uring_service()
{
    if (ring_.ring_fd != -1) ::io_uring_queue_exit(&ring_);
    if (event_fd_ != -1) ::close(event_fd_);
}

class io_uring_service::event_fd_read_op : public channel_task
{
  private:
    io_uring_service *service_;

  public:
    event_fd_read_op(io_uring_service *s)
        : channel_task(
              &event_fd_read_op::do_perform,
              event_fd_read_op::do_complete)
        , service_(s)
    {}

    static status do_perform(channel_task *base)
    {
        event_fd_read_op *self = static_cast<event_fd_read_op *>(base);

        // 读eventfd确认事件
        while (true) {
            uint64_t counter(0);
            errno = 0;
            int bytes_read =
                ::read(self->service_->event_fd_, &counter, sizeof(uint64_t));
            if (bytes_read < 0 && errno == EINTR)
                continue;
            else
                break;
        }

        // 收割完成操作
        task_queue ops;
        self->service_->harvest(ops);
        self->service_->scheduler_->dispatch(ops);

        // 总是返回not_done
        return not_done;
    }
    static void do_complete(void *, async_task *base, void *)
    {
        // 内部事件，只有在析构的时候才会被调用
        event_fd_read_op *self = static_cast<event_fd_read_op *>(base);
        delete self; // 释放event_fd_read_op
    }
};

void io_uring_service::init_ring()
{
    // 初始化uring结构
    int ret = ::io_uring_queue_init(ring_size, &ring_, 0);
    assert(ret == 0);
    // 创建eventfd，非阻塞
    event_fd_ = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    assert(event_fd_ >= 0);
    // 关联eventfd
    ret = ::io_uring_register_eventfd(&ring_, event_fd_);
    assert(ret == 0);
    // 注册到reactor
    reactor_->register_internal_descriptor(
        reactor_op_types::read_op,
        event_fd_,
        event_channel_,
        new event_fd_read_op(this));
}

void io_uring_service::harvest(task_queue &ops)
{
    ::io_uring_cqe *cqe = 0;
    int count = 0;

    while (::io_uring_peek_cqe(&ring_, &cqe) == 0) {
        // 从cq上取下完成的请求
        void *ptr = ::io_uring_cqe_get_data(cqe);
        if (ptr) {
            // 取消操作是空指针，包括指向sqe的和null的两种情况
            uring_task *op = static_cast<uring_task *>(ptr);
            op->perform_io(cqe->res); // 结果暂存在uring_queue上
            ops.enqueue(*op);
        }
        // 转动cq
        ::io_uring_cqe_seen(&ring_, cqe);
        ++count; // 统计完成请求的个数
    }

    outstanding_work_ -= count;
}

channel_task::status io_uring_service::async_request(uring_task *op)
{
    // 限制异步请求数量，保留一半作为cancel
    if (outstanding_work_ >= ring_size / 2) {
        op->result_ = utils::system_errors::system_ENOBUFS;
        op->complete(this, info_to_ptr(op->result_));
        return channel_task::done;
    }

    // 先尝试一把
    if (op->perform(false)) {
        // 尝试成功，直接调度执行
        op->complete(this, 0);
        return channel_task::done;
    } else {
        // 分配sqe
        std::unique_lock<std::mutex> lock(mutex_);

        // 检查是否已经关闭
        if (shutdown_) {
            op->result_ = utils::system_errors::system_ESHUTDOWN;
            lock.unlock();
            op->complete(this, info_to_ptr(op->result_));
            return channel_task::done;
        }

        ::io_uring_sqe *sqe = get_sqe();
        // 填充sqe
        op->prepare(sqe);
        ::io_uring_sqe_set_data(sqe, op);
        // 提交
        post_submit_sqes_op(lock);
    }

    return channel_task::not_done;
}

::io_uring_sqe *io_uring_service::get_sqe()
{
    ::io_uring_sqe *sqe = ::io_uring_get_sqe(&ring_);
    if (!sqe) {
        // sq上没有sqe，提交一把，期望能获得空闲sqe，其实可能性很小
        submit_sqes();
        sqe = ::io_uring_get_sqe(&ring_);
    }
    if (sqe) {
        // 清空sqe
        ::io_uring_sqe_set_data(sqe, 0);
        ++pending_sqes_;
    }
    return sqe;
}

void io_uring_service::submit_sqes()
{
    if (pending_sqes_ != 0) {
        int result = ::io_uring_submit(&ring_);
        if (result > 0) {
            pending_sqes_ -= result;
            outstanding_work_ += result;
        }
        // 如果提交的sqe错误，会在cqe上返回错误码
    }
}

void io_uring_service::post_submit_sqes_op(std::unique_lock<std::mutex> &lock)
{
    if (pending_sqes_ >= submit_batch_size) {
        // 数目够多，批处理提交
        submit_sqes();
    } else if (pending_sqes_ != 0 && !pending_submit_sqes_op_) {
        // 数目不够，调度一个提交操作，相当于延迟一段时间提交
        pending_submit_sqes_op_ = true;
        lock.unlock();
        scheduler_->dispatch(submit_sqes_op_);
    }
}

io_uring_service::submit_sqes_op::submit_sqes_op(io_uring_service *s)
    : async_task(&io_uring_service::submit_sqes_op::do_complete)
    , service_(s)
{}

void io_uring_service::submit_sqes_op::do_complete(
    void *owner,
    async_task *base,
    void *)
{
    if (owner) {
        submit_sqes_op *self = static_cast<submit_sqes_op *>(base);
        std::unique_lock<std::mutex> lock(self->service_->mutex_);
        // 提交sqe
        self->service_->submit_sqes();
        if (self->service_->pending_sqes_ != 0)
            // 提交数目不够，再次调度自己，延时提交
            self->service_->scheduler_->dispatch(*self);
        else // 完全提交，置空
            self->service_->pending_submit_sqes_op_ = false;
    }
}

void io_uring_service::cancel_ops(uring_task *op)
{
    if (op == nullptr) return;
    std::unique_lock<std::mutex> lock(mutex_);
    ::io_uring_sqe *sqe = get_sqe();
    ::io_uring_prep_cancel(sqe, op, 0);
    submit_sqes();
}

void io_uring_service::shutdown()
{
    std::unique_lock<std::mutex> lock(mutex_);
    shutdown_ = true;
    lock.unlock();

    cancel_all(); // 等待取消完成
}

void io_uring_service::cancel_all()
{
    // 利用IORING_ASYNC_CANCEL_ALL标志，取消所有请求
    struct io_uring_sqe *sqe = get_sqe();
    // sqe指向nullptr，cqe侧返回的ptr也为空
    ::io_uring_prep_cancel(sqe, nullptr, IORING_ASYNC_CANCEL_ALL);

    // 同步等待取消完成
    task_queue ops;
    for (; outstanding_work_ > 0; --outstanding_work_) {
        ::io_uring_cqe *cqe = nullptr;
        if (::io_uring_wait_cqe(&ring_, &cqe) != 0) break; // 出错
        // ptr必须是非空，取消操作是空指针
        if (void *ptr = ::io_uring_cqe_get_data(cqe)) {
            uring_task *op = static_cast<uring_task *>(ptr);
            // 处理cqe结果
            op->perform_io(cqe->res);
            ops.enqueue(*op);
        }
    }
    // 调度完成操作
    scheduler_->dispatch(ops);
}

void uring_task::perform_io(int result)
{
    // 返回的cqe对应于队头请求
    if (result < 0) {
        // 出错，result是-errno，转成error_info
        result_ = utils::system_error(-result);
        bytes_transferred_ = 0;
    } else {
        result_ = utils::OK;
        bytes_transferred_ = static_cast<size_t>(result);
    }

    // 调用perform动作
    perform(true);
}

} // namespace wf::io