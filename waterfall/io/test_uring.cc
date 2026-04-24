////
// @file test_uring.cc
// @brief
// 测试uring服务
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "service.h"
#include "thread_pool.h"
#include "scheduler.h"
#include "reactor.h"
#include "timer.h"
#include "uring.h"

namespace wf::io {

// 测试夹具，用于设置uring_service环境
class UringServiceTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // 创建service_portal实例
        registry_ = std::make_unique<service_registry>();

        thread_pool_ = registry_->use_service<thread_pool>();
        ASSERT_NE(thread_pool_, nullptr);
        scheduler_service *sc =
            registry_->use_service<scheduler_service>(registry_.get());
        ASSERT_NE(sc, nullptr);
        scheduler_ = sc;
        reactor_service *rs = registry_->has_service<reactor_service>();
        ASSERT_NE(rs, nullptr);
        reactor_ = rs;
        system_timer_service *ts =
            registry_->has_service<system_timer_service>();
        ASSERT_NE(ts, nullptr);
        io_uring_service *us =
            registry_->use_service<io_uring_service>(registry_.get());
        ASSERT_NE(us, nullptr);
        uring_service_ = us;
    }

    void TearDown() override
    {
        registry_->shutdown_services();
        registry_.reset();
    }

    void start_threads(unsigned short n)
    {
        for (unsigned short i = 0; i < n; ++i)
            thread_pool_->execute([this, i]() { scheduler_->run(i); });
    }
    void join_all() { thread_pool_->join_all(); }

    std::unique_ptr<service_registry> registry_;
    scheduler_service *scheduler_;
    reactor_service *reactor_;
    thread_pool *thread_pool_;
    io_uring_service *uring_service_;
};

// 测试io_uring_service的基本初始化
TEST_F(UringServiceTest, BasicInitialization)
{
    // 验证uring_service已正确初始化
    EXPECT_NE(uring_service_, nullptr);

    // 验证相关的服务都已正确设置
    EXPECT_NE(scheduler_, nullptr);
    EXPECT_NE(reactor_, nullptr);
    EXPECT_NE(thread_pool_, nullptr);
}

// 测试io_uring_service的异步操作提交
TEST_F(UringServiceTest, AsyncOperationSubmission)
{
    // 创建一个简单的uring操作
    class TestUringTask : public uring_task
    {
      public:
        scheduler_service *scheduler_;
        bool completed = false;

        TestUringTask(scheduler_service *scheduler)
            : uring_task(
                  &TestUringTask::do_complete,
                  &TestUringTask::do_prepare,
                  &TestUringTask::do_perform)
            , scheduler_(scheduler)
        {}

        static void do_prepare(uring_task *task, ::io_uring_sqe *sqe)
        {
            ::io_uring_prep_nop(sqe);
        }

        static bool do_perform(uring_task *task, bool after)
        {
            return after; // 在提交后完成
        }

        static void do_complete(void *owner, async_task *task, void *result)
        {
            TestUringTask *self = static_cast<TestUringTask *>(task);
            self->completed = true;
            self->result_ = ptr_to_info(result);
            self->scheduler_->stop();
        }
    };

    // 启动线程处理异步操作
    start_threads(4);

    TestUringTask task(scheduler_);

    // 提交异步操作
    auto status = uring_service_->async_request(&task);
    EXPECT_EQ(status, channel_task::not_done);

    join_all();

    // 验证操作完成状态
    EXPECT_TRUE(task.completed);
    EXPECT_EQ(task.result_, utils::OK);
}

// 测试多线程环境下的io_uring_service
TEST_F(UringServiceTest, MultiThreadedEnvironment)
{
    constexpr int num_operations = 10;
    std::atomic<int> completed_operations{0};

    class MultiThreadedUringOp : public uring_task
    {
      public:
        scheduler_service *scheduler_;
        std::atomic<int> *counter;

        MultiThreadedUringOp(
            scheduler_service *scheduler,
            std::atomic<int> *cnt)
            : uring_task(
                  &MultiThreadedUringOp::do_complete,
                  &MultiThreadedUringOp::do_prepare,
                  &MultiThreadedUringOp::do_perform)
            , scheduler_(scheduler)
            , counter(cnt)
        {}

        static void do_prepare(uring_task *op, ::io_uring_sqe *sqe)
        {
            ::io_uring_prep_nop(sqe);
        }

        static bool do_perform(uring_task *op, bool after) { return after; }

        static void do_complete(void *owner, async_task *op, void *result)
        {
            MultiThreadedUringOp *self =
                static_cast<MultiThreadedUringOp *>(op);
            if (self->counter) {
                (*self->counter)++;
                if (*self->counter == num_operations) self->scheduler_->stop();
            }
        }
    };

    // 启动多个线程处理操作
    start_threads(4);

    std::vector<std::unique_ptr<MultiThreadedUringOp>> operations;

    // 创建多个操作
    for (int i = 0; i < num_operations; ++i) {
        operations.push_back(
            std::make_unique<MultiThreadedUringOp>(
                scheduler_, &completed_operations));
    }

    // 提交所有操作
    for (auto &op : operations) {
        uring_service_->async_request(op.get());
    }

    join_all();

    // 验证所有操作都已完成
    EXPECT_EQ(completed_operations, num_operations);
}

// 测试io_uring_service的关闭功能
TEST_F(UringServiceTest, ServiceShutdown)
{
    EXPECT_FALSE(uring_service_->shutdown_);

    // 提交一个操作
    class ShutdownTestOp : public uring_task
    {
      public:
        scheduler_service *scheduler_;
        bool completed = false;

        ShutdownTestOp(scheduler_service *scheduler)
            : uring_task(
                  &ShutdownTestOp::do_complete,
                  &ShutdownTestOp::do_prepare,
                  &ShutdownTestOp::do_perform)
            , scheduler_(scheduler)
        {}

        static void do_prepare(uring_task *op, ::io_uring_sqe *sqe)
        {
            ::io_uring_prep_nop(sqe);
        }

        static bool do_perform(uring_task *op, bool after) { return after; }

        static void do_complete(void *owner, async_task *op, void *result)
        {
            ShutdownTestOp *self = static_cast<ShutdownTestOp *>(op);
            self->completed = true;
            self->result_ = ptr_to_info(result);
            self->scheduler_->stop();
        }
    };

    start_threads(4);

    ShutdownTestOp op(scheduler_);
    uring_service_->async_request(&op);

    join_all();
    EXPECT_TRUE(op.completed);
}

// 测试io_uring_service的取消操作
TEST_F(UringServiceTest, OperationCancellation)
{
    // 创建一个UDP接收操作，模拟阻塞操作以便测试取消
    class UdpReceiveOp : public uring_task
    {
      private:
        scheduler_service *scheduler_;
        int sockfd_;
        bool completed = false;
        char buffer_[1024];

      public:
        UdpReceiveOp(scheduler_service *scheduler, int sockfd)
            : uring_task(
                  &UdpReceiveOp::do_complete,
                  &UdpReceiveOp::do_prepare,
                  &UdpReceiveOp::do_perform)
            , scheduler_(scheduler)
            , sockfd_(sockfd)
        {}

        bool is_completed() const { return completed; }

      private:
        static void do_prepare(uring_task *op, struct io_uring_sqe *sqe)
        {
            UdpReceiveOp *udp_op = static_cast<UdpReceiveOp *>(op);
            ::io_uring_prep_recv(
                sqe,
                udp_op->sockfd_,
                udp_op->buffer_,
                sizeof(udp_op->buffer_),
                0);
        }

        static bool do_perform(uring_task *op, bool after) { return after; }

        static void do_complete(void *owner, async_task *op, void *result)
        {
            UdpReceiveOp *udp_op = static_cast<UdpReceiveOp *>(op);
            udp_op->completed = true;
            udp_op->result_ = ptr_to_info(result);
            udp_op->scheduler_->stop();
        }
    };
    // 启动线程处理异步操作
    start_threads(4);

    // 创建UDP socket用于测试
    int sockfd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    EXPECT_GT(sockfd_, 0);

    // 绑定到任意端口
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(0);

    int result =
        ::bind(sockfd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    EXPECT_EQ(result, 0);

    UdpReceiveOp op(scheduler_, sockfd_);

    // 提交UDP接收操作（会阻塞等待数据）
    uring_service_->async_request(&op);

    // 立即取消操作
    uring_service_->cancel_ops(&op);

    join_all();

    // 验证操作被取消
    EXPECT_TRUE(op.is_completed());
    EXPECT_EQ(op.result_, utils::system_errors::system_ECANCELED);

    // 清理socket
    close(sockfd_);
}

} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}