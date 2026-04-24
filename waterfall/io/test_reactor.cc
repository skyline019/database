////
// @file test_reactor.cc
// @brief
// 测试reactor
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <atomic>
#include <chrono>
#include <thread>
#include "reactor.h"
#include "scheduler.h"
#include "thread_pool.h"
#include "service.h"

namespace wf::io {

// reactor_channel_pool测试夹具
class ReactorChannelPoolTest : public ::testing::Test
{
  protected:
    void SetUp() override { pool_ = std::make_unique<reactor_channel_pool>(); }

    void TearDown() override { pool_.reset(); }

    std::unique_ptr<reactor_channel_pool> pool_;
};

// 测试1：基础分配和释放功能
TEST_F(ReactorChannelPoolTest, BasicAllocFree)
{
    // 初始状态下，活跃链表应该为空
    EXPECT_EQ(pool_->first(), nullptr);

    // 分配第一个channel
    reactor_channel *channel1 = pool_->alloc();
    EXPECT_NE(channel1, nullptr);
    EXPECT_EQ(pool_->first(), channel1);

    // 分配第二个channel
    reactor_channel *channel2 = pool_->alloc();
    EXPECT_NE(channel2, nullptr);
    EXPECT_EQ(pool_->first(), channel2); // 新分配的应该在链表头部

    // 释放第一个channel
    pool_->free(channel1);
    EXPECT_EQ(pool_->first(), channel2); // 链表头部应该是第二个channel

    // 释放第二个channel
    pool_->free(channel2);
    EXPECT_EQ(pool_->first(), nullptr); // 链表应该为空
}

// 测试2：内存池重用功能
TEST_F(ReactorChannelPoolTest, MemoryReuse)
{
    // 分配并释放多个channel
    std::vector<reactor_channel *> channels;

    // 分配5个channel
    for (int i = 0; i < 5; ++i) {
        reactor_channel *channel = pool_->alloc();
        EXPECT_NE(channel, nullptr);
        channels.push_back(channel);
    }

    // 验证链表结构
    reactor_channel *current = pool_->first();
    int count = 0;
    while (current != nullptr) {
        ++count;
        current = current->next_;
    }
    EXPECT_EQ(count, 5);

    // 释放所有channel
    for (reactor_channel *channel : channels) {
        pool_->free(channel);
    }

    // 重新分配，应该重用之前的内存
    std::vector<reactor_channel *> reused_channels;
    for (int i = 0; i < 5; ++i) {
        reactor_channel *channel = pool_->alloc();
        EXPECT_NE(channel, nullptr);
        reused_channels.push_back(channel);
    }

    // 验证链表结构
    current = pool_->first();
    count = 0;
    while (current != nullptr) {
        ++count;
        current = current->next_;
    }
    EXPECT_EQ(count, 5);

    // 清理
    for (reactor_channel *channel : reused_channels) {
        pool_->free(channel);
    }
}

// 测试3：析构函数正确清理
TEST_F(ReactorChannelPoolTest, DestructorCleanup)
{
    // 分配一些channel但不释放
    std::vector<reactor_channel *> channels;
    for (int i = 0; i < 3; ++i) {
        reactor_channel *channel = pool_->alloc();
        EXPECT_NE(channel, nullptr);
        channels.push_back(channel);
    }

    // 析构函数应该自动清理所有内存
    // 这里主要验证没有内存泄漏
    pool_.reset();

    // 重新创建pool验证可以正常工作
    pool_ = std::make_unique<reactor_channel_pool>();
    reactor_channel *new_channel = pool_->alloc();
    EXPECT_NE(new_channel, nullptr);
    pool_->free(new_channel);
}

// 测试4：链表操作正确性
TEST_F(ReactorChannelPoolTest, LinkedListOperations)
{
    // 分配多个channel并验证链表结构
    reactor_channel *channel1 = pool_->alloc();
    reactor_channel *channel2 = pool_->alloc();
    reactor_channel *channel3 = pool_->alloc();

    // 验证链表顺序：channel3 -> channel2 -> channel1
    EXPECT_EQ(pool_->first(), channel3);
    EXPECT_EQ(channel3->next_, channel2);
    EXPECT_EQ(channel2->next_, channel1);
    EXPECT_EQ(channel1->next_, nullptr);

    // 验证前驱指针
    EXPECT_EQ(channel3->prev_, nullptr);
    EXPECT_EQ(channel2->prev_, channel3);
    EXPECT_EQ(channel1->prev_, channel2);

    // 释放中间的channel2
    pool_->free(channel2);

    // 验证链表结构：channel3 -> channel1
    EXPECT_EQ(pool_->first(), channel3);
    EXPECT_EQ(channel3->next_, channel1);
    EXPECT_EQ(channel1->next_, nullptr);
    EXPECT_EQ(channel1->prev_, channel3);

    // 清理
    pool_->free(channel1);
    pool_->free(channel3);
}

// 测试5：压力测试 - 大量分配和释放（带性能统计）
TEST_F(ReactorChannelPoolTest, StressTest)
{
    const int NUM_ITERATIONS = 1000;
    const int NUM_CHANNELS = 5000;

    // 性能统计变量
    long long total_alloc_time_ns = 0;
    long long total_free_time_ns = 0;
    long long total_iteration_time_ns = 0;
    int fastest_iteration_ns = std::numeric_limits<int>::max();
    int slowest_iteration_ns = 0;

    for (int iter = 0; iter < NUM_ITERATIONS; ++iter) {
        auto iteration_start = std::chrono::high_resolution_clock::now();

        std::vector<reactor_channel *> channels;

        // 分配阶段计时
        auto alloc_start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < NUM_CHANNELS; ++i) {
            reactor_channel *channel = pool_->alloc();
            EXPECT_NE(channel, nullptr);
            channels.push_back(channel);
        }
        auto alloc_end = std::chrono::high_resolution_clock::now();
        long long alloc_time_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                alloc_end - alloc_start)
                .count();
        total_alloc_time_ns += alloc_time_ns;

        // 验证链表长度
        reactor_channel *current = pool_->first();
        int count = 0;
        while (current != nullptr) {
            ++count;
            current = current->next_;
        }
        EXPECT_EQ(count, NUM_CHANNELS);

        // 释放阶段计时
        auto free_start = std::chrono::high_resolution_clock::now();
        for (reactor_channel *channel : channels) {
            pool_->free(channel);
        }
        auto free_end = std::chrono::high_resolution_clock::now();
        long long free_time_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                free_end - free_start)
                .count();
        total_free_time_ns += free_time_ns;

        // 验证链表为空
        EXPECT_EQ(pool_->first(), nullptr);

        auto iteration_end = std::chrono::high_resolution_clock::now();
        long long iteration_time_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                iteration_end - iteration_start)
                .count();
        total_iteration_time_ns += iteration_time_ns;

        // 更新最快和最慢迭代时间
        if (iteration_time_ns < fastest_iteration_ns) {
            fastest_iteration_ns = iteration_time_ns;
        }
        if (iteration_time_ns > slowest_iteration_ns) {
            slowest_iteration_ns = iteration_time_ns;
        }
    }

    // 计算性能指标
    double avg_alloc_time_ns =
        static_cast<double>(total_alloc_time_ns) / NUM_ITERATIONS;
    double avg_free_time_ns =
        static_cast<double>(total_free_time_ns) / NUM_ITERATIONS;
    double avg_iteration_time_ns =
        static_cast<double>(total_iteration_time_ns) / NUM_ITERATIONS;

    // 计算吞吐量（操作/秒）
    double alloc_throughput =
        (NUM_CHANNELS * NUM_ITERATIONS * 1e9) / total_alloc_time_ns;
    double free_throughput =
        (NUM_CHANNELS * NUM_ITERATIONS * 1e9) / total_free_time_ns;
    double total_throughput =
        (2 * NUM_CHANNELS * NUM_ITERATIONS * 1e9) / total_iteration_time_ns;

    // 输出性能统计信息
    std::cout << "\n=== ReactorChannelPool StressTest 性能统计 ==="
              << std::endl;
    std::cout << "测试配置:" << std::endl;
    std::cout << "  - 迭代次数: " << NUM_ITERATIONS << std::endl;
    std::cout << "  - 每次分配/释放对象数: " << NUM_CHANNELS << std::endl;
    std::cout << "  - 总操作数: " << 2 * NUM_CHANNELS * NUM_ITERATIONS
              << std::endl;

    std::cout << "\n时间统计 (纳秒):" << std::endl;
    std::cout << "  - 平均分配时间: " << avg_alloc_time_ns << " ns"
              << std::endl;
    std::cout << "  - 平均释放时间: " << avg_free_time_ns << " ns" << std::endl;
    std::cout << "  - 平均迭代时间: " << avg_iteration_time_ns << " ns"
              << std::endl;
    std::cout << "  - 最快迭代时间: " << fastest_iteration_ns << " ns"
              << std::endl;
    std::cout << "  - 最慢迭代时间: " << slowest_iteration_ns << " ns"
              << std::endl;

    std::cout << "\n吞吐量统计 (操作/秒):" << std::endl;
    std::cout << "  - 分配吞吐量: " << alloc_throughput << " ops/s"
              << std::endl;
    std::cout << "  - 释放吞吐量: " << free_throughput << " ops/s" << std::endl;
    std::cout << "  - 总吞吐量: " << total_throughput << " ops/s" << std::endl;

    std::cout << "\n单操作性能:" << std::endl;
    std::cout << "  - 平均单次分配时间: " << avg_alloc_time_ns / NUM_CHANNELS
              << " ns" << std::endl;
    std::cout << "  - 平均单次释放时间: " << avg_free_time_ns / NUM_CHANNELS
              << " ns" << std::endl;
    std::cout << "  - 平均单次操作时间: "
              << avg_iteration_time_ns / (2 * NUM_CHANNELS) << " ns"
              << std::endl;

    std::cout << "============================================\n" << std::endl;

    // 验证性能指标在合理范围内
    EXPECT_GT(alloc_throughput, 100000); // 分配吞吐量应大于10万操作/秒
    EXPECT_GT(free_throughput, 100000);  // 释放吞吐量应大于10万操作/秒
    EXPECT_LT(
        avg_alloc_time_ns / NUM_CHANNELS, 10000); // 单次分配时间应小于10微秒
    EXPECT_LT(
        avg_free_time_ns / NUM_CHANNELS, 10000); // 单次释放时间应小于10微秒
}

// 测试6：混合工作负载测试
TEST_F(ReactorChannelPoolTest, MixedWorkload)
{
    // 模拟真实使用场景：交替进行不同数量的分配和释放
    std::vector<reactor_channel *> active_channels;

    // 第一轮：分配10个channel
    for (int i = 0; i < 10; ++i) {
        reactor_channel *channel = pool_->alloc();
        EXPECT_NE(channel, nullptr);
        active_channels.push_back(channel);
    }

    // 第二轮：释放5个，分配3个
    for (int i = 0; i < 5; ++i) {
        pool_->free(active_channels.back());
        active_channels.pop_back();
    }
    for (int i = 0; i < 3; ++i) {
        reactor_channel *channel = pool_->alloc();
        EXPECT_NE(channel, nullptr);
        active_channels.push_back(channel);
    }

    // 验证当前活跃channel数量：10 - 5 + 3 = 8
    reactor_channel *current = pool_->first();
    int count = 0;
    while (current != nullptr) {
        ++count;
        current = current->next_;
    }
    EXPECT_EQ(count, 8);

    // 第三轮：释放所有
    for (reactor_channel *channel : active_channels) {
        pool_->free(channel);
    }
    EXPECT_EQ(pool_->first(), nullptr);
}

// 基础功能测试夹具
class ReactorBasicTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        registry_ = std::make_unique<service_registry>();
        thread_pool_ = registry_->use_service<thread_pool>();
        ASSERT_NE(thread_pool_, nullptr);
        scheduler_ = registry_->use_service<scheduler_service>(registry_.get());
        ASSERT_NE(scheduler_, nullptr);
        reactor_ = registry_->has_service<reactor_service>();
        ASSERT_NE(reactor_, nullptr);
    }

    void TearDown() override
    {
        registry_->shutdown_services();
        EXPECT_NE(reactor_->interrupt_fd_, -1);
        EXPECT_NE(reactor_->epoll_fd_, -1);

        EXPECT_EQ(reactor_->channel_pool_.first(), nullptr);
        registry_.reset();
    }

    void start_scheduler_threads(unsigned short n)
    {
        for (unsigned short i = 0; i < n; ++i) {
            thread_pool_->execute([this, i]() { scheduler_->run(i); });
        }
    }

    void stop_scheduler_threads()
    {
        scheduler_->stop();
        thread_pool_->join_all();
    }

    std::unique_ptr<service_registry> registry_;
    scheduler_service *scheduler_;
    reactor_service *reactor_;
    thread_pool *thread_pool_;
};

// 测试1：reactor_service构造函数和析构函数
TEST_F(ReactorBasicTest, ConstructorAndDestructor)
{
    EXPECT_NE(reactor_, nullptr);

    EXPECT_NE(reactor_->service_registry_, nullptr);
    EXPECT_NE(reactor_->scheduler_, nullptr);
    EXPECT_NE(reactor_->dummy_, nullptr);

    EXPECT_NE(reactor_->epoll_fd_, -1);
    EXPECT_NE(reactor_->interrupt_fd_, -1);

    // EXPECT_NE(reactor_->timer_service_, nullptr);
}

// 测试2：register_descriptor基础功能测试
TEST_F(ReactorBasicTest, RegisterDescriptorBasic)
{
    // 创建管道用于测试
    int pipe_fds[2];
    int result = pipe(pipe_fds);
    ASSERT_EQ(result, 0);

    reactor_channel *channel = nullptr;

    // 注册文件描述符
    error_info ret = reactor_->register_descriptor(pipe_fds[0], channel);
    EXPECT_EQ(ret, nullptr);
    EXPECT_NE(channel, nullptr);

    // 验证channel状态
    EXPECT_EQ(channel->reactor_, reactor_);
    EXPECT_EQ(channel->descriptor_, pipe_fds[0]);
    EXPECT_FALSE(channel->shutdown_);
    // 验证注册的事件
    EXPECT_NE(channel->registered_events_, 0);

    // 使用epoll_ctl检查是否全事件注册
    ::epoll_event ev;
    int epoll_result =
        ::epoll_ctl(reactor_->epoll_fd_, EPOLL_CTL_ADD, pipe_fds[0], &ev);
    // EPOLL_CTL_MOD会失败，但我们可以通过错误信息来验证描述符是否已注册
    EXPECT_EQ(epoll_result, -1); // 应该失败，因为描述符已注册
    EXPECT_EQ(errno, EEXIST);    // 错误码应该是EEXIST，表示描述符已存在

    // 调用deregister_descriptor
    reactor_->deregister_descriptor(pipe_fds[0], channel, false);

    // 第1次deregister
    EXPECT_NE(channel, nullptr); // deregister后channel应被置为nullptr
    EXPECT_EQ(channel->descriptor_, -1);
    EXPECT_TRUE(channel->shutdown_);

    epoll_result =
        ::epoll_ctl(reactor_->epoll_fd_, EPOLL_CTL_MOD, pipe_fds[0], &ev);
    EXPECT_EQ(epoll_result, -1); // 应该失败，因为描述符已注册
    EXPECT_EQ(errno, ENOENT);    // 错误码应该是ENOENT，表示未注册

    // 再次调用deregister_descriptor清理新channel
    reactor_->deregister_descriptor(pipe_fds[0], channel, false);
    EXPECT_EQ(channel, nullptr);

    // 清理文件描述符
    close(pipe_fds[0]);
    close(pipe_fds[1]);
}

TEST_F(ReactorBasicTest, RegisterInternalDescriptor)
{
    // Create a dummy file descriptor
    int dummy_fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_NE(dummy_fd, -1);

    // Create a mock reactor operation
    class MockReactorOp : public channel_task
    {
      public:
        MockReactorOp()
            : channel_task(&perform, do_complete)
        {}

        static status perform(channel_task *) { return channel_task::not_done; }
        static void do_complete(void *, async_task *, void *) {}
    } op;
    // Register the internal descriptor
    reactor_channel *channel = nullptr;
    error_info result =
        reactor_->register_internal_descriptor(read_op, dummy_fd, channel, &op);

    // Verify the registration was successful
    EXPECT_EQ(result, nullptr);
    ASSERT_NE(channel, nullptr);
    EXPECT_EQ(channel->descriptor_, dummy_fd);
    EXPECT_EQ(channel->reactor_, reactor_);
    EXPECT_FALSE(channel->shutdown_);

    // Verify the operation was enqueued
    EXPECT_FALSE(channel->op_queue_[read_op].empty());
    EXPECT_EQ(channel->op_queue_[read_op].head(), &op);

    // Clean up
    reactor_->deregister_internal_descriptor(dummy_fd, channel);
    ::close(dummy_fd);
}

TEST_F(ReactorBasicTest, DeregisterInternalDescriptor)
{
    // Create a dummy file descriptor
    int dummy_fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_NE(dummy_fd, -1);

    // Create a mock reactor operation
    class MockReactorOp : public channel_task
    {
      public:
        MockReactorOp()
            : channel_task(&perform, do_complete)
        {}

        static status perform(channel_task *) { return channel_task::not_done; }
        static void do_complete(void *, async_task *, void *) {}
    } op;

    // Register the internal descriptor
    reactor_channel *channel = nullptr;
    ASSERT_EQ(
        reactor_->register_internal_descriptor(read_op, dummy_fd, channel, &op),
        nullptr);
    ASSERT_NE(channel, nullptr);

    // Verify the operation was enqueued
    EXPECT_FALSE(channel->op_queue_[read_op].empty());
    EXPECT_EQ(channel->op_queue_[read_op].head(), &op);

    // Deregister the internal descriptor
    reactor_->deregister_internal_descriptor(dummy_fd, channel);

    // Verify the channel is marked as shutdown
    EXPECT_TRUE(channel->shutdown_);
    EXPECT_EQ(channel->descriptor_, -1);

    // Verify the operation queue is empty
    EXPECT_TRUE(channel->op_queue_[read_op].empty());

    // Clean up
    ::close(dummy_fd);
}

TEST_F(ReactorBasicTest, CleanupChannel)
{
    // Allocate a channel
    reactor_channel *channel = reactor_->allocate_channel();
    ASSERT_NE(channel, nullptr);

    // Call cleanup_channel
    reactor_->cleanup_channel(channel);

    // Verify the channel is cleaned up
    EXPECT_EQ(channel, nullptr);
}

TEST_F(ReactorBasicTest, FreeChannel)
{
    // uring的event分配了一个channel
    reactor_channel *uring = reactor_->channel_pool_.live_list_;

    // Allocate a channel
    reactor_channel *channel = reactor_->allocate_channel();
    ASSERT_NE(channel, nullptr);

    // Call free_channel
    reactor_->free_channel(channel);

    // Verify the channel is returned to the pool
    EXPECT_EQ(reactor_->channel_pool_.live_list_, uring);
}

TEST_F(ReactorBasicTest, AsyncRequest)
{
    // Create a dummy file descriptor
    int dummy_fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_NE(dummy_fd, -1);

    // Register the descriptor
    reactor_channel *channel = nullptr;
    ASSERT_EQ(reactor_->register_descriptor(dummy_fd, channel), nullptr);
    ASSERT_NE(channel, nullptr);

    // Create a mock reactor operation
    bool called = false;
    class MockReactorOp : public channel_task
    {
      public:
        bool *called;
        MockReactorOp(bool *p)
            : channel_task(&perform, do_complete)
            , called(p)
        {}

        static status perform(channel_task *) { return channel_task::not_done; }

        static void do_complete(void *, async_task *, void *) {}
    } op(&called);

    // Call async_request
    reactor_->async_request(read_op, dummy_fd, channel, &op);

    // Verify the operation was enqueued
    EXPECT_FALSE(channel->op_queue_[read_op].empty());
    EXPECT_EQ(channel->op_queue_[read_op].head(), &op);

    // Verify the immediate callback was not called
    EXPECT_FALSE(called);

    // 将channel中的op掏出来
    task_queue ops;
    ops.merge(channel->op_queue_[read_op]);
    EXPECT_FALSE(ops.empty());

    // Clean up
    reactor_->deregister_descriptor(dummy_fd, channel, true);
    ::close(dummy_fd);
}

} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}