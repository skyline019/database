////
// @file test_strand.cc
// @brief
// 测试strand
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include "service.h"
#include "thread_pool.h"
#include "scheduler.h"
#include "timer.h"
#include "reactor.h"
#include "strand.h"

namespace wf::io {

// 测试夹具，用于设置system_timer_service环境
class StrandServiceTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // 创建service_portal实例
        registry_ = std::make_unique<service_registry>();

        // 预加载服务
        thread_pool_ = registry_->use_service<thread_pool>();
        ASSERT_NE(thread_pool_, nullptr);
        scheduler_service *sc =
            registry_->use_service<scheduler_service>(registry_.get());
        ASSERT_NE(sc, nullptr);
        scheduler_ = sc;
        strand_service *st = registry_->use_service<strand_service>();
        ASSERT_NE(st, nullptr);
        strand_ = st;
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
    thread_pool *thread_pool_;
    strand_service *strand_;
};

TEST_F(StrandServiceTest, GetRuntimeExecutor)
{
    start_threads(4);

    strand_executor strand(strand_);
    async_task op1([](void *, async_task *, void *) {});
    async_task op2([](void *, async_task *, void *) {});
    strand.execute(op1);
    strand.execute(op2);

    scheduler_->stop();
    join_all();
}

// 测试strand的线程安全性和串行化特性
TEST_F(StrandServiceTest, StrandThreadSafety)
{
    start_threads(4);

    std::atomic<int> execution_order{0};
    std::atomic<int> task_count{0};
    std::atomic<bool> test_completed{false};
    const int num_tasks = 10;

    strand_executor strand(strand_);

    // 创建用于测试的任务类
    class StrandSafetyTask : public async_task
    {
      private:
        scheduler_service *scheduler_;
        std::atomic<int> &execution_order_;
        std::atomic<int> &task_count_;
        std::atomic<bool> &test_completed_;
        int expected_order_;

      public:
        StrandSafetyTask(
            scheduler_service *scheduler,
            std::atomic<int> &execution_order,
            std::atomic<int> &task_count,
            std::atomic<bool> &test_completed,
            int expected_order)
            : async_task(&do_complete)
            , scheduler_(scheduler)
            , execution_order_(execution_order)
            , task_count_(task_count)
            , test_completed_(test_completed)
            , expected_order_(expected_order)
        {}

      private:
        static void do_complete(void *owner, async_task *op, void *)
        {
            StrandSafetyTask *self = static_cast<StrandSafetyTask *>(op);
            if (owner) {
                // 验证执行顺序
                int current_order = self->execution_order_.fetch_add(
                    1, std::memory_order_relaxed);
                EXPECT_EQ(current_order, self->expected_order_);

                // 统计完成的任务数
                int completed =
                    self->task_count_.fetch_add(1, std::memory_order_relaxed) +
                    1;

                if (completed == num_tasks) {
                    self->test_completed_.store(
                        true, std::memory_order_relaxed);
                    self->scheduler_->stop();
                }
                delete self;
            }
        }
    };

    // 并发提交多个任务到同一个strand
    for (int i = 0; i < num_tasks; ++i) {
        StrandSafetyTask *task = new StrandSafetyTask(
            scheduler_, execution_order, task_count, test_completed, i);
        strand.execute(*task);
    }

    join_all();

    // 验证所有任务都按顺序执行完成
    EXPECT_TRUE(test_completed.load());
    EXPECT_EQ(execution_order.load(), num_tasks);
    EXPECT_EQ(task_count.load(), num_tasks);
}

// 测试strand的严格串行化特性
TEST_F(StrandServiceTest, SerializationVerification)
{
    start_threads(4);

    std::atomic<int> current_executing_task{-1};     // 当前正在执行的任务ID
    std::atomic<int> completed_tasks{0};             // 已完成的任务数
    std::atomic<bool> serialization_violated{false}; // 串行化是否被违反
    const int num_tasks = 20; // 增加任务数量以更好地测试并发性

    strand_executor strand(strand_);

    // 创建用于测试串行化的任务类
    class SerializationTask : public async_task
    {
      private:
        scheduler_service *scheduler_;
        std::atomic<int> &current_executing_task_;
        std::atomic<int> &completed_tasks_;
        std::atomic<bool> &serialization_violated_;
        int task_id_;

      public:
        SerializationTask(
            scheduler_service *scheduler,
            std::atomic<int> &current_executing_task,
            std::atomic<int> &completed_tasks,
            std::atomic<bool> &serialization_violated,
            int task_id)
            : async_task(&do_complete)
            , scheduler_(scheduler)
            , current_executing_task_(current_executing_task)
            , completed_tasks_(completed_tasks)
            , serialization_violated_(serialization_violated)
            , task_id_(task_id)
        {}

      private:
        static void do_complete(void *owner, async_task *op, void *)
        {
            SerializationTask *self = static_cast<SerializationTask *>(op);
            if (owner) {
                // 检查当前是否有其他任务正在执行
                int current_executing = self->current_executing_task_.load(
                    std::memory_order_acquire);

                // 如果当前有任务正在执行，说明串行化被违反
                if (current_executing != -1) {
                    self->serialization_violated_.store(
                        true, std::memory_order_relaxed);
                }

                // 设置当前正在执行的任务ID
                self->current_executing_task_.store(
                    self->task_id_, std::memory_order_release);

                // 模拟任务执行时间，增加并发检测的机会
                std::this_thread::sleep_for(std::chrono::microseconds(100));

                // 完成任务执行
                self->current_executing_task_.store(
                    -1, std::memory_order_release);

                // 统计完成的任务数
                int completed = self->completed_tasks_.fetch_add(
                                    1, std::memory_order_relaxed) +
                                1;

                if (completed == num_tasks) self->scheduler_->stop();
                delete self;
            }
        }
    };

    // 并发提交多个任务到同一个strand
    for (int i = 0; i < num_tasks; ++i) {
        SerializationTask *task = new SerializationTask(
            scheduler_,
            current_executing_task,
            completed_tasks,
            serialization_violated,
            i);
        strand.execute(*task);
    }

    join_all();

    // 验证串行化特性
    EXPECT_FALSE(serialization_violated.load())
        << "串行化被违反：检测到多个任务同时执行";
    EXPECT_EQ(completed_tasks.load(), num_tasks) << "任务完成数量不正确";
    EXPECT_EQ(current_executing_task.load(), -1) << "任务执行状态未正确重置";
}

static std::atomic<int> g_recursion_counter = 0;
static constexpr int kMaxRecursionDepth = 1000000; // 测试递归深度
TEST_F(StrandServiceTest, NoStackOverflowIfTailCallOptimized)
{
    strand_executor strand(strand_);

    // 自定义 operation，递归调用自身
    struct RecursiveOp : async_task
    {
        strand_executor &strand;
        explicit RecursiveOp(strand_executor &s)
            : async_task(do_complete)
            , strand(s)
        {}
        static void do_complete(void *owner, async_task *op, void *)
        {
            RecursiveOp *self = static_cast<RecursiveOp *>(op);
            if (owner) {
                if (g_recursion_counter++ < kMaxRecursionDepth) {
                    self->strand.execute(*self); // 递归提交自身
                }
            }
        }
    };

    RecursiveOp op(strand);
    strand.execute(op); // 启动递归调用

    // 如果没有堆栈溢出，递归深度应达到最大值
    EXPECT_EQ(g_recursion_counter, kMaxRecursionDepth + 1);
}

class MockOperationQueue
{
  public:
    bool empty() const { return empty_; }
    void set_empty(bool val) { empty_ = val; }

  private:
    bool empty_ = true;
};
class MockStrandImpl
{
    // Empty mock implementation
};
class MockStrandService
{
  public:
    static bool enqueue(void *impl, MockOperationQueue &ops)
    {
        return !ops.empty();
    }

    static void execute(void *impl) { execute_called = true; }
    static bool execute_called;
};
bool MockStrandService::execute_called = false;

TEST_F(StrandServiceTest, ExecuteWithEmptyQueue)
{
    MockOperationQueue ops;
    ops.set_empty(true);
    MockStrandImpl impl;

    auto execute_wrapper = [](MockStrandImpl *impl, MockOperationQueue &ops) {
        if (MockStrandService::enqueue(impl, ops))
            MockStrandService::execute(impl);
    };

    execute_wrapper(&impl, ops);

    EXPECT_FALSE(MockStrandService::execute_called);
}

TEST_F(StrandServiceTest, ExecuteWithNonEmptyQueue)
{
    MockOperationQueue ops;
    ops.set_empty(false);
    MockStrandImpl impl;

    auto execute_wrapper = [](MockStrandImpl *impl, MockOperationQueue &ops) {
        if (MockStrandService::enqueue(impl, ops))
            MockStrandService::execute(impl);
    };

    execute_wrapper(&impl, ops);

    EXPECT_TRUE(MockStrandService::execute_called);
}

TEST_F(StrandServiceTest, ExecuteWithNullImpl)
{
    MockOperationQueue ops;
    ops.set_empty(false);

    auto execute_wrapper = [](MockStrandImpl *impl, MockOperationQueue &ops) {
        if (MockStrandService::enqueue(impl, ops))
            MockStrandService::execute(impl);
    };

    execute_wrapper(nullptr, ops);

    EXPECT_TRUE(MockStrandService::execute_called);
}

} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}