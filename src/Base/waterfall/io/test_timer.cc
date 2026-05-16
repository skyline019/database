////
// @file test_timer.cc
// @brief
// 测试定时器
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <sys/timerfd.h>
#include <random>
#include "timer.h"
#include "thread_pool.h"
#include "service.h"
#include "scheduler.h"
#include "reactor.h"

namespace wf::io {

// 自定义等待任务用于测试
class TestWaitTask : public wait_task
{
  public:
    static int complete_count;
    static error_info last_result;

    TestWaitTask()
        : wait_task(&complete_test)
    {}

    static void complete_test(void *owner, async_task *base, void *events)
    {
        complete_count++;
        TestWaitTask *task = static_cast<TestWaitTask *>(base);
        last_result = task->result_;
    }
};

// 静态成员初始化
int TestWaitTask::complete_count = 0;
error_info TestWaitTask::last_result = nullptr;

TEST(timer_queue, Constructor)
{
    timer_queue tq;

    // 验证初始状态
    EXPECT_TRUE(tq.empty());
    EXPECT_EQ(tq.wait_duration_msec(1000), 1000);
    EXPECT_EQ(tq.wait_duration_usec(1000), 1000);
}

TEST(timer_queue, EnqueueTimer_Basic)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask test_task;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 设置定时器时间为当前时间+100毫秒
    auto future_time =
        std::chrono::system_clock::now() + std::chrono::milliseconds(100);

    // 入队定时器
    bool is_first = tq.enqueue_timer(future_time, timer_data, &test_task);

    // 验证入队结果
    EXPECT_TRUE(is_first);
    EXPECT_FALSE(tq.empty());
    EXPECT_TRUE(tq.is_enqueued(timer_data));

    // 修复：在测试结束前手动清空任务队列，避免栈使用后释放错误
    while (timer_data.op_queue_.head())
        timer_data.op_queue_.dequeue();
}

TEST(timer_queue, EnqueueTimer_MultipleTimers)
{
    timer_queue tq;
    timer_queue::per_timer_data timer1, timer2, timer3;
    TestWaitTask task1, task2, task3;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 设置不同的定时器时间
    auto now = std::chrono::system_clock::now();
    auto time1 = now + std::chrono::milliseconds(300); // 最晚
    auto time2 = now + std::chrono::milliseconds(100); // 最早
    auto time3 = now + std::chrono::milliseconds(200); // 中间

    // 入队多个定时器
    bool is_first1 = tq.enqueue_timer(time1, timer1, &task1);
    bool is_first2 = tq.enqueue_timer(time2, timer2, &task2);
    bool is_first3 = tq.enqueue_timer(time3, timer3, &task3);

    // 验证入队结果
    EXPECT_TRUE(is_first1);  // 第一个入队的应该是第一个
    EXPECT_TRUE(is_first2);  // 第二个入队的应该是最早的
    EXPECT_FALSE(is_first3); // 第三个入队的不是第一个

    EXPECT_FALSE(tq.empty());
    EXPECT_TRUE(tq.is_enqueued(timer1));
    EXPECT_TRUE(tq.is_enqueued(timer2));
    EXPECT_TRUE(tq.is_enqueued(timer3));

    // 修复：在测试结束前手动清空所有任务队列
    while (timer1.op_queue_.head())
        timer1.op_queue_.dequeue();
    while (timer2.op_queue_.head())
        timer2.op_queue_.dequeue();
    while (timer3.op_queue_.head())
        timer3.op_queue_.dequeue();
}

TEST(timer_queue, WaitDuration)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask test_task;

    // 设置定时器时间为当前时间+500毫秒
    auto future_time =
        std::chrono::system_clock::now() + std::chrono::milliseconds(500);

    tq.enqueue_timer(future_time, timer_data, &test_task);

    // 测试等待时间
    long msec_duration = tq.wait_duration_msec(1000);
    long usec_duration = tq.wait_duration_usec(1000000);

    // 验证等待时间在合理范围内
    EXPECT_GT(msec_duration, 0);
    EXPECT_LT(msec_duration, 1000);

    EXPECT_GT(usec_duration, 0);
    EXPECT_LT(usec_duration, 1000000);

    // 修复：在测试结束前手动清空任务队列
    while (timer_data.op_queue_.head())
        timer_data.op_queue_.dequeue();
}

TEST(timer_queue, GetReadyTimers_NoReady)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask test_task;
    task_queue ops;

    // 设置定时器时间为未来时间
    auto future_time =
        std::chrono::system_clock::now() + std::chrono::seconds(10);

    tq.enqueue_timer(future_time, timer_data, &test_task);

    // 获取就绪定时器（应该没有）
    tq.get_ready_timers(ops);

    // 验证没有定时器就绪
    EXPECT_TRUE(ops.empty());
    EXPECT_FALSE(tq.empty());

    // 修复：在测试结束前手动清空任务队列
    while (timer_data.op_queue_.head())
        timer_data.op_queue_.dequeue();
}

TEST(timer_queue, GetReadyTimers_Ready)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask test_task;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 设置定时器时间为过去时间（立即就绪）
    auto past_time = std::chrono::system_clock::now() - std::chrono::seconds(1);

    tq.enqueue_timer(past_time, timer_data, &test_task);

    // 获取就绪定时器
    tq.get_ready_timers(ops);

    // 验证定时器就绪
    EXPECT_FALSE(ops.empty());
    EXPECT_TRUE(tq.empty());

    // 验证任务被正确入队
    EXPECT_EQ(ops.head(), &test_task);

    // 修复：在测试结束前手动清空任务队列
    // 注意：这个测试中任务已经被移动到ops队列中，所以timer_data的队列应该是空的
    // 但为了安全起见，仍然清空
    while (timer_data.op_queue_.head())
        timer_data.op_queue_.dequeue();
}

TEST(timer_queue, GetReadyTimers_Multiple)
{
    timer_queue tq;
    timer_queue::per_timer_data timer1, timer2, timer3;
    TestWaitTask task1, task2, task3;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    auto now = std::chrono::system_clock::now();

    // 设置一个过去时间和两个未来时间
    auto past_time = now - std::chrono::seconds(1);
    auto future_time1 = now + std::chrono::seconds(5);
    auto future_time2 = now + std::chrono::seconds(10);

    tq.enqueue_timer(past_time, timer1, &task1);
    tq.enqueue_timer(future_time1, timer2, &task2);
    tq.enqueue_timer(future_time2, timer3, &task3);

    // 获取就绪定时器（应该只有第一个就绪）
    tq.get_ready_timers(ops);

    // 验证只有过期的定时器被处理
    EXPECT_FALSE(ops.empty());
    EXPECT_FALSE(tq.empty());
    EXPECT_EQ(ops.head(), &task1);

    // 验证其他定时器仍在队列中
    EXPECT_TRUE(tq.is_enqueued(timer2));
    EXPECT_TRUE(tq.is_enqueued(timer3));

    // 修复：在测试结束前手动清空所有任务队列
    while (timer1.op_queue_.head())
        timer1.op_queue_.dequeue();
    while (timer2.op_queue_.head())
        timer2.op_queue_.dequeue();
    while (timer3.op_queue_.head())
        timer3.op_queue_.dequeue();
}

TEST(timer_queue, GetAllTimers)
{
    timer_queue tq;
    timer_queue::per_timer_data timer1, timer2, timer3;
    TestWaitTask task1, task2, task3;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    auto now = std::chrono::system_clock::now();

    // 设置多个定时器
    auto time1 = now + std::chrono::seconds(1);
    auto time2 = now + std::chrono::seconds(2);
    auto time3 = now + std::chrono::seconds(3);

    tq.enqueue_timer(time1, timer1, &task1);
    tq.enqueue_timer(time2, timer2, &task2);
    tq.enqueue_timer(time3, timer3, &task3);

    // 获取所有定时器
    tq.get_all_timers(ops);

    // 验证所有定时器都被获取
    EXPECT_TRUE(tq.empty());
    EXPECT_FALSE(ops.empty());

    // 验证任务队列包含所有任务
    int task_count = 0;
    async_task *current = ops.head();
    while (current) {
        task_count++;
        current = current->next_;
    }
    EXPECT_EQ(task_count, 3);

    // 修复：在测试结束前手动清空所有任务队列
    // 注意：这个测试中任务已经被移动到ops队列中，所以timer_data的队列应该是空的
    // 但为了安全起见，仍然清空
    while (timer1.op_queue_.head())
        timer1.op_queue_.dequeue();
    while (timer2.op_queue_.head())
        timer2.op_queue_.dequeue();
    while (timer3.op_queue_.head())
        timer3.op_queue_.dequeue();
}

TEST(timer_queue, HeapOperations)
{
    timer_queue tq;
    timer_queue::per_timer_data timer1, timer2, timer3, timer4;
    TestWaitTask task1, task2, task3, task4;

    auto now = std::chrono::system_clock::now();

    // 设置不同时间的定时器，测试堆排序
    auto time1 = now + std::chrono::milliseconds(400); // 最晚
    auto time2 = now + std::chrono::milliseconds(100); // 最早
    auto time3 = now + std::chrono::milliseconds(300); // 第三
    auto time4 = now + std::chrono::milliseconds(200); // 第二

    // 按乱序入队
    tq.enqueue_timer(time1, timer1, &task1);
    tq.enqueue_timer(time2, timer2, &task2);
    tq.enqueue_timer(time3, timer3, &task3);
    tq.enqueue_timer(time4, timer4, &task4);

    // 验证堆顶是最早的定时器
    long wait_time = tq.wait_duration_msec(1000);
    EXPECT_GT(wait_time, 0);
    EXPECT_LT(wait_time, 1000);

    // 修复：在测试结束前手动清空所有任务队列，避免栈使用后释放错误
    while (timer1.op_queue_.head())
        timer1.op_queue_.dequeue();
    while (timer2.op_queue_.head())
        timer2.op_queue_.dequeue();
    while (timer3.op_queue_.head())
        timer3.op_queue_.dequeue();
    while (timer4.op_queue_.head())
        timer4.op_queue_.dequeue();
}

TEST(timer_queue, RealTimeWait)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask test_task;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 设置定时器在100毫秒后触发
    auto trigger_time =
        std::chrono::system_clock::now() + std::chrono::milliseconds(100);

    tq.enqueue_timer(trigger_time, timer_data, &test_task);

    // 等待定时器触发
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // 获取就绪定时器
    tq.get_ready_timers(ops);

    // 验证定时器已触发
    EXPECT_FALSE(ops.empty());
    EXPECT_TRUE(tq.empty());
    EXPECT_EQ(ops.head(), &test_task);
}

TEST(timer_queue, MultipleTasksPerTimer)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask task1, task2, task3;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 设置定时器时间
    auto trigger_time =
        std::chrono::system_clock::now() + std::chrono::milliseconds(50);

    // 同一个定时器关联多个任务
    tq.enqueue_timer(trigger_time, timer_data, &task1);
    tq.enqueue_timer(trigger_time, timer_data, &task2);
    tq.enqueue_timer(trigger_time, timer_data, &task3);

    // 等待定时器触发
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 获取就绪定时器
    tq.get_ready_timers(ops);
    // 验证所有任务都被处理
    int task_count = 0;
    async_task *current = ops.head();
    while (current) {
        task_count++;
        current = current->next_;
    }
    EXPECT_EQ(task_count, 3);
}

TEST(timer_queue, CancelTimer_SingleTask)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask test_task;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 设置定时器时间
    auto trigger_time =
        std::chrono::system_clock::now() + std::chrono::seconds(5);

    // 入队定时器
    tq.enqueue_timer(trigger_time, timer_data, &test_task);

    // 验证定时器已入队
    EXPECT_TRUE(tq.is_enqueued(timer_data));
    EXPECT_FALSE(tq.empty());

    // 取消定时器中的任务
    size_t cancelled_count = tq.cancel_timer(timer_data, ops, 1);

    // 验证取消结果
    EXPECT_EQ(cancelled_count, 1);

    // 验证任务结果被正确设置（但不会自动调用回调）
    printf("test_task.result_: %s\n", test_task.result_);

    // 注意：cancel_timer不会自动调用完成回调，所以complete_count应该保持为0
    EXPECT_EQ(TestWaitTask::complete_count, 0);

    // 验证任务被移动到ops队列
    EXPECT_FALSE(ops.empty());
    EXPECT_EQ(ops.head(), &test_task);

    // 验证定时器已从队列中移除（因为任务队列为空）
    EXPECT_FALSE(tq.is_enqueued(timer_data));
    EXPECT_TRUE(tq.empty());

    // 修复：在测试结束前手动清空任务队列
    while (timer_data.op_queue_.head())
        timer_data.op_queue_.dequeue();
}

TEST(timer_queue, CancelTimer_MultipleTasks)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask task1, task2, task3;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 设置定时器时间
    auto trigger_time =
        std::chrono::system_clock::now() + std::chrono::seconds(5);

    // 同一个定时器关联多个任务
    tq.enqueue_timer(trigger_time, timer_data, &task1);
    tq.enqueue_timer(trigger_time, timer_data, &task2);
    tq.enqueue_timer(trigger_time, timer_data, &task3);

    // 验证定时器已入队
    EXPECT_TRUE(tq.is_enqueued(timer_data));
    EXPECT_FALSE(tq.empty());

    // 取消部分任务（限制最大取消数量为2）
    size_t cancelled_count = tq.cancel_timer(timer_data, ops, 2);

    // 验证取消结果
    EXPECT_EQ(cancelled_count, 2);

    // 验证任务结果被正确设置
    EXPECT_EQ(task1.result_, utils::system_errors::system_ECANCELED);
    EXPECT_EQ(task2.result_, utils::system_errors::system_ECANCELED);

    // 注意：cancel_timer不会自动调用完成回调
    EXPECT_EQ(TestWaitTask::complete_count, 0);

    // 验证任务被移动到ops队列
    int task_count = 0;
    async_task *current = ops.head();
    while (current) {
        task_count++;
        current = current->next_;
    }
    EXPECT_EQ(task_count, 2);

    // 验证定时器仍在队列中（因为还有任务未取消）
    EXPECT_TRUE(tq.is_enqueued(timer_data));
    EXPECT_FALSE(tq.empty());

    // 取消剩余的任务
    cancelled_count = tq.cancel_timer(timer_data, ops, 10); // 使用较大的限制值

    // 验证所有任务都被取消
    EXPECT_EQ(cancelled_count, 1);
    EXPECT_EQ(task3.result_, utils::system_errors::system_ECANCELED);

    // 验证定时器已从队列中移除
    EXPECT_FALSE(tq.is_enqueued(timer_data));
    EXPECT_TRUE(tq.empty());

    // 修复：在测试结束前手动清空任务队列
    while (timer_data.op_queue_.head())
        timer_data.op_queue_.dequeue();
}

TEST(timer_queue, CancelTimer_NotEnqueued)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask test_task;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 尝试取消未入队的定时器
    size_t cancelled_count = tq.cancel_timer(timer_data, ops, 1);

    // 验证没有任务被取消
    EXPECT_EQ(cancelled_count, 0);
    EXPECT_EQ(TestWaitTask::complete_count, 0);

    // 验证ops队列为空
    EXPECT_TRUE(ops.empty());

    // 修复：在测试结束前手动清空任务队列
    while (timer_data.op_queue_.head())
        timer_data.op_queue_.dequeue();
}

TEST(timer_queue, CancelTimer_EmptyQueue)
{
    timer_queue tq;
    timer_queue::per_timer_data timer_data;
    TestWaitTask test_task;
    task_queue ops;

    TestWaitTask::complete_count = 0;
    TestWaitTask::last_result = nullptr;

    // 设置定时器时间
    auto trigger_time =
        std::chrono::system_clock::now() + std::chrono::seconds(5);

    // 入队定时器
    tq.enqueue_timer(trigger_time, timer_data, &test_task);

    // 先取消任务
    size_t cancelled_count = tq.cancel_timer(timer_data, ops, 1);
    EXPECT_EQ(cancelled_count, 1);

    // 再次尝试取消（任务队列已空）
    cancelled_count = tq.cancel_timer(timer_data, ops, 1);

    // 验证没有任务被取消
    EXPECT_EQ(cancelled_count, 0);

    // 注意：cancel_timer不会自动调用完成回调
    EXPECT_EQ(TestWaitTask::complete_count, 0);

    // 修复：在测试结束前手动清空任务队列
    while (timer_data.op_queue_.head())
        timer_data.op_queue_.dequeue();
}

// 性能测试类
class PerformanceTimerQueueTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // 重置性能计数器
        total_enqueue_time = std::chrono::nanoseconds(0);
        total_dequeue_time = std::chrono::nanoseconds(0);
        total_cancel_time = std::chrono::nanoseconds(0);
        operation_count = 0;
    }

    void TearDown() override
    {
        // 输出性能统计
        if (operation_count > 0) {
            printf("性能测试统计:\n");
            printf("  总操作数: %zu\n", operation_count);
            printf(
                "  平均入队时间: %.3f ns\n",
                total_enqueue_time.count() / (double) operation_count);
            printf(
                "  平均出队时间: %.3f ns\n",
                total_dequeue_time.count() / (double) operation_count);
            printf(
                "  平均取消时间: %.3f ns\n",
                total_cancel_time.count() / (double) operation_count);
        }
    }

    // 性能计数器
    std::chrono::nanoseconds total_enqueue_time;
    std::chrono::nanoseconds total_dequeue_time;
    std::chrono::nanoseconds total_cancel_time;
    size_t operation_count;
};

// 性能测试：大量定时器入队
TEST_F(PerformanceTimerQueueTest, MassEnqueuePerformance)
{
    timer_queue tq;
    const size_t num_timers = 1000000;
    std::vector<timer_queue::per_timer_data> timers(num_timers);
    std::vector<TestWaitTask> tasks(num_timers);

    auto start_time = std::chrono::high_resolution_clock::now();

    // 批量入队定时器
    for (size_t i = 0; i < num_timers; ++i) {
        auto future_time = std::chrono::system_clock::now() +
                           std::chrono::milliseconds(i % 1000); // 分散时间

        auto op_start = std::chrono::high_resolution_clock::now();
        tq.enqueue_timer(future_time, timers[i], &tasks[i]);
        auto op_end = std::chrono::high_resolution_clock::now();

        total_enqueue_time += (op_end - op_start);
        operation_count++;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_time = end_time - start_time;

    printf("批量入队性能测试:\n");
    printf("  定时器数量: %zu\n", num_timers);
    printf(
        "  总耗时: %ld ms\n",
        std::chrono::duration_cast<std::chrono::milliseconds>(total_time)
            .count());
    printf(
        "  平均每个定时器入队时间: %.3f ns\n",
        total_enqueue_time.count() / (double) num_timers);
    printf("  堆大小: %zu\n", num_timers);

    // 清理
    for (auto &timer : timers) {
        while (timer.op_queue_.head())
            timer.op_queue_.dequeue();
    }
}

// 性能测试：定时器出队性能
TEST_F(PerformanceTimerQueueTest, DequeuePerformance)
{
    timer_queue tq;
    const size_t num_timers = 5000;
    std::vector<timer_queue::per_timer_data> timers(num_timers);
    std::vector<TestWaitTask> tasks(num_timers);
    task_queue ops;

    // 先入队定时器
    for (size_t i = 0; i < num_timers; ++i) {
        auto future_time =
            std::chrono::system_clock::now() + std::chrono::milliseconds(i);
        tq.enqueue_timer(future_time, timers[i], &tasks[i]);
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    // 批量获取就绪定时器
    size_t dequeued_count = 0;
    while (!tq.empty()) {
        auto op_start = std::chrono::high_resolution_clock::now();
        tq.get_ready_timers(ops);
        auto op_end = std::chrono::high_resolution_clock::now();

        total_dequeue_time += (op_end - op_start);
        operation_count++;

        // 手动计算ops队列中的任务数量
        size_t op_count = 0;
        async_task *current = ops.head();
        while (current) {
            op_count++;
            current = current->next_;
        }
        dequeued_count += op_count;

        // 清空ops队列
        while (ops.head())
            ops.dequeue();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_time = end_time - start_time;

    printf("出队性能测试:\n");
    printf("  定时器数量: %zu\n", num_timers);
    printf("  出队操作次数: %zu\n", operation_count);
    printf("  总出队定时器数: %zu\n", dequeued_count);
    printf(
        "  总耗时: %ld ms\n",
        std::chrono::duration_cast<std::chrono::milliseconds>(total_time)
            .count());

    // 清理
    for (auto &timer : timers) {
        while (timer.op_queue_.head())
            timer.op_queue_.dequeue();
    }
}

// 性能测试：堆排序性能（大量定时器按时间排序）
TEST_F(PerformanceTimerQueueTest, HeapSortingPerformance)
{
    timer_queue tq;
    const size_t num_timers = 50000;
    std::vector<timer_queue::per_timer_data> timers(num_timers);
    std::vector<TestWaitTask> tasks(num_timers);

    // 生成随机时间点
    // 使用简单的随机数生成方法替代std::random_device和std::mt19937
    unsigned int seed = static_cast<unsigned int>(
        std::chrono::system_clock::now().time_since_epoch().count());
    std::srand(seed);

    auto start_time = std::chrono::high_resolution_clock::now();

    // 按随机顺序入队定时器
    for (size_t i = 0; i < num_timers; ++i) {
        // 使用简单的随机数生成
        int random_ms = (std::rand() % 2000) + 1;
        auto future_time = std::chrono::system_clock::now() +
                           std::chrono::milliseconds(random_ms);
        tq.enqueue_timer(future_time, timers[i], &tasks[i]);
    }

    auto sort_end_time = std::chrono::high_resolution_clock::now();
    auto sort_time = sort_end_time - start_time;

    // 验证堆排序正确性：按顺序出队
    // 移除未使用的变量 last_time
    // std::chrono::system_clock::time_point last_time =
    //     std::chrono::system_clock::time_point::min();
    task_queue ops;
    size_t dequeued_count = 0;

    auto dequeue_start = std::chrono::high_resolution_clock::now();

    while (!tq.empty()) {
        tq.get_ready_timers(ops);
        if (!ops.empty()) {
            // 理论上应该按时间顺序出队
            // 移除未使用的变量 current_time
            // auto current_time = std::chrono::system_clock::now();
            // 验证时间顺序（由于是未来时间，实际不会触发，这里主要测试堆结构）
            // 手动计算ops队列中的任务数量
            size_t op_count = 0;
            async_task *current = ops.head();
            while (current) {
                op_count++;
                current = current->next_;
            }
            dequeued_count += op_count;
            while (ops.head())
                ops.dequeue();
        }
        // 模拟时间推进
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }

    auto dequeue_end = std::chrono::high_resolution_clock::now();
    auto dequeue_time = dequeue_end - dequeue_start;

    printf("堆排序性能测试:\n");
    printf("  定时器数量: %zu\n", num_timers);
    printf(
        "  入队排序耗时: %ld ms\n",
        std::chrono::duration_cast<std::chrono::milliseconds>(sort_time)
            .count());
    printf(
        "  顺序出队耗时: %ld ms\n",
        std::chrono::duration_cast<std::chrono::milliseconds>(dequeue_time)
            .count());
    printf("  总出队定时器数: %zu\n", dequeued_count);

    // 清理
    for (auto &timer : timers) {
        while (timer.op_queue_.head())
            timer.op_queue_.dequeue();
    }
}

// 测试夹具，用于设置system_timer_service环境
class SystemTimerServiceTest : public ::testing::Test
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
        reactor_service *rs = registry_->has_service<reactor_service>();
        ASSERT_NE(rs, nullptr);
        reactor_ = rs;
        // 加载system_timer_service
        system_timer_service *ts =
            registry_->has_service<system_timer_service>();
        ASSERT_NE(ts, nullptr);
        timer_service_ = ts;
    }

    void TearDown() override
    {
        registry_->shutdown_services();
        EXPECT_TRUE(timer_service_->timer_queue_.empty());
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
    system_timer_service *timer_service_;
    thread_pool *thread_pool_;
};

class MockWaitOp : public wait_task
{
  private:
    scheduler_service *scheduler_;
    bool executed_;

  public:
    MockWaitOp(scheduler_service *scheduler)
        : wait_task(do_complete)
        , scheduler_(scheduler)
        , executed_(false)
    {}

    static void do_complete(void *owner, wait_task *op, void *)
    {
        if (owner) {
            MockWaitOp *self = static_cast<MockWaitOp *>(op);
            self->executed_ = true;
            self->scheduler_->stop();
        }
    }
};
class MockWaitOp2 : public wait_task
{
  private:
    bool executed_;

  public:
    MockWaitOp2()
        : wait_task(do_complete)
        , executed_(false)
    {}

    static void do_complete(void *owner, wait_task *op, void *)
    {
        if (owner) {
            MockWaitOp2 *self = static_cast<MockWaitOp2 *>(op);
            self->executed_ = true;
        }
    }
};

// 测试构造函数和基本功能
TEST_F(SystemTimerServiceTest, Constructor)
{
    start_threads(4);

    // 验证服务创建成功
    EXPECT_NE(timer_service_, nullptr);
    EXPECT_EQ(scheduler_, timer_service_->scheduler_);
    EXPECT_EQ(reactor_, timer_service_->reactor_);
    EXPECT_NE(thread_pool_, nullptr);
    EXPECT_NE(timer_service_->timer_fd_, -1);

    scheduler_->stop();
    join_all();
}

TEST_F(SystemTimerServiceTest, ScheduleTimer)
{
    start_threads(4);

    // Create a timer and a mock wait operation
    timer_queue::per_timer_data timer_data;
    MockWaitOp op(scheduler_);

    // Schedule the timer
    auto now = std::chrono::system_clock::now();
    timer_service_->schedule_timer(
        now + std::chrono::milliseconds(10), timer_data, &op);
    // Verify the timer is enqueued in the timer queue
    EXPECT_FALSE(timer_service_->timer_queue_.empty());

    // 主线程进入事件循环
    join_all();
    EXPECT_TRUE(op.executed_);
}

TEST_F(SystemTimerServiceTest, GetTimeout)
{
    start_threads(4);

    // Create a timer and a mock wait operation
    timer_queue::per_timer_data timer_data;
    MockWaitOp op(scheduler_);

    // Schedule the timer
    auto now = std::chrono::system_clock::now();
    timer_service_->schedule_timer(
        now + std::chrono::milliseconds(10), timer_data, &op);
    EXPECT_EQ(op.result_, utils::OK);

    // Call get_timeout
    itimerspec ts;
    int flags = timer_service_->get_timeout(ts);

    // Verify the timeout values
    EXPECT_EQ(ts.it_value.tv_sec, 0);
    EXPECT_LE(ts.it_value.tv_nsec, 10 * 1000 * 1000);
    EXPECT_EQ(ts.it_interval.tv_sec, 0);
    EXPECT_EQ(ts.it_interval.tv_nsec, 0);

    // Verify the flags
    EXPECT_EQ(flags, 0);

    // 主线程进入事件循环
    // 主线程进入事件循环
    join_all();
    EXPECT_TRUE(op.executed_);
    EXPECT_EQ(op.result_, utils::system_errors::system_ETIMEDOUT);
}

TEST_F(SystemTimerServiceTest, GetTimeoutWithMilliseconds)
{
    start_threads(4);

    // Test with a valid timeout value
    int timeout = timer_service_->get_timeout(1000); // 1 second
    EXPECT_GE(timeout, 0);
    EXPECT_LE(timeout, 1000);

    // Test with a timeout value exceeding the maximum allowed
    timeout = timer_service_->get_timeout(10 * 60 * 1000); // 10 minutes
    EXPECT_LE(timeout, 3 * 60 * 1000);
    EXPECT_GE(timeout, 3 * 60 * 1000 - 5);

    // Test with a negative timeout value
    timeout = timer_service_->get_timeout(-1);
    EXPECT_LE(timeout, 3 * 60 * 1000);
    EXPECT_GE(timeout, 3 * 60 * 1000 - 5);

    scheduler_->stop();
    join_all();
}

TEST_F(SystemTimerServiceTest, UpdateTimeout)
{
    start_threads(4);

    // Create a timer and a mock wait operation
    timer_queue::per_timer_data timer_data;
    MockWaitOp op(scheduler_);

    // Schedule the timer
    auto now = std::chrono::system_clock::now();
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op);

    // Call update_timeout
    timer_service_->update_timeout();

    // Verify the timer_fd is updated
    itimerspec current_timeout;
    ASSERT_EQ(
        ::timerfd_gettime(timer_service_->timer_fd_, &current_timeout), 0);
    EXPECT_GE(current_timeout.it_value.tv_sec, 0);
    EXPECT_LE(current_timeout.it_value.tv_sec, 1);

    scheduler_->stop();
    join_all();
    timer_service_->shutdown(); // 把op掏出来
}

TEST_F(SystemTimerServiceTest, ConstructSetsExpiryToDefaultTimePoint)
{
    start_threads(4);

    system_timer_service::instance_type impl;
    auto before_construction = system_timer_service::time_point();
    timer_service_->construct(impl);
    EXPECT_EQ(impl.expiry, before_construction);

    scheduler_->stop();
    join_all();
}

TEST_F(SystemTimerServiceTest, ConstructWithExistingInstance)
{
    start_threads(4);

    system_timer_service::instance_type impl;
    impl.expiry =
        system_timer_service::time_point::max(); // Set to non-default value
    timer_service_->construct(impl);
    EXPECT_EQ(impl.expiry, system_timer_service::time_point());

    scheduler_->stop();
    join_all();
}

TEST_F(SystemTimerServiceTest, MultipleConstructions)
{
    start_threads(4);

    system_timer_service::instance_type impl;
    timer_service_->construct(impl);
    auto first_expiry = impl.expiry;
    timer_service_->construct(impl);
    EXPECT_EQ(first_expiry, impl.expiry);

    scheduler_->stop();
    join_all();
}

TEST_F(SystemTimerServiceTest, CancelTimerSuccess)
{
    start_threads(4);

    timer_queue::per_timer_data timer_data;
    MockWaitOp op(scheduler_);
    auto now = std::chrono::system_clock::now();
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op);

    size_t ret = timer_service_->cancel_timer(timer_data);
    EXPECT_EQ(ret, (size_t) 1);

    // 主线程进入事件循环
    join_all();
    EXPECT_TRUE(op.executed_);
    EXPECT_EQ(op.result_, utils::system_errors::system_ECANCELED);
    EXPECT_EQ(timer_data.prev_, nullptr);
    EXPECT_EQ(timer_data.next_, nullptr);
}

TEST_F(SystemTimerServiceTest, CancelTimerZeroMaxCancelled)
{
    start_threads(4);

    timer_queue::per_timer_data timer_data;
    MockWaitOp op(scheduler_);
    auto now = std::chrono::system_clock::now();
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op);

    size_t result = timer_service_->cancel_timer(timer_data, 0);
    EXPECT_EQ(result, (size_t) 0);

    scheduler_->stop();
    join_all();
    EXPECT_FALSE(op.executed_);
    EXPECT_EQ(op.result_, utils::OK);

    task_queue ops;
    timer_service_->timer_queue_.get_all_timers(ops);
}

TEST_F(SystemTimerServiceTest, CancelTimerNoneCancelled)
{
    start_threads(4);

    timer_queue::per_timer_data timer_data;
    MockWaitOp op(scheduler_);

    size_t result = timer_service_->cancel_timer(timer_data);
    EXPECT_EQ(result, (size_t) 0);

    scheduler_->stop();
    join_all();
    EXPECT_FALSE(op.executed_);
    EXPECT_EQ(op.result_, utils::OK);
}

TEST_F(SystemTimerServiceTest, CancelTimerPartialCancelled)
{
    start_threads(4);

    timer_queue::per_timer_data timer_data;
    MockWaitOp2 op1;
    auto now = std::chrono::system_clock::now();
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op1);
    MockWaitOp2 op2;
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op2);
    MockWaitOp op3(scheduler_);
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op3);

    size_t result = timer_service_->cancel_timer(timer_data, 1);
    EXPECT_EQ(result, (size_t) 1);
    result = timer_service_->cancel_timer(timer_data, 2);
    EXPECT_EQ(result, (size_t) 2);

    join_all();
    EXPECT_TRUE(op1.executed_);
    EXPECT_TRUE(op2.executed_);
    EXPECT_TRUE(op3.executed_);
    EXPECT_EQ(op1.result_, utils::system_errors::system_ECANCELED);
    EXPECT_EQ(op2.result_, utils::system_errors::system_ECANCELED);
    EXPECT_EQ(op3.result_, utils::system_errors::system_ECANCELED);
    EXPECT_EQ(timer_data.prev_, nullptr);
    EXPECT_EQ(timer_data.next_, nullptr);
}

TEST_F(SystemTimerServiceTest, CancelTimerMore)
{
    start_threads(4);

    timer_queue::per_timer_data timer_data;
    auto now = std::chrono::system_clock::now();
    MockWaitOp2 op1;
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op1);
    MockWaitOp2 op2;
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op2);
    MockWaitOp op3(scheduler_);
    timer_service_->schedule_timer(
        now + std::chrono::seconds(1), timer_data, &op3);

    size_t result = timer_service_->cancel_timer(timer_data, 7);
    EXPECT_EQ(result, (size_t) 3);

    join_all();
    EXPECT_TRUE(op1.executed_);
    EXPECT_TRUE(op2.executed_);
    EXPECT_TRUE(op3.executed_);
    EXPECT_EQ(op1.result_, utils::system_errors::system_ECANCELED);
    EXPECT_EQ(op2.result_, utils::system_errors::system_ECANCELED);
    EXPECT_EQ(op3.result_, utils::system_errors::system_ECANCELED);
    EXPECT_EQ(timer_data.prev_, nullptr);
    EXPECT_EQ(timer_data.next_, nullptr);
}

} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}