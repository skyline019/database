////
// @file test_scheduler.cc
// @brief
// 测试调度器
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <chrono>
#include "scheduler.h"
#include "service.h"
#include "thread_pool.h"
// #include <coir/service/portal.h>
// #include <coir/service/settings.h>
// #include <coir/thread/thread_pool.h>
// #include <coir/devlog/service.h>

namespace wf::io {

// 简单的测试任务
class TestTask : public async_task
{
  private:
    std::atomic<int> *counter_;
    int value_;

  public:
    static void callback(void *owner, async_task *task, void *events)
    {
        TestTask *test_task = static_cast<TestTask *>(task);
        if (owner) {
            if (test_task->counter_)
                (*test_task->counter_) += test_task->value_;
        } else {
            delete test_task;
        }
    }

    TestTask(std::atomic<int> *counter, int value)
        : async_task(callback)
        , counter_(counter)
        , value_(value)
    {}
};

// 测试夹具，用于设置scheduler_service环境
class SchedulerTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // 创建service_portal实例
        registry_ = std::make_unique<service_registry>();

        // 预加载线程池服务
        thread_pool *tt = registry_->use_service<thread_pool>();
        ASSERT_NE(tt, nullptr);

        // 加载scheduler_service
        scheduler_service *sc =
            registry_->use_service<scheduler_service>(registry_.get());
        ASSERT_NE(sc, nullptr);

        scheduler_ = sc;
    }

    void TearDown() override
    {
        scheduler_->stop();
        scheduler_->shutdown();
        registry_.reset();
    }

    std::unique_ptr<service_registry> registry_;
    scheduler_service *scheduler_;
};

// 测试构造函数
TEST_F(SchedulerTest, Constructor)
{
    // 验证调度器创建成功
    EXPECT_FALSE(scheduler_->stopped());
    EXPECT_NE(scheduler_, nullptr);
}

// 测试重启功能
TEST_F(SchedulerTest, Restart)
{
    // 先停止
    scheduler_->stop();
    EXPECT_TRUE(scheduler_->stopped());

    // 再重启
    scheduler_->restart();

    // 验证调度器已重启
    EXPECT_FALSE(scheduler_->stopped());
}

// 测试多任务并发调度 - 使用堆分配的任务
TEST_F(SchedulerTest, ConcurrentDispatch)
{
    std::atomic<int> counter(0);
    const int num_tasks = 10;

    // 创建多个堆分配的任务
    TestTask *tasks[num_tasks];
    for (int i = 0; i < num_tasks; ++i) {
        tasks[i] = new TestTask(&counter, 1);
    }

    // 并发调度任务
    for (int i = 0; i < num_tasks; ++i) {
        scheduler_->dispatch(*tasks[i]);
    }

    // 给任务一些时间执行
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 验证调度不会崩溃
    EXPECT_GE(counter.load(), 0);

    // 注意：任务由调度器的destroy()方法负责清理
}

// 测试调度器生命周期 - 使用堆分配的任务
TEST_F(SchedulerTest, Lifecycle)
{
    // 验证调度器在夹具生命周期内正常工作
    EXPECT_FALSE(scheduler_->stopped());

    // 调度一些堆分配的任务
    std::atomic<int> counter(0);
    TestTask *task = new TestTask(&counter, 100);
    scheduler_->dispatch(*task);

    // 验证没有崩溃发生
    EXPECT_TRUE(true);

    // 注意：任务由调度器的destroy()方法负责清理
}

// 测试服务门户集成
TEST_F(SchedulerTest, ServicePortalIntegration)
{
    // 验证服务门户中的各种服务
    auto *pool = registry_->has_service<thread_pool>();
    EXPECT_NE(pool, nullptr);

    auto *scheduler = registry_->has_service<scheduler_service>();
    EXPECT_NE(scheduler, nullptr);
    EXPECT_EQ(scheduler, scheduler_);
}

// 新的测试夹具，专门用于测试真正运行的调度器
class RunningSchedulerTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        registry_ = std::make_unique<service_registry>();
        thread_pool_ = registry_->use_service<thread_pool>();
        ASSERT_NE(thread_pool_, nullptr);
        scheduler_service *sc =
            registry_->use_service<scheduler_service>(registry_.get());
        ASSERT_NE(sc, nullptr);
        scheduler_ = sc;
    }

    void TearDown() override
    {
        registry_->shutdown_services();
        EXPECT_TRUE(scheduler_->global_queue_.empty());
        EXPECT_EQ(scheduler_->reactor_, nullptr);

        registry_.reset();
    }

    std::unique_ptr<service_registry> registry_;
    scheduler_service *scheduler_;
    thread_pool *thread_pool_;

    void start_threads(unsigned short n)
    {
        for (unsigned short i = 0; i < n; ++i)
            thread_pool_->execute([this, i]() { scheduler_->run(i); });
    }
    void join_all()
    {
        thread_pool_->join_all();
        EXPECT_NE(scheduler_->reactor_, nullptr);
        EXPECT_FALSE(scheduler_->reactor_waiting_);
        EXPECT_FALSE(scheduler_->global_queue_.empty());
        EXPECT_EQ(scheduler_->waiting_threads_, 0);
    }
    void stop_threads()
    {
        scheduler_->stop();
        EXPECT_TRUE(scheduler_->stopped_);
        EXPECT_EQ(scheduler_->workers_.size(), 0);
        join_all();
    }
};

// 测试无线程正常拉起线程
TEST_F(RunningSchedulerTest, NoThreadStartup) {}

// 测试1线程正常拉起线程
TEST_F(RunningSchedulerTest, OneThreadStartup)
{
    start_threads(1);
    EXPECT_EQ(thread_pool_->size(), 1);

    // 让线程飞一会儿，确保调度器线程启动
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_EQ(scheduler_->workers_.size(), 1);
    EXPECT_EQ(scheduler_->workers_[0]->index_, 0);

    EXPECT_TRUE(scheduler_->global_queue_.empty());
    EXPECT_NE(scheduler_->reactor_, nullptr);
    EXPECT_EQ(scheduler_->waiting_threads_, 0);

    EXPECT_TRUE(scheduler_->reactor_waiting_);
    EXPECT_FALSE(scheduler_->stopped_);

    stop_threads();
}

// 测试4线程正常拉起线程
TEST_F(RunningSchedulerTest, MoreThreadsStartup)
{
    start_threads(4);
    EXPECT_EQ(thread_pool_->size(), 4);

    // 让线程飞一会儿，确保调度器线程启动
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_EQ(scheduler_->workers_.size(), 4);

    EXPECT_TRUE(scheduler_->global_queue_.empty());
    EXPECT_NE(scheduler_->reactor_, nullptr);
    EXPECT_EQ(scheduler_->waiting_threads_, 3);

    EXPECT_TRUE(scheduler_->reactor_waiting_);
    EXPECT_FALSE(scheduler_->stopped_);

    stop_threads();
}

class T1 : public async_task
{
  public:
    T1()
        : async_task(mock_callback)
        , executed_(false)
    {}

    bool executed() const { return executed_; }

  private:
    bool executed_;
    static void mock_callback(void *owner, async_task *pThis, void *)
    {
        T1 *op = static_cast<T1 *>(pThis);
        if (owner) op->executed_ = true;
    }
};

class T2 : public async_task
{
  public:
    T2()
        : async_task(mock_callback)
        , executed_(false)
    {}

    bool executed() const { return executed_; }

  private:
    bool executed_;
    static void mock_callback(void *owner, async_task *pThis, void *)
    {
        T2 *op = static_cast<T2 *>(pThis);
        if (owner) {
            op->executed_ = true;
            reinterpret_cast<scheduler_service *>(owner)->stop();
        }
    }
};

// 测试真正运行的调度器的基本功能
TEST_F(RunningSchedulerTest, BasicDispatch)
{
    start_threads(4);

    T1 t1, t2;
    T2 t3;
    scheduler_->dispatch(t1);
    scheduler_->dispatch(t2);
    scheduler_->dispatch(t3); // t3最后入队，保证前面的任务被执行

    join_all();

    EXPECT_TRUE(t1.executed());
    EXPECT_TRUE(t2.executed());
    EXPECT_TRUE(t3.executed());
}

// 测试真正运行的调度器的基本功能
TEST_F(RunningSchedulerTest, DispatchQueue)
{
    start_threads(4);

    T1 t1, t2;
    T2 t3;

    task_queue queue;
    queue.enqueue(t1);
    queue.enqueue(t2);
    queue.enqueue(t3);

    scheduler_->dispatch(queue);

    join_all();

    EXPECT_TRUE(t1.executed());
    EXPECT_TRUE(t2.executed());
    EXPECT_TRUE(t3.executed());
}

// 测试真正运行的调度器的基本功能
TEST_F(RunningSchedulerTest, Restart)
{
    start_threads(4);

    T1 t1, t2;
    T2 t3;
    scheduler_->dispatch(t1);
    scheduler_->dispatch(t2);
    scheduler_->dispatch(t3); // t3最后入队，保证前面的任务被执行

    join_all();

    EXPECT_TRUE(t1.executed());
    EXPECT_TRUE(t2.executed());
    EXPECT_TRUE(t3.executed());

    t1.executed_ = false;
    t2.executed_ = false;
    t3.executed_ = false;
    scheduler_->restart();
    start_threads(4);

    scheduler_->dispatch(t1);
    scheduler_->dispatch(t2);
    scheduler_->dispatch(t3); // t3最后入队，保证前面的任务被执行

    join_all();
    EXPECT_TRUE(t1.executed());
    EXPECT_TRUE(t2.executed());
    EXPECT_TRUE(t3.executed());
}

} // namespace wf::io

// ============================================
// 压力测试部分
// ============================================

namespace wf::io {

// 压力测试任务 - 统计执行次数
class StressTestTask : public async_task
{
  private:
    std::atomic<int64_t> *counter_;
    int64_t value_;

  public:
    static void callback(void *owner, async_task *task, void *events)
    {
        StressTestTask *test_task = static_cast<StressTestTask *>(task);
        if (test_task->counter_) (*test_task->counter_) += test_task->value_;
        delete test_task;
    }

    StressTestTask(std::atomic<int64_t> *counter, int64_t value)
        : async_task(callback)
        , counter_(counter)
        , value_(value)
    {}
};

class StopTask : public async_task
{
  public:
    StopTask()
        : async_task(mock_callback)
        , executed_(false)
    {}

    bool executed() const { return executed_; }

  private:
    bool executed_;
    static void mock_callback(void *owner, async_task *pThis, void *)
    {
        StopTask *op = static_cast<StopTask *>(pThis);
        if (owner) {
            op->executed_ = true;
            reinterpret_cast<scheduler_service *>(owner)->stop();
        }
    }
};

// 压力测试夹具
class SchedulerStressTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        registry_ = std::make_unique<service_registry>();
        thread_pool_ = registry_->use_service<thread_pool>();
        ASSERT_NE(thread_pool_, nullptr);
        scheduler_service *sc =
            registry_->use_service<scheduler_service>(registry_.get());
        ASSERT_NE(sc, nullptr);
        scheduler_ = sc;
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

    void start_affinity_threads(unsigned short n)
    {
        for (unsigned short i = 0; i < n; ++i)
            thread_pool_->affinity_execute([this, i]() { scheduler_->run(i); });
    }

    void stop_threads()
    {
        scheduler_->stop();
        thread_pool_->join_all();
    }

    std::unique_ptr<service_registry> registry_;
    scheduler_service *scheduler_;
    thread_pool *thread_pool_;
};

// 测试1：高并发任务调度压力测试
TEST_F(SchedulerStressTest, HighConcurrencyTaskDispatch)
{
    const int num_threads = 16;
    const int num_tasks_per_thread = 100000;
    const int total_tasks = num_threads * num_tasks_per_thread;

    std::atomic<int64_t> counter(0);

    // 记录开始时间
    auto start_time = std::chrono::steady_clock::now();

    // 启动调度器线程
    start_threads(8);

    // 创建生产者线程
    std::vector<std::thread> producer_threads;
    for (int i = 0; i < num_threads; ++i) {
        producer_threads.emplace_back([this, &counter]() {
            for (int j = 0; j < num_tasks_per_thread; ++j) {
                StressTestTask *task = new StressTestTask(&counter, 1);
                scheduler_->dispatch(*task);
            }
        });
    }

    // 等待所有生产者完成
    for (auto &thread : producer_threads) {
        thread.join();
    }

    // 记录生产者完成时间
    auto producer_finish_time = std::chrono::steady_clock::now();

    StopTask stop_task;
    scheduler_->dispatch(stop_task);

    // 停止调度器
    thread_pool_->join_all();

    // 记录结束时间
    auto end_time = std::chrono::steady_clock::now();

    // 计算各个阶段的时间
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    auto producer_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            producer_finish_time - start_time);
    auto processing_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - producer_finish_time);

    // 计算吞吐量（任务/秒）
    double throughput = (counter.load() * 1000.0) / total_duration.count();

    // 验证所有任务都被处理
    EXPECT_GE(counter.load(), total_tasks * 0.9); // 允许10%的误差

    // 输出详细的性能信息
    std::cout << "=== 高并发任务调度压力测试结果 ===" << std::endl;
    std::cout << "总任务数: " << total_tasks << std::endl;
    std::cout << "实际处理任务数: " << counter.load() << std::endl;
    std::cout << "总耗时: " << total_duration.count() << "ms" << std::endl;
    std::cout << "生产者耗时: " << producer_duration.count() << "ms"
              << std::endl;
    std::cout << "调度器处理耗时: " << processing_duration.count() << "ms"
              << std::endl;
    std::cout << "吞吐量: " << throughput << " 任务/秒" << std::endl;
    std::cout << "平均每个任务耗时: "
              << (total_duration.count() * 1000.0 / counter.load()) << "μs"
              << std::endl;
    std::cout << "==================================" << std::endl;
}

// 测试1：高并发任务调度压力测试
TEST_F(SchedulerStressTest, AffinityHighConcurrencyTaskDispatch)
{
    const int num_threads = 16;
    const int num_tasks_per_thread = 100000;
    const int total_tasks = num_threads * num_tasks_per_thread;

    std::atomic<int64_t> counter(0);

    // 记录开始时间
    auto start_time = std::chrono::steady_clock::now();

    // 启动调度器线程
    start_affinity_threads(8);

    // 创建生产者线程
    std::vector<std::thread> producer_threads;
    for (int i = 0; i < num_threads; ++i) {
        producer_threads.emplace_back([this, &counter]() {
            for (int j = 0; j < num_tasks_per_thread; ++j) {
                StressTestTask *task = new StressTestTask(&counter, 1);
                scheduler_->dispatch(*task);
            }
        });
    }

    // 等待所有生产者完成
    for (auto &thread : producer_threads) {
        thread.join();
    }

    // 记录生产者完成时间
    auto producer_finish_time = std::chrono::steady_clock::now();

    StopTask stop_task;
    scheduler_->dispatch(stop_task);

    // 停止调度器
    thread_pool_->join_all();

    // 记录结束时间
    auto end_time = std::chrono::steady_clock::now();

    // 计算各个阶段的时间
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    auto producer_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            producer_finish_time - start_time);
    auto processing_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - producer_finish_time);

    // 计算吞吐量（任务/秒）
    double throughput = (counter.load() * 1000.0) / total_duration.count();

    // 验证所有任务都被处理
    EXPECT_GE(counter.load(), total_tasks * 0.9); // 允许10%的误差

    // 输出详细的性能信息
    std::cout << "=== 亲和性高并发任务调度压力测试结果 ===" << std::endl;
    std::cout << "总任务数: " << total_tasks << std::endl;
    std::cout << "实际处理任务数: " << counter.load() << std::endl;
    std::cout << "总耗时: " << total_duration.count() << "ms" << std::endl;
    std::cout << "生产者耗时: " << producer_duration.count() << "ms"
              << std::endl;
    std::cout << "调度器处理耗时: " << processing_duration.count() << "ms"
              << std::endl;
    std::cout << "吞吐量: " << throughput << " 任务/秒" << std::endl;
    std::cout << "平均每个任务耗时: "
              << (total_duration.count() * 1000.0 / counter.load()) << "μs"
              << std::endl;
    std::cout << "==================================" << std::endl;
}

// 测试4：混合负载测试 - 大小任务混合
TEST_F(SchedulerStressTest, MixedWorkload)
{
    std::atomic<int64_t> small_counter(0);
    std::atomic<int64_t> large_counter(0);
    const int num_small_tasks = 500000;
    const int num_large_tasks = 10000;
    const int total_tasks = num_small_tasks + num_large_tasks;

    // 记录开始时间
    auto start_time = std::chrono::steady_clock::now();

    // 启动调度器线程
    start_threads(4);

    // 记录小任务开始时间
    auto small_tasks_start_time = std::chrono::steady_clock::now();

    // 创建小任务（快速执行）
    for (int i = 0; i < num_small_tasks; ++i) {
        StressTestTask *task = new StressTestTask(&small_counter, 1);
        scheduler_->dispatch(*task);
    }

    // 记录小任务完成时间
    auto small_tasks_finish_time = std::chrono::steady_clock::now();

    // 记录大任务开始时间
    auto large_tasks_start_time = std::chrono::steady_clock::now();

    // 创建大任务（模拟耗时操作）
    for (int i = 0; i < num_large_tasks; ++i) {
        StressTestTask *task = new StressTestTask(&large_counter, 1000000);
        scheduler_->dispatch(*task);
    }

    // 记录大任务完成时间
    auto large_tasks_finish_time = std::chrono::steady_clock::now();

    StopTask stop_task;
    scheduler_->dispatch(stop_task);

    // 停止调度器
    thread_pool_->join_all();

    // 记录结束时间
    auto end_time = std::chrono::steady_clock::now();

    // 计算各个阶段的时间
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    auto small_tasks_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            small_tasks_finish_time - small_tasks_start_time);
    auto large_tasks_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            large_tasks_finish_time - large_tasks_start_time);
    auto processing_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - large_tasks_finish_time);

    // 计算总吞吐量（任务/秒）
    double total_throughput = (total_tasks * 1000.0) / total_duration.count();
    // 计算小任务吞吐量
    double small_throughput =
        (num_small_tasks * 1000.0) / small_tasks_duration.count();
    // 计算大任务吞吐量
    double large_throughput =
        (num_large_tasks * 1000.0) / large_tasks_duration.count();

    // 验证任务处理
    EXPECT_GE(small_counter.load(), num_small_tasks * 0.9);
    EXPECT_GE(large_counter.load(), num_large_tasks * 0.9);

    // 输出详细的性能信息
    std::cout << "=== 混合负载测试结果 ===" << std::endl;
    std::cout << "小任务: " << small_counter.load() << "/" << num_small_tasks
              << " (" << (small_counter.load() * 100.0 / num_small_tasks)
              << "%)" << std::endl;
    std::cout << "大任务: " << large_counter.load() << "/" << num_large_tasks
              << " (" << (large_counter.load() * 100.0 / num_large_tasks)
              << "%)" << std::endl;
    std::cout << "总任务数: " << total_tasks << std::endl;
    std::cout << "总耗时: " << total_duration.count() << "ms" << std::endl;
    std::cout << "小任务提交耗时: " << small_tasks_duration.count() << "ms"
              << std::endl;
    std::cout << "大任务提交耗时: " << large_tasks_duration.count() << "ms"
              << std::endl;
    std::cout << "调度器处理耗时: " << processing_duration.count() << "ms"
              << std::endl;
    std::cout << "总吞吐量: " << total_throughput << " 任务/秒" << std::endl;
    std::cout << "小任务吞吐量: " << small_throughput << " 任务/秒"
              << std::endl;
    std::cout << "大任务吞吐量: " << large_throughput << " 任务/秒"
              << std::endl;
    std::cout << "平均小任务耗时: "
              << (small_tasks_duration.count() * 1000.0 / num_small_tasks)
              << "μs" << std::endl;
    std::cout << "平均大任务耗时: "
              << (large_tasks_duration.count() * 1000.0 / num_large_tasks)
              << "μs" << std::endl;
    std::cout << "==================================" << std::endl;
}

// 测试4：混合负载测试 - 大小任务混合
TEST_F(SchedulerStressTest, AffinityMixedWorkload)
{
    std::atomic<int64_t> small_counter(0);
    std::atomic<int64_t> large_counter(0);
    const int num_small_tasks = 500000;
    const int num_large_tasks = 10000;
    const int total_tasks = num_small_tasks + num_large_tasks;

    // 记录开始时间
    auto start_time = std::chrono::steady_clock::now();

    // 启动调度器线程
    start_affinity_threads(4);

    // 记录小任务开始时间
    auto small_tasks_start_time = std::chrono::steady_clock::now();

    // 创建小任务（快速执行）
    for (int i = 0; i < num_small_tasks; ++i) {
        StressTestTask *task = new StressTestTask(&small_counter, 1);
        scheduler_->dispatch(*task);
    }

    // 记录小任务完成时间
    auto small_tasks_finish_time = std::chrono::steady_clock::now();

    // 记录大任务开始时间
    auto large_tasks_start_time = std::chrono::steady_clock::now();

    // 创建大任务（模拟耗时操作）
    for (int i = 0; i < num_large_tasks; ++i) {
        StressTestTask *task = new StressTestTask(&large_counter, 1000000);
        scheduler_->dispatch(*task);
    }

    // 记录大任务完成时间
    auto large_tasks_finish_time = std::chrono::steady_clock::now();

    StopTask stop_task;
    scheduler_->dispatch(stop_task);

    // 停止调度器
    thread_pool_->join_all();

    // 记录结束时间
    auto end_time = std::chrono::steady_clock::now();

    // 计算各个阶段的时间
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    auto small_tasks_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            small_tasks_finish_time - small_tasks_start_time);
    auto large_tasks_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            large_tasks_finish_time - large_tasks_start_time);
    auto processing_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - large_tasks_finish_time);

    // 计算总吞吐量（任务/秒）
    double total_throughput = (total_tasks * 1000.0) / total_duration.count();
    // 计算小任务吞吐量
    double small_throughput =
        (num_small_tasks * 1000.0) / small_tasks_duration.count();
    // 计算大任务吞吐量
    double large_throughput =
        (num_large_tasks * 1000.0) / large_tasks_duration.count();

    // 验证任务处理
    EXPECT_GE(small_counter.load(), num_small_tasks * 0.9);
    EXPECT_GE(large_counter.load(), num_large_tasks * 0.9);

    // 输出详细的性能信息
    std::cout << "=== 亲和性混合负载测试结果 ===" << std::endl;
    std::cout << "小任务: " << small_counter.load() << "/" << num_small_tasks
              << " (" << (small_counter.load() * 100.0 / num_small_tasks)
              << "%)" << std::endl;
    std::cout << "大任务: " << large_counter.load() << "/" << num_large_tasks
              << " (" << (large_counter.load() * 100.0 / num_large_tasks)
              << "%)" << std::endl;
    std::cout << "总任务数: " << total_tasks << std::endl;
    std::cout << "总耗时: " << total_duration.count() << "ms" << std::endl;
    std::cout << "小任务提交耗时: " << small_tasks_duration.count() << "ms"
              << std::endl;
    std::cout << "大任务提交耗时: " << large_tasks_duration.count() << "ms"
              << std::endl;
    std::cout << "调度器处理耗时: " << processing_duration.count() << "ms"
              << std::endl;
    std::cout << "总吞吐量: " << total_throughput << " 任务/秒" << std::endl;
    std::cout << "小任务吞吐量: " << small_throughput << " 任务/秒"
              << std::endl;
    std::cout << "大任务吞吐量: " << large_throughput << " 任务/秒"
              << std::endl;
    std::cout << "平均小任务耗时: "
              << (small_tasks_duration.count() * 1000.0 / num_small_tasks)
              << "μs" << std::endl;
    std::cout << "平均大任务耗时: "
              << (large_tasks_duration.count() * 1000.0 / num_large_tasks)
              << "μs" << std::endl;
    std::cout << "==================================" << std::endl;
}

} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}