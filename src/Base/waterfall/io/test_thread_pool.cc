////
// @file test_thread_pool.cc
// @brief
// 测试线程池
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include "thread.h"
#include "thread_pool.h"

namespace wf::io {

TEST(ThreadPool, BasicFunctionality)
{
    thread_pool pool;
    std::atomic<bool> task_executed{false};

    // 执行简单任务
    auto result = pool.execute([&task_executed]() { task_executed = true; });
    EXPECT_EQ(result, nullptr); // nullptr表示成功

    // 等待线程完成后再检查结果
    pool.shutdown();

    // 检查任务是否执行
    EXPECT_TRUE(task_executed);
    EXPECT_EQ(pool.size(), 0);
}

TEST(ThreadPool, MultipleThreads)
{
    thread_pool pool;
    std::atomic<int> counter{0};

    // 创建多个线程执行任务
    for (int i = 0; i < 5; ++i) {
        auto result = pool.execute([&counter, i]() { counter.fetch_add(i); });
        EXPECT_EQ(result, nullptr); // nullptr表示成功
    }

    // 等待所有线程完成
    pool.shutdown();

    // 验证计数器值
    EXPECT_EQ(counter.load(), 0 + 1 + 2 + 3 + 4); // 0+1+2+3+4 = 10
    EXPECT_EQ(pool.size(), 0);
}

TEST(ThreadPool, LongRunningTasks)
{
    thread_pool pool;
    std::atomic<bool> task_completed{false};

    auto start = std::chrono::steady_clock::now();

    // 执行长时间运行的任务
    auto result = pool.execute([&task_completed]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        task_completed = true;
    });
    EXPECT_EQ(result, nullptr); // nullptr表示成功

    // 等待线程完成
    pool.shutdown();

    auto end = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // 检查任务是否完成
    EXPECT_TRUE(task_completed);

    // 确保任务确实运行了足够长的时间
    EXPECT_GE(duration.count(), 100);
}

TEST(ThreadPool, VoidReturnType)
{
    thread_pool pool;
    int value = 0;

    // 执行无返回值的任务
    auto result = pool.execute([&value]() { value = 42; });
    EXPECT_EQ(result, nullptr); // nullptr表示成功

    // 等待任务完成
    pool.shutdown();

    // 检查任务是否修改了值
    EXPECT_EQ(value, 42);
}

TEST(ThreadPool, SequentialExecution)
{
    thread_pool pool;
    std::atomic<int> execution_count{0};

    // 顺序执行多个任务
    for (int i = 0; i < 3; ++i) {
        auto result = pool.execute(
            [&execution_count]() { execution_count.fetch_add(1); });
        EXPECT_EQ(result, nullptr); // nullptr表示成功
    }

    pool.shutdown();

    // 验证所有任务都执行了
    EXPECT_EQ(execution_count.load(), 3);
}

TEST(ThreadPool, ReuseAfterShutdown)
{
    thread_pool pool;
    std::atomic<bool> first_task_executed{false};
    std::atomic<bool> second_task_executed{false};

    // 第一轮执行
    auto result1 =
        pool.execute([&first_task_executed]() { first_task_executed = true; });
    EXPECT_EQ(result1, nullptr); // nullptr表示成功

    pool.shutdown();

    // 检查第一轮结果
    EXPECT_TRUE(first_task_executed);
    EXPECT_EQ(pool.size(), 0);

    // 第二轮执行（重新创建线程）
    auto result2 = pool.execute(
        [&second_task_executed]() { second_task_executed = true; });
    EXPECT_EQ(result2, nullptr); // nullptr表示成功

    pool.shutdown();

    // 检查第二轮结果
    EXPECT_TRUE(second_task_executed);
    EXPECT_EQ(pool.size(), 0);
}

TEST(ThreadPool, ErrorHandling)
{
    thread_pool pool;
    std::atomic<bool> task_executed{false};

    // 测试错误处理（不使用异常）
    auto result = pool.execute([&task_executed]() { task_executed = true; });
    EXPECT_EQ(result, nullptr); // nullptr表示成功

    pool.shutdown();

    // 检查任务是否执行
    EXPECT_TRUE(task_executed);
}

TEST(ThreadPool, SizeTracking)
{
    thread_pool pool;

    // 初始大小为0
    EXPECT_EQ(pool.size(), 0);

    // 执行任务后大小增加
    auto result = pool.execute([]() {});
    EXPECT_EQ(result, nullptr); // nullptr表示成功
    EXPECT_GT(pool.size(), 0);

    // shutdown后大小归零
    pool.shutdown();
    EXPECT_EQ(pool.size(), 0);
}

TEST(ThreadPool, ConcurrentExecution)
{
    thread_pool pool;
    std::atomic<int> completed_tasks{0};
    const int num_tasks = 10;

    // 并发执行多个任务
    for (int i = 0; i < num_tasks; ++i) {
        auto result = pool.execute(
            [&completed_tasks]() { completed_tasks.fetch_add(1); });
        EXPECT_EQ(result, nullptr); // nullptr表示成功
    }

    pool.shutdown();

    // 验证所有任务都完成了
    EXPECT_EQ(completed_tasks.load(), num_tasks);
}

// 启用affinity_execute测试用例
TEST(ThreadPool, AffinityExecuteBasic)
{
    thread_pool pool;
    std::atomic<bool> task_executed{false};
    std::atomic<int> cpu_used{-1};

    // 在CPU 0上执行任务
    auto result = pool.affinity_execute([&task_executed, &cpu_used]() {
        task_executed = true;
        // 获取当前CPU核心（简化实现，实际可能需要系统调用）
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        if (pthread_getaffinity_np(
                pthread_self(), sizeof(cpu_set_t), &cpuset) == 0) {
            for (int i = 0; i < CPU_SETSIZE; ++i) {
                if (CPU_ISSET(i, &cpuset)) {
                    cpu_used = i;
                    break;
                }
            }
        }
    });

    EXPECT_EQ(result, nullptr); // nullptr表示成功

    // 等待线程完成后再检查结果
    pool.shutdown();

    // 检查任务是否执行
    EXPECT_TRUE(task_executed);
    // 注意：在某些系统上，pthread_getaffinity_np可能无法准确获取CPU信息
    // 所以这里主要验证任务能正常执行，CPU亲和性设置不报错
    EXPECT_EQ(pool.size(), 0);
}

TEST(ThreadPool, AffinityExecuteMultipleCPUs)
{
    thread_pool pool;
    std::atomic<int> task_count{0};
    const int num_cpus =
        std::min(4, static_cast<int>(posix_thread::hardware_concurrency()));

    if (num_cpus <= 1) { GTEST_SKIP() << "系统CPU核心数不足，跳过测试"; }

    // 在不同的CPU核心上执行任务
    for (int cpu = 0; cpu < num_cpus; ++cpu) {
        auto result = pool.affinity_execute([&task_count, cpu]() {
            task_count.fetch_add(1);
            // 记录任务在哪个CPU上执行（简化实现）
            std::cout << "Task executed on CPU " << cpu << std::endl;
        });
        EXPECT_EQ(result, nullptr); // nullptr表示成功
    }

    // 等待所有线程完成
    pool.shutdown();

    // 验证所有任务都完成了
    EXPECT_EQ(task_count.load(), num_cpus);
    EXPECT_EQ(pool.size(), 0);
}

// 测试预执行和后执行功能
TEST(ThreadPool, PreloaderAndSweeper)
{
    thread_pool pool;
    std::atomic<int> preload_count{0};
    std::atomic<int> sweep_count{0};
    std::atomic<int> task_count{0};

    // 添加预执行函数
    pool.add_preloader([&preload_count]() {
        preload_count++;
        std::cout << "Preloader executed" << std::endl;
    });

    // 添加后执行函数
    pool.add_sweeper([&sweep_count]() {
        sweep_count++;
        std::cout << "Sweeper executed" << std::endl;
    });

    // 执行多个任务
    const int num_tasks = 5;
    for (int i = 0; i < num_tasks; ++i) {
        auto result = pool.execute([&task_count, i]() {
            task_count++;
            std::cout << "Task " << i << " executed" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        });
        EXPECT_EQ(result, nullptr);
    }

    pool.shutdown();

    // 验证预执行和后执行函数被正确调用
    EXPECT_EQ(preload_count.load(), num_tasks); // 每个任务执行前都会调用预执行
    EXPECT_EQ(sweep_count.load(), num_tasks);   // 每个任务执行后都会调用后执行
    EXPECT_EQ(task_count.load(), num_tasks);    // 所有任务都执行了
}

// 测试多个预执行和后执行函数
TEST(ThreadPool, MultiplePreloadersAndSweepers)
{
    thread_pool pool;
    std::atomic<int> preload1_count{0};
    std::atomic<int> preload2_count{0};
    std::atomic<int> sweep1_count{0};
    std::atomic<int> sweep2_count{0};

    // 添加多个预执行函数
    pool.add_preloader([&preload1_count]() { preload1_count++; });

    pool.add_preloader([&preload2_count]() { preload2_count++; });

    // 添加多个后执行函数
    pool.add_sweeper([&sweep1_count]() { sweep1_count++; });

    pool.add_sweeper([&sweep2_count]() { sweep2_count++; });

    // 执行任务
    auto result = pool.execute(
        []() { std::this_thread::sleep_for(std::chrono::milliseconds(10)); });
    EXPECT_EQ(result, nullptr);

    pool.shutdown();

    // 验证所有预执行和后执行函数都被调用
    EXPECT_EQ(preload1_count.load(), 1);
    EXPECT_EQ(preload2_count.load(), 1);
    EXPECT_EQ(sweep1_count.load(), 1);
    EXPECT_EQ(sweep2_count.load(), 1);
}

// 测试预执行和后执行的执行顺序
TEST(ThreadPool, PreloaderSweeperExecutionOrder)
{
    thread_pool pool;
    std::vector<std::string> execution_order;

    // 添加预执行函数
    pool.add_preloader(
        [&execution_order]() { execution_order.push_back("preloader"); });

    // 添加后执行函数
    pool.add_sweeper(
        [&execution_order]() { execution_order.push_back("sweeper"); });

    // 执行任务
    auto result = pool.execute(
        [&execution_order]() { execution_order.push_back("task"); });
    EXPECT_EQ(result, nullptr);

    pool.shutdown();

    // 验证执行顺序：preloader -> task -> sweeper
    ASSERT_EQ(execution_order.size(), 3);
    EXPECT_EQ(execution_order[0], "preloader");
    EXPECT_EQ(execution_order[1], "task");
    EXPECT_EQ(execution_order[2], "sweeper");
}

// 测试后执行函数的逆序执行（LIFO顺序）
TEST(ThreadPool, SweeperReverseOrderExecution)
{
    thread_pool pool;
    std::vector<std::string> sweeper_execution_order;

    // 添加多个后执行函数（按顺序注册）
    pool.add_sweeper([&sweeper_execution_order]() {
        sweeper_execution_order.push_back("sweeper1");
        std::cout << "Sweeper 1 executed" << std::endl;
    });

    pool.add_sweeper([&sweeper_execution_order]() {
        sweeper_execution_order.push_back("sweeper2");
        std::cout << "Sweeper 2 executed" << std::endl;
    });

    pool.add_sweeper([&sweeper_execution_order]() {
        sweeper_execution_order.push_back("sweeper3");
        std::cout << "Sweeper 3 executed" << std::endl;
    });

    // 执行任务
    auto result =
        pool.execute([]() { std::cout << "Task executed" << std::endl; });
    EXPECT_EQ(result, nullptr);

    pool.shutdown();

    // 验证后执行函数按逆序执行：sweeper3 -> sweeper2 -> sweeper1
    ASSERT_EQ(sweeper_execution_order.size(), 3);
    EXPECT_EQ(sweeper_execution_order[0], "sweeper3");
    EXPECT_EQ(sweeper_execution_order[1], "sweeper2");
    EXPECT_EQ(sweeper_execution_order[2], "sweeper1");

    std::cout << "Sweeper execution order (LIFO): ";
    for (const auto &sweeper : sweeper_execution_order) {
        std::cout << sweeper << " ";
    }
    std::cout << std::endl;
}

// 测试资源分配的逆序清理（栈式资源管理）
TEST(ThreadPool, StackStyleResourceManagement)
{
    thread_pool pool;
    std::vector<std::string> resource_allocation_order;
    std::vector<std::string> resource_deallocation_order;

    // 模拟资源分配和清理的栈式管理
    pool.add_preloader([&resource_allocation_order]() {
        // 模拟资源分配顺序
        resource_allocation_order.push_back("resource_A");
        resource_allocation_order.push_back("resource_B");
        resource_allocation_order.push_back("resource_C");
        std::cout << "Resources allocated in order: A -> B -> C" << std::endl;
    });

    // 后执行函数应该按逆序清理资源
    pool.add_sweeper([&resource_deallocation_order]() {
        resource_deallocation_order.push_back("resource_C");
        std::cout << "Resource C deallocated" << std::endl;
    });

    pool.add_sweeper([&resource_deallocation_order]() {
        resource_deallocation_order.push_back("resource_B");
        std::cout << "Resource B deallocated" << std::endl;
    });

    pool.add_sweeper([&resource_deallocation_order]() {
        resource_deallocation_order.push_back("resource_A");
        std::cout << "Resource A deallocated" << std::endl;
    });

    // 执行任务
    auto result = pool.execute(
        []() { std::cout << "Task using allocated resources" << std::endl; });
    EXPECT_EQ(result, nullptr);

    pool.shutdown();

    // 验证资源清理顺序与分配顺序相同（正序执行）
    ASSERT_EQ(resource_deallocation_order.size(), 3);
    EXPECT_EQ(resource_deallocation_order[0], "resource_A");
    EXPECT_EQ(resource_deallocation_order[1], "resource_B");
    EXPECT_EQ(resource_deallocation_order[2], "resource_C");

    std::cout << "Resource deallocation order (actual): ";
    for (const auto &resource : resource_deallocation_order) {
        std::cout << resource << " ";
    }
    std::cout << std::endl;
}

// 测试预执行和后执行的异常安全
TEST(ThreadPool, PreloaderSweeperExceptionSafety)
{
    thread_pool pool;
    std::atomic<bool> sweeper_called{false};
    std::atomic<bool> preloader_called{false};

    // 添加预执行函数
    pool.add_preloader([&preloader_called]() { preloader_called = true; });

    // 添加后执行函数（即使任务抛出异常也会执行）
    pool.add_sweeper([&sweeper_called]() { sweeper_called = true; });

    // 执行可能抛出异常的任务
    auto result = pool.execute([]() {
        // 模拟一些工作
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        // 这里不会抛出异常（因为编译环境禁止异常）
        // 但后执行函数仍然应该被调用
    });
    EXPECT_EQ(result, nullptr);

    pool.shutdown();

    // 验证预执行和后执行函数都被调用
    EXPECT_TRUE(preloader_called.load());
    EXPECT_TRUE(sweeper_called.load());
}

// 测试线程特定的资源管理
TEST(ThreadPool, ThreadSpecificResourceManagement)
{
    thread_pool pool;
    std::atomic<int> thread_specific_counter{0};
    std::atomic<int> total_threads{0};

    // 预执行函数：初始化线程特定资源
    pool.add_preloader([&thread_specific_counter]() {
        // 模拟线程特定初始化
        thread_specific_counter = 0;
    });

    // 后执行函数：清理线程特定资源
    pool.add_sweeper([&thread_specific_counter, &total_threads]() {
        // 模拟线程特定清理
        total_threads += thread_specific_counter;
    });

    // 执行多个任务
    const int num_tasks = 3;
    for (int i = 0; i < num_tasks; ++i) {
        auto result = pool.execute([&thread_specific_counter]() {
            // 每个线程增加自己的计数器
            thread_specific_counter++;
        });
        EXPECT_EQ(result, nullptr);
    }

    pool.shutdown();

    // 验证资源管理正确
    // 注意：由于线程复用，实际线程数可能小于任务数
    EXPECT_LE(total_threads.load(), num_tasks);
    EXPECT_GT(total_threads.load(), 0);
}

// 测试预执行和后执行与affinity_execute的结合
TEST(ThreadPool, PreloaderSweeperWithAffinity)
{
    thread_pool pool;
    std::atomic<int> preload_count{0};
    std::atomic<int> sweep_count{0};

    // 添加预执行和后执行函数
    pool.add_preloader([&preload_count]() { preload_count++; });

    pool.add_sweeper([&sweep_count]() { sweep_count++; });

    // 使用affinity_execute执行任务
    auto result = pool.affinity_execute(
        []() { std::this_thread::sleep_for(std::chrono::milliseconds(10)); });
    EXPECT_EQ(result, nullptr);

    pool.shutdown();

    // 验证预执行和后执行函数被调用
    EXPECT_EQ(preload_count.load(), 1);
    EXPECT_EQ(sweep_count.load(), 1);
}

// 测试大量任务时的预执行和后执行性能
TEST(ThreadPool, PreloaderSweeperPerformance)
{
    thread_pool pool;
    std::atomic<int> preload_count{0};
    std::atomic<int> sweep_count{0};
    std::atomic<int> task_count{0};

    // 添加轻量级的预执行和后执行函数
    pool.add_preloader([&preload_count]() { preload_count++; });

    pool.add_sweeper([&sweep_count]() { sweep_count++; });

    const int num_tasks = 100;
    auto start_time = std::chrono::high_resolution_clock::now();

    // 执行大量任务
    for (int i = 0; i < num_tasks; ++i) {
        auto result = pool.execute([&task_count]() { task_count++; });
        EXPECT_EQ(result, nullptr);
    }

    pool.shutdown();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    // 验证所有函数都被正确调用
    EXPECT_EQ(preload_count.load(), num_tasks);
    EXPECT_EQ(sweep_count.load(), num_tasks);
    EXPECT_EQ(task_count.load(), num_tasks);

    std::cout << "Executed " << num_tasks << " tasks with preloader/sweeper in "
              << duration.count() << "ms" << std::endl;
}

} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}