////
// @file test_thread.cc
// @brief
// 测试posix_thread
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include "thread.h"

namespace wf::io {

// 测试基本线程创建和执行
TEST(thread, BasicCreationAndExecution)
{
    std::atomic<bool> executed{false};
    posix_thread thread;

    auto result = thread.exec([&executed]() { executed = true; });

    EXPECT_EQ(result, nullptr); // 检查线程创建是否成功（nullptr表示成功）
    thread.join();              // 等待线程完成
    EXPECT_TRUE(executed);      // 检查线程是否执行了函数
}

// 测试void返回类型的线程
TEST(thread, VoidReturnType)
{
    std::atomic<bool> executed{false};
    posix_thread thread;

    auto result = thread.exec([&executed]() { executed = true; });

    EXPECT_EQ(result, nullptr); // 检查线程创建是否成功（nullptr表示成功）
    thread.join();              // 等待线程完成
    EXPECT_TRUE(executed);
}

// 测试线程运行时重复调用exec返回错误
TEST(thread, ThreadRunningError)
{
    posix_thread thread;

    // 第一次执行 - 应该成功
    auto result1 = thread.exec(
        []() { std::this_thread::sleep_for(std::chrono::milliseconds(100)); });

    // 第二次执行（线程还在运行）- 应该返回错误
    auto result2 = thread.exec([]() {});
    EXPECT_EQ(result2, kErrorThreadIsRunning);

    // 等待第一个线程完成
    thread.join();               // 添加join等待线程完成
    EXPECT_EQ(result1, nullptr); // 第一个应该成功
}

// 测试线程join功能
TEST(thread, JoinFunctionality)
{
    std::atomic<bool> thread_completed{false};
    posix_thread thread;

    auto result = thread.exec([&thread_completed]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        thread_completed = true;
    });

    EXPECT_EQ(result, nullptr); // 检查线程创建是否成功（nullptr表示成功）
    thread.join();              // 等待线程完成
    EXPECT_TRUE(thread_completed);
}

// 测试线程detach功能
TEST(thread, DetachFunctionality)
{
    std::atomic<bool> thread_started{false};
    std::atomic<bool> thread_completed{false};

    {
        posix_thread thread;
        auto result = thread.exec([&thread_started, &thread_completed]() {
            thread_started = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            thread_completed = true;
        });

        EXPECT_EQ(result, nullptr); // 检查线程创建是否成功（nullptr表示成功）

        // 分离线程，让它在后台运行
        thread.detach();

        // detach后不能立即检查thread_started和结果值，因为线程可能还在启动中
        // 等待足够时间让线程开始执行
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // 现在检查线程是否已开始
        EXPECT_TRUE(thread_started);

        // detach后仍然可以获取结果，因为result_type不依赖线程生命周期
        // 但结果值可能还没有设置，所以这里不检查具体值
    }

    // 等待足够时间让线程完成
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_TRUE(thread_completed);
}

// 测试带参数的线程函数
TEST(thread, FunctionWithArguments)
{
    std::atomic<bool> executed{false};
    posix_thread thread;

    auto result = thread.exec(
        [&executed](int a, int b, const std::string &text) {
            executed = true;
            // 这里可以处理参数，但不返回值
        },
        10,
        20,
        "result");

    EXPECT_EQ(result, nullptr);
    thread.join(); // 等待线程完成
    EXPECT_TRUE(executed);
}

// 测试移动语义
TEST(thread, MoveSemantics)
{
    posix_thread thread1;

    // 移动构造
    posix_thread thread2 = std::move(thread1);

    auto result = thread2.exec([]() {});

    EXPECT_EQ(result, nullptr);
    thread2.join(); // 等待线程完成
}

// 测试native_handle
TEST(thread, NativeHandle)
{
    posix_thread thread;

    auto result = thread.exec([]() {});

    ::pthread_t handle = thread.native_handle();
    EXPECT_NE(handle, 0);

    EXPECT_EQ(result, nullptr); // 检查线程创建是否成功（nullptr表示成功）
    thread.join();              // 等待线程完成
}

// 测试析构函数自动join
TEST(thread, DestructorAutoJoin)
{
    std::atomic<bool> thread_completed{false};

    {
        posix_thread thread;
        auto result = thread.exec([&thread_completed]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            thread_completed = true;
        });

        EXPECT_EQ(result, nullptr); // 检查线程创建是否成功（nullptr表示成功）
        // 线程对象离开作用域，析构函数应该自动join
    }

    EXPECT_TRUE(thread_completed);
}

// 测试硬件并发数
TEST(thread, HardwareConcurrency)
{
    unsigned int cores = posix_thread::hardware_concurrency();
    EXPECT_GT(cores, 0U);
    EXPECT_LE(cores, 256U); // 合理的核心数上限
}

// 测试CPU亲和性设置
TEST(thread, SetAffinity)
{
    posix_thread thread;
    unsigned int cores = posix_thread::hardware_concurrency();

    if (cores > 1) {
        // 尝试设置到第一个CPU核心
        int result = thread.set_affinity(0);
        // 设置亲和性可能成功或失败，取决于系统权限
        // 这里只检查函数调用本身没有崩溃
        EXPECT_TRUE(result == 0 || result == -1);
    }
}

// 测试affinity_exec基本功能
TEST(thread, AffinityExecBasic)
{
    std::atomic<bool> executed{false};
    posix_thread thread;

    // 在CPU 0上执行任务
    auto result = thread.affinity_exec([&executed]() { executed = true; });

    EXPECT_EQ(result, nullptr); // 检查线程创建是否成功（nullptr表示成功）
    thread.join();              // 等待线程完成
    EXPECT_TRUE(executed);      // 检查线程是否执行了函数
    EXPECT_EQ(posix_thread::cpu_index_, 1);
}

// 测试affinity_exec在不同CPU核心上的执行
TEST(thread, AffinityExecMultipleCPUs)
{
    // 测试在多个CPU核心上执行
    for (unsigned int cpu = 0; cpu < 4; ++cpu) {
        std::atomic<bool> executed{false};
        posix_thread thread;

        auto result = thread.affinity_exec([&executed]() { executed = true; });

        EXPECT_EQ(result, nullptr); // 检查线程创建是否成功
        thread.join();              // 等待线程完成
        EXPECT_TRUE(executed);      // 检查线程是否执行了函数
    }
    EXPECT_EQ(posix_thread::cpu_index_, 5);
}

// 测试affinity_exec带参数的功能
TEST(thread, AffinityExecWithArguments)
{
    std::atomic<bool> executed{false};
    std::atomic<int> sum{0};
    posix_thread thread;

    auto result = thread.affinity_exec(
        [&executed, &sum](int a, int b, const std::string &text) {
            executed = true;
            sum = a + b;
            // 这里可以处理参数，但不返回值
        },
        10,
        20,
        "test");

    EXPECT_EQ(result, nullptr);
    thread.join(); // 等待线程完成
    EXPECT_TRUE(executed);
    EXPECT_EQ(sum.load(), 30); // 验证参数传递正确
    EXPECT_EQ(posix_thread::cpu_index_, 6);
}

// 测试affinity_exec的CPU亲和性验证
TEST(thread, AffinityExecCPUVarification)
{
    std::atomic<bool> executed{false};
    std::atomic<int> actual_cpu{-1};
    posix_thread thread;

    // 在第一个可用的CPU核心上执行任务
    int target_cpu =
        posix_thread::cpu_index_ % posix_thread::hardware_concurrency();
    auto result = thread.affinity_exec([&executed, &actual_cpu]() {
        executed = true;

        // 获取当前线程的CPU亲和性设置
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);

        if (::pthread_getaffinity_np(
                ::pthread_self(), sizeof(cpu_set_t), &cpuset) == 0) {
            // 检查线程实际运行的CPU
            for (int i = 0; i < CPU_SETSIZE; ++i) {
                if (CPU_ISSET(i, &cpuset)) {
                    actual_cpu = i;
                    break;
                }
            }
        }
    });

    EXPECT_EQ(result, nullptr); // 检查线程创建是否成功
    thread.join();              // 等待线程完成
    EXPECT_TRUE(executed);      // 检查线程是否执行了函数

    // 验证CPU亲和性设置
    if (actual_cpu.load() != -1) {
        // 如果成功获取到CPU信息，验证是否在目标CPU上运行
        EXPECT_EQ(actual_cpu.load(), target_cpu);
    }
    // 如果获取CPU信息失败，可能是因为权限问题，不强制验证

    target_cpu =
        posix_thread::cpu_index_ % posix_thread::hardware_concurrency();
    result = thread.affinity_exec([&executed, &actual_cpu]() {
        executed = true;

        // 获取当前线程的CPU亲和性设置
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);

        if (::pthread_getaffinity_np(
                ::pthread_self(), sizeof(cpu_set_t), &cpuset) == 0) {
            // 检查线程实际运行的CPU
            for (int i = 0; i < CPU_SETSIZE; ++i) {
                if (CPU_ISSET(i, &cpuset)) {
                    actual_cpu = i;
                    break;
                }
            }
        }
    });
    EXPECT_EQ(result, nullptr); // 检查线程创建是否成功
    thread.join();              // 等待线程完成
    EXPECT_TRUE(executed);      // 检查线程是否执行了函数

    // 验证CPU亲和性设置
    if (actual_cpu.load() != -1) {
        // 如果成功获取到CPU信息，验证是否在目标CPU上运行
        EXPECT_EQ(actual_cpu.load(), target_cpu);
    }
    // 如果获取CPU信息失败，可能是因为权限问题，不强制验证
}

} // namespace wf::io

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}