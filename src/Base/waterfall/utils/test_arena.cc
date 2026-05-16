////
// @file test_arena.cc
// @brief
// 测试arena
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include "arena.h"
#include <cstring>
#include <vector>
#include <string>
#include <memory>

namespace wf::utils {

class ArenaTest : public ::testing::Test
{
  protected:
    void SetUp() override { arena_ = new object_arena(); }

    void TearDown() override { delete arena_; }

    object_arena *arena_;
};

// 测试基础内存分配
TEST_F(ArenaTest, BasicAllocation)
{
    char *p1 = arena_->allocate(10);
    char *p2 = arena_->allocate(20);

    EXPECT_NE(p1, nullptr);
    EXPECT_NE(p2, nullptr);
    EXPECT_NE(p1, p2); // 确保分配的内存地址不同

    // 测试内存可写
    std::strcpy(p1, "test1");
    std::strcpy(p2, "test2");

    EXPECT_STREQ(p1, "test1");
    EXPECT_STREQ(p2, "test2");
}

// 测试连续分配
TEST_F(ArenaTest, SequentialAllocation)
{
    const int num_allocations = 100;
    char *pointers[num_allocations];

    for (int i = 0; i < num_allocations; ++i) {
        pointers[i] = arena_->allocate(10);
        EXPECT_NE(pointers[i], nullptr);

        // 写入数据验证
        std::sprintf(pointers[i], "data%d", i);
    }

    // 验证数据完整性
    for (int i = 0; i < num_allocations; ++i) {
        char expected[10];
        std::sprintf(expected, "data%d", i);
        EXPECT_STREQ(pointers[i], expected);
    }
}

// 测试大内存分配（大于块大小的1/4）
TEST_F(ArenaTest, LargeAllocation)
{
    const size_t large_size = 2048; // 大于4096/4=1024
    char *large_block = arena_->allocate(large_size);

    EXPECT_NE(large_block, nullptr);

    // 填充数据验证
    std::memset(large_block, 0xAB, large_size);
    for (size_t i = 0; i < large_size; ++i) {
        EXPECT_EQ(large_block[i], static_cast<char>(0xAB));
    }
}

// 测试对齐分配
TEST_F(ArenaTest, AlignedAllocation)
{
    char *aligned_ptr = arena_->aligned_allocate(64);
    EXPECT_NE(aligned_ptr, nullptr);

    // 验证对齐（应该对齐到指针大小或8字节，取较大者）
    uintptr_t addr = reinterpret_cast<uintptr_t>(aligned_ptr);
    size_t align = (sizeof(void *) > 8) ? sizeof(void *) : 8;
    EXPECT_EQ(addr & (align - 1), 0u);
}

// 测试内存使用统计
TEST_F(ArenaTest, MemoryUsage)
{
    size_t initial_usage = arena_->usage();
    EXPECT_EQ(initial_usage, 0u); // 初始使用量应该为0

    // 分配大内存（大于1024字节，直接分配）
    arena_->allocate(1500);
    size_t after_first_alloc = arena_->usage();
    EXPECT_GT(after_first_alloc, initial_usage);

    // 验证内存使用量 = 1500 + sizeof(char*)
    EXPECT_GE(after_first_alloc, 1500 + sizeof(char *));
    EXPECT_EQ(after_first_alloc % kArenaAlignment, 0u);

    // 再分配大内存（大于1024字节，直接分配）
    arena_->allocate(2000);
    size_t after_second_alloc = arena_->usage();
    EXPECT_GT(after_second_alloc, after_first_alloc);

    // 验证内存使用量 = 1500 + 2000 + 2*sizeof(char*)
    EXPECT_GE(after_second_alloc, 1500 + 2000 + 2 * sizeof(char *));
    EXPECT_EQ(after_second_alloc % kArenaAlignment, 0u);

    // 分配小内存（小于1024字节，会分配新块）
    arena_->allocate(100);
    size_t after_small_alloc = arena_->usage();
    EXPECT_GT(after_small_alloc, after_second_alloc);
    // 验证内存使用量 = 1500 + 2000 + 4096 + 3*sizeof(char*)
    EXPECT_GE(after_small_alloc, 1500 + 2000 + 4096 + 3 * sizeof(char *));
    EXPECT_EQ(after_small_alloc % kArenaAlignment, 0u);
}

// 测试精确的内存使用统计
TEST_F(ArenaTest, ExactMemoryUsage)
{
    // 分配大内存块（大于块大小的1/4）
    size_t large_size1 = 2048;
    arena_->allocate(large_size1);
    size_t usage_after_large1 = arena_->usage();

    // 应该等于 large_size1 + sizeof(char*)
    EXPECT_EQ(usage_after_large1, large_size1 + sizeof(char *));

    // 再分配大内存块
    size_t large_size2 = 3000;
    arena_->allocate(large_size2);
    size_t usage_after_large2 = arena_->usage();

    // 应该等于 large_size1 + large_size2 + 2*sizeof(char*)
    EXPECT_EQ(
        usage_after_large2, large_size1 + large_size2 + 2 * sizeof(char *));

    // 分配小内存（会分配新块）
    arena_->allocate(500);
    size_t usage_after_small = arena_->usage();

    // 应该等于之前的用量 + 4096 + sizeof(char*)
    EXPECT_EQ(usage_after_small, usage_after_large2 + 4096 + sizeof(char *));
}

// 测试混合分配（小内存和大内存）
TEST_F(ArenaTest, MixedAllocations)
{
    // 分配一些小内存
    char *small1 = arena_->allocate(10);
    char *small2 = arena_->allocate(20);
    char *small3 = arena_->allocate(30);

    // 分配一个大内存
    char *large = arena_->allocate(1500);

    EXPECT_NE(small1, nullptr);
    EXPECT_NE(small2, nullptr);
    EXPECT_NE(small3, nullptr);
    EXPECT_NE(large, nullptr);

    // 验证所有内存都可写
    std::strcpy(small1, "small1");
    std::strcpy(small2, "small2");
    std::strcpy(small3, "small3");
    std::memset(large, 0xCD, 1500);

    EXPECT_STREQ(small1, "small1");
    EXPECT_STREQ(small2, "small2");
    EXPECT_STREQ(small3, "small3");

    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(large[i], static_cast<char>(0xCD));
    }
}

// 测试多次创建和销毁arena
TEST_F(ArenaTest, MultipleArenas)
{
    // 创建第一个arena并分配内存
    object_arena arena1;
    char *p1 = arena1.allocate(100);
    EXPECT_NE(p1, nullptr);

    // 创建第二个arena并分配内存
    object_arena arena2;
    char *p2 = arena2.allocate(100);
    EXPECT_NE(p2, nullptr);

    // 两个arena分配的内存应该不同
    EXPECT_NE(p1, p2);
}

// arena_allocator测试类
class ArenaAllocatorTest : public ::testing::Test
{
  protected:
    void SetUp() override { arena_ = new object_arena(); }

    void TearDown() override { delete arena_; }

    object_arena *arena_;
};

// 测试基础分配器功能
TEST_F(ArenaAllocatorTest, BasicAllocator)
{
    arena_allocator<int> alloc(arena_);

    // 分配内存
    int *ptr = alloc.allocate(10);
    EXPECT_NE(ptr, nullptr);

    // 构造对象
    for (int i = 0; i < 10; ++i) {
        alloc.construct(ptr + i, i * 10);
    }

    // 验证对象
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(ptr[i], i * 10);
    }

    // 销毁对象
    for (int i = 0; i < 10; ++i) {
        alloc.destroy(ptr + i);
    }

    // 释放内存（arena分配器通常不释放单个对象）
    alloc.deallocate(ptr, 10);
}

// 测试vector使用arena_allocator
TEST_F(ArenaAllocatorTest, VectorWithArenaAllocator)
{
    using ArenaVector = std::vector<int, arena_allocator<int>>;

    arena_allocator<int> alloc(arena_);
    ArenaVector vec(alloc);

    // 测试push_back
    for (int i = 0; i < 100; ++i) {
        vec.push_back(i);
        EXPECT_EQ(vec[i], i);
    }

    // 测试size和capacity
    EXPECT_EQ(vec.size(), 100);
    EXPECT_GE(vec.capacity(), 100);

    // 测试迭代器
    int expected = 0;
    for (auto it = vec.begin(); it != vec.end(); ++it, ++expected) {
        EXPECT_EQ(*it, expected);
    }

    // 测试clear
    vec.clear();
    EXPECT_EQ(vec.size(), 0);
    EXPECT_TRUE(vec.empty());
}

// 测试string使用arena_allocator
TEST_F(ArenaAllocatorTest, StringWithArenaAllocator)
{
    using ArenaString =
        std::basic_string<char, std::char_traits<char>, arena_allocator<char>>;

    arena_allocator<char> alloc(arena_);

    // 使用带有分配器的构造函数
    ArenaString str(alloc);

    // 测试字符串操作 - 使用更明确的字符串
    str = "Hello";
    EXPECT_EQ(str, "Hello");
    EXPECT_EQ(str.length(), 5);

    str += ", World!";
    EXPECT_EQ(str, "Hello, World!");
    EXPECT_EQ(str.length(), 13);

    str += " Test";
    EXPECT_EQ(str, "Hello, World! Test");
    EXPECT_EQ(str.length(), 18);

    // 测试子字符串
    ArenaString sub(str, 0, 5, alloc); // 使用明确的构造函数
    EXPECT_EQ(sub, "Hello");
    EXPECT_EQ(sub.length(), 5);
}

// 测试分配器比较操作
TEST_F(ArenaAllocatorTest, AllocatorComparison)
{
    arena_allocator<int> alloc1(arena_);
    arena_allocator<int> alloc2(arena_);
    arena_allocator<double> alloc3(arena_);

    // 相同arena的分配器应该相等
    EXPECT_TRUE(alloc1 == alloc2);
    EXPECT_FALSE(alloc1 != alloc2);

    // 不同类型但相同arena的分配器应该相等
    EXPECT_TRUE(alloc1 == alloc3);
    EXPECT_FALSE(alloc1 != alloc3);

    // 测试不同arena的分配器
    object_arena arena2;
    arena_allocator<int> alloc4(&arena2);

    EXPECT_FALSE(alloc1 == alloc4);
    EXPECT_TRUE(alloc1 != alloc4);
}

// 测试分配器rebind功能
TEST_F(ArenaAllocatorTest, AllocatorRebind)
{
    arena_allocator<int> int_alloc(arena_);

    // 测试rebind到double
    arena_allocator<int>::rebind<double>::other double_alloc(int_alloc);

    double *ptr = double_alloc.allocate(5);
    EXPECT_NE(ptr, nullptr);

    for (int i = 0; i < 5; ++i) {
        double_alloc.construct(ptr + i, i * 1.5);
        EXPECT_EQ(ptr[i], i * 1.5);
    }

    for (int i = 0; i < 5; ++i) {
        double_alloc.destroy(ptr + i);
    }

    double_alloc.deallocate(ptr, 5);
}

// 测试大容量vector分配
TEST_F(ArenaAllocatorTest, LargeVectorAllocation)
{
    using ArenaVector = std::vector<int, arena_allocator<int>>;

    arena_allocator<int> alloc(arena_);
    ArenaVector vec(alloc);

    const size_t large_size = 10000;

    // 预分配大容量
    vec.reserve(large_size);
    EXPECT_GE(vec.capacity(), large_size);

    // 填充数据
    for (size_t i = 0; i < large_size; ++i) {
        vec.push_back(static_cast<int>(i));
    }

    EXPECT_EQ(vec.size(), large_size);

    // 验证数据完整性
    for (size_t i = 0; i < large_size; ++i) {
        EXPECT_EQ(vec[i], static_cast<int>(i));
    }
}

// 测试混合类型容器
TEST_F(ArenaAllocatorTest, MixedTypeContainers)
{
    // 测试vector of strings
    using ArenaString =
        std::basic_string<char, std::char_traits<char>, arena_allocator<char>>;
    using ArenaStringVector =
        std::vector<ArenaString, arena_allocator<ArenaString>>;

    arena_allocator<char> char_alloc(arena_);
    arena_allocator<ArenaString> string_alloc(arena_);

    ArenaStringVector strings(string_alloc);

    // 添加多个字符串
    for (int i = 0; i < 10; ++i) {
        ArenaString str(char_alloc); // 使用明确的构造函数
        str = "String " + std::to_string(i);
        strings.push_back(str);
    }

    EXPECT_EQ(strings.size(), 10);

    // 验证字符串内容
    for (int i = 0; i < 10; ++i) {
        ArenaString expected(char_alloc); // 使用明确的构造函数
        expected = "String " + std::to_string(i);
        EXPECT_EQ(strings[i], expected);
    }
}

// 测试内存使用统计
TEST_F(ArenaAllocatorTest, MemoryUsageWithAllocator)
{
    // 移除未使用的变量
    // size_t initial_usage = arena_->usage();

    using ArenaVector = std::vector<int, arena_allocator<int>>;
    arena_allocator<int> alloc(arena_);
    ArenaVector vec(alloc);

    // 分配前内存使用量
    size_t before_allocation = arena_->usage();

    // 分配大量数据
    const size_t element_count = 1000;
    vec.resize(element_count);

    size_t after_allocation = arena_->usage();

    // 内存使用量应该增加
    EXPECT_GT(after_allocation, before_allocation);

    // 填充数据
    for (size_t i = 0; i < element_count; ++i) {
        vec[i] = static_cast<int>(i);
    }

    // 验证数据
    for (size_t i = 0; i < element_count; ++i) {
        EXPECT_EQ(vec[i], static_cast<int>(i));
    }
}

// 测试分配失败情况（不抛出异常，返回nullptr）
TEST_F(ArenaAllocatorTest, AllocationFailure)
{
    arena_allocator<int> alloc(arena_);

    // 测试超大分配（超过max_size）
    size_t huge_size = std::numeric_limits<size_t>::max() / sizeof(int) + 1;
    int *ptr = alloc.allocate(huge_size);

    // 应该返回nullptr而不是抛出异常
    EXPECT_EQ(ptr, nullptr);

    // 测试零分配
    int *zero_ptr = alloc.allocate(0);
    EXPECT_EQ(zero_ptr, nullptr);
}

// 测试STL容器在分配失败时的行为
TEST_F(ArenaAllocatorTest, STLContainerAllocationFailure)
{
    using ArenaVector = std::vector<int, arena_allocator<int>>;
    arena_allocator<int> alloc(arena_);
    ArenaVector vec(alloc);

    // 正常分配应该成功
    vec.reserve(100);
    EXPECT_GE(vec.capacity(), 100);

    // 填充一些数据
    for (int i = 0; i < 100; ++i) {
        vec.push_back(i);
    }

    EXPECT_EQ(vec.size(), 100);

    // 测试vector在分配失败时的行为（不会抛出异常）
    // 注意：实际使用中，STL容器可能会在内部处理分配失败
    // 这里主要验证我们的分配器不会抛出异常
}

} // namespace wf::utils

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}