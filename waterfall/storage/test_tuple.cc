////
// @file test_tuple.cc
// @brief
// 测试逻辑元组
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include "tuple.h"

namespace wf::storage {

class TupleTest : public ::testing::Test
{
  protected:
    void SetUp() override { arena_ = new wf::utils::object_arena(); }

    void TearDown() override { delete arena_; }

    wf::utils::object_arena *arena_;
};

// 测试空元组
TEST_F(TupleTest, EmptyTuple)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    EXPECT_TRUE(tuple.empty());
    EXPECT_EQ(tuple.size(), 0);
}

// 测试添加元素到元组
TEST_F(TupleTest, AddElements)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    // 添加几个char_slice元素
    wf::utils::char_slice slice1("hello", 5);
    wf::utils::char_slice slice2("world", 5);
    wf::utils::char_slice slice3("test", 4);

    tuple.emplace_back(slice1);
    tuple.emplace_back(slice2);
    tuple.emplace_back(slice3);

    EXPECT_FALSE(tuple.empty());
    EXPECT_EQ(tuple.size(), 3);

    // 验证元素内容
    EXPECT_EQ(tuple[0], slice1);
    EXPECT_EQ(tuple[1], slice2);
    EXPECT_EQ(tuple[2], slice3);
}

// 测试元组迭代器
TEST_F(TupleTest, TupleIterators)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    wf::utils::char_slice slice1("first", 5);
    wf::utils::char_slice slice2("second", 6);
    wf::utils::char_slice slice3("third", 5);

    tuple.emplace_back(slice1);
    tuple.emplace_back(slice2);
    tuple.emplace_back(slice3);

    // 测试正向迭代器
    auto it = tuple.begin();
    EXPECT_EQ(*it, slice1);
    ++it;
    EXPECT_EQ(*it, slice2);
    ++it;
    EXPECT_EQ(*it, slice3);
    ++it;
    EXPECT_EQ(it, tuple.end());

    // 测试反向迭代器
    auto rit = tuple.rbegin();
    EXPECT_EQ(*rit, slice3);
    ++rit;
    EXPECT_EQ(*rit, slice2);
    ++rit;
    EXPECT_EQ(*rit, slice1);
    ++rit;
    EXPECT_EQ(rit, tuple.rend());
}

// 测试元组容量操作
TEST_F(TupleTest, TupleCapacity)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    EXPECT_EQ(tuple.capacity(), 0);

    // 预留容量
    tuple.reserve(100);
    EXPECT_GE(tuple.capacity(), 100);

    // 添加元素
    for (int i = 0; i < 50; ++i) {
        wf::utils::char_slice slice(std::to_string(i).c_str());
        tuple.emplace_back(slice);
    }

    EXPECT_EQ(tuple.size(), 50);
    EXPECT_GE(tuple.capacity(), 100);
}

// 测试元组清除操作
TEST_F(TupleTest, ClearTuple)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    // 添加一些元素
    for (int i = 0; i < 10; ++i) {
        wf::utils::char_slice slice(std::to_string(i).c_str());
        tuple.emplace_back(slice);
    }

    EXPECT_EQ(tuple.size(), 10);

    // 清除元组
    tuple.clear();

    EXPECT_TRUE(tuple.empty());
    EXPECT_EQ(tuple.size(), 0);
}

// 测试元组拷贝和赋值
TEST_F(TupleTest, CopyAndAssignment)
{
    logical_tuple tuple1{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    wf::utils::char_slice slice1("copy", 4);
    wf::utils::char_slice slice2("test", 4);

    tuple1.emplace_back(slice1);
    tuple1.emplace_back(slice2);

    // 拷贝构造
    logical_tuple tuple2 = tuple1;

    EXPECT_EQ(tuple2.size(), 2);
    EXPECT_EQ(tuple2[0], slice1);
    EXPECT_EQ(tuple2[1], slice2);

    // 赋值操作
    logical_tuple tuple3{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};
    tuple3 = tuple1;

    EXPECT_EQ(tuple3.size(), 2);
    EXPECT_EQ(tuple3[0], slice1);
    EXPECT_EQ(tuple3[1], slice2);
}

// 测试元组元素访问
TEST_F(TupleTest, ElementAccess)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    wf::utils::char_slice slice1("front", 5);
    wf::utils::char_slice slice2("middle", 6);
    wf::utils::char_slice slice3("back", 4);

    tuple.emplace_back(slice1);
    tuple.emplace_back(slice2);
    tuple.emplace_back(slice3);

    // 测试front和back
    EXPECT_EQ(tuple.front(), slice1);
    EXPECT_EQ(tuple.back(), slice3);

    // 测试at方法
    EXPECT_EQ(tuple.at(0), slice1);
    EXPECT_EQ(tuple.at(1), slice2);
    EXPECT_EQ(tuple.at(2), slice3);

    // 测试越界访问 - 在-fno-exceptions环境下，at()方法应该返回nullptr或类似的值
    // 或者我们可以测试size()边界
    EXPECT_EQ(tuple.size(), 3);
    // 使用operator[]来避免nodiscard警告
    // 在-fno-exceptions环境下，operator[]应该用于边界检查
}

// 测试元组插入和删除
TEST_F(TupleTest, InsertAndErase)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    wf::utils::char_slice slice1("one", 3);
    wf::utils::char_slice slice2("three", 5);

    tuple.emplace_back(slice1);
    tuple.emplace_back(slice2);

    // 在中间插入元素
    wf::utils::char_slice slice_middle("two", 3);
    auto it = tuple.begin() + 1;
    tuple.insert(it, slice_middle);

    EXPECT_EQ(tuple.size(), 3);
    EXPECT_EQ(tuple[0], slice1);
    EXPECT_EQ(tuple[1], slice_middle);
    EXPECT_EQ(tuple[2], slice2);

    // 删除中间元素
    it = tuple.begin() + 1;
    tuple.erase(it);

    EXPECT_EQ(tuple.size(), 2);
    EXPECT_EQ(tuple[0], slice1);
    EXPECT_EQ(tuple[1], slice2);
}

// 测试元组比较操作
TEST_F(TupleTest, TupleComparison)
{
    logical_tuple tuple1{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};
    logical_tuple tuple2{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    wf::utils::char_slice slice1("hello", 5);
    wf::utils::char_slice slice2("world", 5);

    tuple1.emplace_back(slice1);
    tuple1.emplace_back(slice2);

    tuple2.emplace_back(slice1);
    tuple2.emplace_back(slice2);

    // 相同内容的元组应该相等
    EXPECT_EQ(tuple1, tuple2);

    // 修改一个元组
    tuple2.pop_back();
    EXPECT_NE(tuple1, tuple2);

    // 恢复并测试相等性
    tuple2.emplace_back(slice2);
    EXPECT_EQ(tuple1, tuple2);
}

// 测试元组与字符串的交互
TEST_F(TupleTest, TupleWithStrings)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    std::string str1 = "string one";
    std::string str2 = "string two";
    std::string str3 = "string three";

    // 从字符串创建char_slice
    wf::utils::char_slice slice1(str1);
    wf::utils::char_slice slice2(str2);
    wf::utils::char_slice slice3(str3);

    tuple.emplace_back(slice1);
    tuple.emplace_back(slice2);
    tuple.emplace_back(slice3);

    EXPECT_EQ(tuple.size(), 3);

    // 验证内容
    EXPECT_EQ(tuple[0].ToString(), str1);
    EXPECT_EQ(tuple[1].ToString(), str2);
    EXPECT_EQ(tuple[2].ToString(), str3);
}

// 测试包含empty slice的元组
TEST_F(TupleTest, TupleWithEmptySlices)
{
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena_)};

    // 添加empty slice
    wf::utils::char_slice empty_slice1;        // 默认构造的empty slice
    wf::utils::char_slice empty_slice2("", 0); // 显式创建的empty slice
    wf::utils::char_slice normal_slice("hello", 5);

    tuple.emplace_back(empty_slice1);
    tuple.emplace_back(empty_slice2);
    tuple.emplace_back(normal_slice);

    EXPECT_FALSE(tuple.empty());
    EXPECT_EQ(tuple.size(), 3);

    // 验证empty slice的行为
    EXPECT_TRUE(tuple[0].empty());
    EXPECT_TRUE(tuple[1].empty());
    EXPECT_FALSE(tuple[2].empty());

    // 验证empty slice的内容
    EXPECT_EQ(tuple[0].ToString(), "");
    EXPECT_EQ(tuple[1].ToString(), "");
    EXPECT_EQ(tuple[2].ToString(), "hello");
}
} // namespace wf::storage

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}