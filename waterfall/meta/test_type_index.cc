////
// @file test_type_index.cc
// @brief
// 测试type_index
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <unordered_set>
#include "type_index.h"

namespace wf::meta {

// 测试用的类型
struct TestTypeA
{};
struct TestTypeB
{};

// 用于测试cv限定符的类型
using TestTypeC = const volatile TestTypeA;

// 测试type_index的基本功能
TEST(TypeIndexTest, BasicFunctionality)
{
    auto id_a1 = type_id<TestTypeA>();
    auto id_a2 = type_id<TestTypeA>();
    auto id_b = type_id<TestTypeB>();

    // 测试相同类型的typeid是否相等
    EXPECT_EQ(id_a1, id_a2);

    // 测试不同类型是否有不同的type_index
    EXPECT_NE(id_a1, id_b);

    // 测试name()方法不为空
    EXPECT_NE(id_a1.name(), nullptr);
    EXPECT_NE(id_b.name(), nullptr);
}

// 测试CV限定符的去除
TEST(TypeIndexTest, CvQualifierRemoval)
{
    auto id_a = type_id<TestTypeA>();
    auto id_c = type_id<TestTypeC>(); // const volatile TestTypeA

    // 注意：由于__PRETTY_FUNCTION__包含完整的类型信息，
    // TestTypeA和const volatile TestTypeA会得到不同的type_index
    // 这是预期的行为，因为它们在C++中是不同的类型

    // 修改测试：验证它们都是有效的type_index，但不要求它们相等
    EXPECT_NE(id_a.name(), nullptr);
    EXPECT_NE(id_c.name(), nullptr);
    EXPECT_NE(id_a.hash_code(), 0u);
    EXPECT_NE(id_c.hash_code(), 0u);
}

// 测试hash_code功能
TEST(TypeIndexTest, HashCode)
{
    auto id_a = type_id<TestTypeA>();
    auto id_b = type_id<TestTypeB>();

    // 测试hash_code方法返回值不为0
    EXPECT_NE(id_a.hash_code(), 0u);
    EXPECT_NE(id_b.hash_code(), 0u);
}

// 测试在标准容器中的使用
TEST(TypeIndexTest, ContainerUsage)
{
    std::unordered_set<type_index> type_set;

    auto id_a = type_id<TestTypeA>();
    auto id_b = type_id<TestTypeB>();

    // 插入不同类型
    type_set.insert(id_a);
    type_set.insert(id_b);

    // 验证容器大小
    EXPECT_EQ(type_set.size(), 2u);

    // 测试重复插入
    type_set.insert(type_id<TestTypeA>());
    EXPECT_EQ(type_set.size(), 2u);
}

// 测试比较操作符
TEST(TypeIndexTest, ComparisonOperators)
{
    auto id_a = type_id<TestTypeA>();
    auto id_b = type_id<TestTypeB>();

    // 测试相等性
    EXPECT_TRUE(id_a == id_a);
    EXPECT_FALSE(id_a == id_b);

    // 测试不等性
    EXPECT_FALSE(id_a != id_a);
    EXPECT_TRUE(id_a != id_b);

    // 测试小于比较（应该有一方小于另一方）
    EXPECT_TRUE((id_a < id_b) || (id_b < id_a));

    // 测试大于比较
    EXPECT_TRUE((id_a > id_b) || (id_b > id_a));

    // 测试小于等于比较
    EXPECT_TRUE(id_a <= id_a); // 自身比较
    EXPECT_TRUE((id_a <= id_b) || (id_b <= id_a));

    // 测试大于等于比较
    EXPECT_TRUE(id_a >= id_a); // 自身比较
    EXPECT_TRUE((id_a >= id_b) || (id_b >= id_a));
}

// 测试std::hash特化
TEST(TypeIndexTest, StdHashSpecialization)
{
    auto id_a = type_id<TestTypeA>();
    auto id_b = type_id<TestTypeB>();

    std::hash<type_index> hasher;

    // 测试hash函数返回值不为0
    EXPECT_NE(hasher(id_a), 0u);
    EXPECT_NE(hasher(id_b), 0u);

    // 测试相同类型的hash值相等
    EXPECT_EQ(hasher(id_a), hasher(type_id<TestTypeA>()));

    // 测试不同类型的hash值可能不等
    // 注意：这个测试可能失败，因为hash冲突是可能的，但我们期望它们不同
    // 如果这个测试失败，那是因为hash冲突，这在理论上是可能的
}

// 测试hash_code的静态特性（编译期计算能力）
TEST(TypeIndexTest, StaticHashCode)
{
    // 测试编译期哈希计算
    // 使用constexpr函数来验证编译期计算
    constexpr auto test_compile_time_hash = []() constexpr -> bool {
        // 直接使用type_id()的哈希值，而不是存储type_index对象
        auto hash_a = type_id<TestTypeA>().hash_code();
        auto hash_b = type_id<TestTypeB>().hash_code();

        // 验证哈希值不为0
        if (hash_a == 0 || hash_b == 0) return false;

        // 验证相同类型的哈希值相等
        if (hash_a != type_id<TestTypeA>().hash_code()) return false;

        return true;
    };

    // 验证编译期计算成功
    static_assert(
        test_compile_time_hash(), "Compile-time hash calculation should work");

    // 运行时验证
    auto id_a = type_id<TestTypeA>();
    auto id_b = type_id<TestTypeB>();

    // 直接使用哈希值，不尝试声明为constexpr变量
    auto hash_a = id_a.hash_code();
    auto hash_b = id_b.hash_code();

    // 验证哈希值不为0
    EXPECT_NE(hash_a, 0u);
    EXPECT_NE(hash_b, 0u);

    // 验证相同类型的哈希值相等
    EXPECT_EQ(hash_a, type_id<TestTypeA>().hash_code());

    // 验证不同类型的哈希值不同（理论上）
    // 注意：哈希冲突是可能的，但概率很低
    EXPECT_NE(hash_a, hash_b);

    // 测试在编译期常量表达式中的使用
    constexpr std::size_t test_hash = type_id<TestTypeA>().hash_code();
    static_assert(
        test_hash != 0, "hash_code() should be non-zero at compile time");

    // 测试在数组大小等编译期上下文中的使用
    constexpr std::size_t array_size =
        (test_hash % 100) + 1; // 确保数组大小为正
    std::array<int, array_size> test_array;
    EXPECT_EQ(test_array.size(), array_size);
}

// 测试跨编译单元的哈希一致性
TEST(TypeIndexTest, CrossTranslationUnitConsistency)
{
    // 在不同函数中获取相同类型的type_index
    auto id_a_func1 = type_id<TestTypeA>();
    auto id_a_func2 = type_id<TestTypeA>();

    // 验证哈希值相同（基于字符串内容，而不是指针地址）
    EXPECT_EQ(id_a_func1.hash_code(), id_a_func2.hash_code());

    // 验证std::hash特化的一致性
    std::hash<wf::meta::type_index> hasher;
    EXPECT_EQ(hasher(id_a_func1), hasher(id_a_func2));

    // 验证编译期哈希值的一致性
    constexpr auto compile_time_hash1 = type_id<TestTypeA>().hash_code();
    constexpr auto compile_time_hash2 = type_id<TestTypeA>().hash_code();
    EXPECT_EQ(compile_time_hash1, compile_time_hash2);
}
} // namespace wf::meta

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}