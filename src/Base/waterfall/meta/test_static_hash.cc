////
// @file test_static_hash.cc
// @brief
// 测试静态哈希函数
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <string>
#include <array>
#include "static_hash.h"

namespace wf::meta {

// 测试字符串哈希函数
TEST(StaticHashTest, StringHash)
{
    // 编译期字符串哈希
    constexpr auto hash1 = hash_string("hello world");
    constexpr auto hash2 = hash_string("hello world");
    constexpr auto hash3 = hash_string("hello world!");

    // 相同字符串应该产生相同哈希值
    EXPECT_EQ(hash1, hash2);

    // 不同字符串应该产生不同哈希值（理论上）
    EXPECT_NE(hash1, hash3);

    // 哈希值不应为0
    EXPECT_NE(hash1, 0u);
    EXPECT_NE(hash2, 0u);
    EXPECT_NE(hash3, 0u);
}

// 测试字符串视图哈希函数
TEST(StaticHashTest, StringViewHash)
{
    constexpr std::string_view str1 = "test string";
    constexpr std::string_view str2 = "test string";
    constexpr std::string_view str3 = "different string";

    constexpr auto hash1 = hash_string_view(str1);
    constexpr auto hash2 = hash_string_view(str2);
    constexpr auto hash3 = hash_string_view(str3);

    // 相同内容应该产生相同哈希值
    EXPECT_EQ(hash1, hash2);

    // 不同内容应该产生不同哈希值（理论上）
    EXPECT_NE(hash1, hash3);

    // 哈希值不应为0
    EXPECT_NE(hash1, 0u);
}

// 测试字节数组哈希函数
TEST(StaticHashTest, BytesHash)
{
    constexpr uint8_t bytes1[] = {0x01, 0x02, 0x03, 0x04};
    constexpr uint8_t bytes2[] = {0x01, 0x02, 0x03, 0x04};
    constexpr uint8_t bytes3[] = {0x01, 0x02, 0x03, 0x05};

    constexpr auto hash1 = hash_bytes(bytes1);
    constexpr auto hash2 = hash_bytes(bytes2);
    constexpr auto hash3 = hash_bytes(bytes3);

    // 相同字节数组应该产生相同哈希值
    EXPECT_EQ(hash1, hash2);

    // 不同字节数组应该产生不同哈希值（理论上）
    EXPECT_NE(hash1, hash3);

    // 哈希值不应为0
    EXPECT_NE(hash1, 0u);
}

// 测试整数哈希函数
TEST(StaticHashTest, IntegerHash)
{
    // 运行时整数哈希（因为reinterpret_cast在编译期不可用）
    auto hash1 = hash_integer(42);
    auto hash2 = hash_integer(42);
    auto hash3 = hash_integer(43);

    // 相同整数应该产生相同哈希值
    EXPECT_EQ(hash1, hash2);

    // 不同整数应该产生不同哈希值（理论上）
    EXPECT_NE(hash1, hash3);

    // 哈希值不应为0
    EXPECT_NE(hash1, 0u);
}

// 测试编译期安全的整数哈希函数（使用字符串表示）
TEST(StaticHashTest, IntegerHashCompileTime)
{
    // 对于编译期使用，可以将整数转换为字符串进行哈希
    constexpr auto hash1 = hash_string("42");
    constexpr auto hash2 = hash_string("42");
    constexpr auto hash3 = hash_string("43");

    // 相同字符串应该产生相同哈希值
    EXPECT_EQ(hash1, hash2);

    // 不同字符串应该产生不同哈希值
    EXPECT_NE(hash1, hash3);

    // 哈希值不应为0
    EXPECT_NE(hash1, 0u);
}

// 测试组合哈希函数
TEST(StaticHashTest, CombineHashes)
{
    constexpr auto hash1 = hash_string("hello");
    constexpr auto hash2 = hash_string("world");

    constexpr auto combined1 = combine_hashes(hash1, hash2);
    constexpr auto combined2 = combine_hashes(hash1, hash2);
    constexpr auto combined3 = combine_hashes(hash2, hash1);

    // 相同组合应该产生相同哈希值
    EXPECT_EQ(combined1, combined2);

    // 不同顺序的组合应该产生不同哈希值
    EXPECT_NE(combined1, combined3);

    // 哈希值不应为0
    EXPECT_NE(combined1, 0u);
}

// 测试带种子的哈希函数
TEST(StaticHashTest, SeededHash)
{
    constexpr auto seed1 = 12345u;
    constexpr auto seed2 = 67890u;

    constexpr auto hash1 = hash_string_seeded("test", seed1);
    constexpr auto hash2 = hash_string_seeded("test", seed1);
    constexpr auto hash3 = hash_string_seeded("test", seed2);

    // 相同种子和字符串应该产生相同哈希值
    EXPECT_EQ(hash1, hash2);

    // 不同种子应该产生不同哈希值
    EXPECT_NE(hash1, hash3);

    // 哈希值不应为0
    EXPECT_NE(hash1, 0u);
}

// 测试哈希函数的分布特性
TEST(StaticHashTest, HashDistribution)
{
    // 测试相似字符串的哈希值分布
    constexpr auto hash1 = hash_string("hello");
    constexpr auto hash2 = hash_string("hello!");
    constexpr auto hash3 = hash_string("hello!!");

    // 相似但不同的字符串应该产生不同的哈希值
    EXPECT_NE(hash1, hash2);
    EXPECT_NE(hash1, hash3);
    EXPECT_NE(hash2, hash3);
}

// 测试运行时使用
TEST(StaticHashTest, RuntimeUsage)
{
    // 运行时字符串哈希
    std::string runtime_str = "runtime string";
    auto runtime_hash = hash_string(runtime_str.c_str());

    // 编译期相同字符串哈希
    constexpr auto compile_hash = hash_string("runtime string");

    // 运行时和编译期应该产生相同哈希值
    EXPECT_EQ(runtime_hash, compile_hash);

    // 哈希值不应为0
    EXPECT_NE(runtime_hash, 0u);
}

// 测试哈希冲突概率（基本测试）
TEST(StaticHashTest, HashCollision)
{
    // 测试一组不同的字符串，期望没有哈希冲突
    constexpr std::array<const char *, 10> test_strings = {
        "hello",
        "world",
        "test",
        "string",
        "hash",
        "function",
        "compile",
        "time",
        "static",
        "coir"};

    std::array<uint64_t, 10> hashes;
    for (size_t i = 0; i < test_strings.size(); ++i) {
        hashes[i] = hash_string(test_strings[i]);
    }

    // 检查是否有重复的哈希值
    for (size_t i = 0; i < hashes.size(); ++i) {
        for (size_t j = i + 1; j < hashes.size(); ++j) {
            EXPECT_NE(hashes[i], hashes[j])
                << "Hash collision detected for: '" << test_strings[i]
                << "' and '" << test_strings[j] << "'";
        }
    }
}

// 测试数组哈希函数
TEST(StaticHashTest, ArrayHash)
{
    // 编译期数组哈希
    constexpr std::array<uint8_t, 4> arr1 = {0x01, 0x02, 0x03, 0x04};
    constexpr std::array<uint8_t, 4> arr2 = {0x01, 0x02, 0x03, 0x04};
    constexpr std::array<uint8_t, 4> arr3 = {0x01, 0x02, 0x03, 0x05};
    constexpr std::array<uint8_t, 5> arr4 = {0x01, 0x02, 0x03, 0x04, 0x05};

    constexpr auto hash1 = hash_array(arr1);
    constexpr auto hash2 = hash_array(arr2);
    constexpr auto hash3 = hash_array(arr3);
    constexpr auto hash4 = hash_array(arr4);

    // 相同数组应该产生相同哈希值
    EXPECT_EQ(hash1, hash2);

    // 不同内容应该产生不同哈希值（理论上）
    EXPECT_NE(hash1, hash3);

    // 不同长度应该产生不同哈希值（理论上）
    EXPECT_NE(hash1, hash4);

    // 哈希值不应为0
    EXPECT_NE(hash1, 0u);
    EXPECT_NE(hash2, 0u);
    EXPECT_NE(hash3, 0u);
    EXPECT_NE(hash4, 0u);
}

// 测试不同数据类型的数组哈希
TEST(StaticHashTest, ArrayHashDifferentTypes)
{
    // 测试不同数据类型的数组
    constexpr std::array<int, 3> int_arr = {1, 2, 3};
    constexpr std::array<char, 3> char_arr = {1, 2, 3};
    constexpr std::array<uint8_t, 3> byte_arr = {1, 2, 3};

    constexpr auto int_hash = hash_array(int_arr);
    constexpr auto char_hash = hash_array(char_arr);
    constexpr auto byte_hash = hash_array(byte_arr);

    // 相同字节内容但不同类型应该产生相同哈希值
    EXPECT_EQ(int_hash, char_hash);
    EXPECT_EQ(int_hash, byte_hash);

    // 哈希值不应为0
    EXPECT_NE(int_hash, 0u);
}

// 测试空数组哈希
TEST(StaticHashTest, EmptyArrayHash)
{
    constexpr std::array<uint8_t, 0> empty_arr = {};
    constexpr auto empty_hash = hash_array(empty_arr);

    // 空数组应该产生有效的哈希值（基础值）
    EXPECT_NE(empty_hash, 0u);

    // 空数组的哈希值应该等于FNV基础值
    EXPECT_EQ(empty_hash, 14695981039346656037ULL);
}

// 测试数组哈希与字节数组哈希的一致性
TEST(StaticHashTest, ArrayHashConsistency)
{
    constexpr std::array<uint8_t, 4> arr = {0x01, 0x02, 0x03, 0x04};
    constexpr uint8_t bytes[] = {0x01, 0x02, 0x03, 0x04};

    constexpr auto arr_hash = hash_array(arr);
    constexpr auto bytes_hash = hash_bytes(bytes);

    // 相同内容的数组和字节数组应该产生相同哈希值
    EXPECT_EQ(arr_hash, bytes_hash);
}

// 测试运行时数组哈希
TEST(StaticHashTest, RuntimeArrayHash)
{
    // 运行时数组哈希
    std::array<uint8_t, 4> runtime_arr = {0x10, 0x20, 0x30, 0x40};
    auto runtime_hash = hash_array(runtime_arr);

    // 编译期相同数组哈希
    constexpr std::array<uint8_t, 4> compile_arr = {0x10, 0x20, 0x30, 0x40};
    constexpr auto compile_hash = hash_array(compile_arr);

    // 运行时和编译期应该产生相同哈希值
    EXPECT_EQ(runtime_hash, compile_hash);

    // 哈希值不应为0
    EXPECT_NE(runtime_hash, 0u);
}

} // namespace wf::meta

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}