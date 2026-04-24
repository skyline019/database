////
// @file test_slice.cc
// @brief
// 测试slice.h
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include "slice.h"
#include "arena.h" // 添加arena.h包含
#include <string>

namespace wf::utils {

class SliceTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // 设置测试数据
        test_string = "Hello, World!";
        test_data = test_string.data();
        test_size = test_string.size();
    }

    std::string test_string;
    const char *test_data;
    size_t test_size;
};

// 测试默认构造函数
TEST_F(SliceTest, DefaultConstructor)
{
    char_slice slice;
    EXPECT_EQ(slice.data(), "");
    EXPECT_EQ(slice.size(), 0u);
    EXPECT_TRUE(slice.empty());
}

// 测试指针和大小构造函数
TEST_F(SliceTest, PointerSizeConstructor)
{
    char_slice slice(test_data, test_size);
    EXPECT_EQ(slice.data(), test_data);
    EXPECT_EQ(slice.size(), test_size);
    EXPECT_FALSE(slice.empty());
}

// 测试std::string构造函数
TEST_F(SliceTest, StringConstructor)
{
    char_slice slice(test_string);
    EXPECT_EQ(slice.data(), test_string.data());
    EXPECT_EQ(slice.size(), test_string.size());
    EXPECT_FALSE(slice.empty());
}

// 测试C字符串构造函数
TEST_F(SliceTest, CStringConstructor)
{
    char_slice slice("Hello");
    EXPECT_STREQ(slice.data(), "Hello");
    EXPECT_EQ(slice.size(), 5u);
    EXPECT_FALSE(slice.empty());
}

// 测试拷贝构造函数
TEST_F(SliceTest, CopyConstructor)
{
    char_slice original(test_data, test_size);
    char_slice copy(original);

    EXPECT_EQ(copy.data(), original.data());
    EXPECT_EQ(copy.size(), original.size());
    EXPECT_EQ(copy.empty(), original.empty());
}

// 测试赋值运算符
TEST_F(SliceTest, AssignmentOperator)
{
    char_slice original(test_data, test_size);
    char_slice assigned;
    assigned = original;

    EXPECT_EQ(assigned.data(), original.data());
    EXPECT_EQ(assigned.size(), original.size());
    EXPECT_EQ(assigned.empty(), original.empty());
}

// 测试下标运算符
TEST_F(SliceTest, SubscriptOperator)
{
    char_slice slice("ABC");
    EXPECT_EQ(slice[0], 'A');
    EXPECT_EQ(slice[1], 'B');
    EXPECT_EQ(slice[2], 'C');
}

// 测试clear方法
TEST_F(SliceTest, Clear)
{
    char_slice slice(test_data, test_size);
    EXPECT_FALSE(slice.empty());

    slice.clear();
    EXPECT_EQ(slice.data(), "");
    EXPECT_EQ(slice.size(), 0u);
    EXPECT_TRUE(slice.empty());
}

// 测试remove_prefix方法
TEST_F(SliceTest, RemovePrefix)
{
    char_slice slice("Hello World");

    slice.remove_prefix(6); // 移除"Hello "
    EXPECT_EQ(slice.size(), 5u);
    EXPECT_STREQ(slice.data(), "World");

    slice.remove_prefix(5); // 移除全部
    EXPECT_EQ(slice.size(), 0u);
    EXPECT_TRUE(slice.empty());
}

// 测试ToString方法
TEST_F(SliceTest, ToString)
{
    char_slice slice(test_data, test_size);
    std::string result = slice.ToString();

    EXPECT_EQ(result, test_string);
    EXPECT_EQ(result.size(), test_size);
}

// 测试compare方法
TEST_F(SliceTest, Compare)
{
    char_slice a("abc");
    char_slice b("abc");
    char_slice c("abd");
    char_slice d("ab");
    char_slice e("abcd");

    // 相等比较
    EXPECT_EQ(a.compare(b), 0);

    // 小于比较
    EXPECT_LT(a.compare(c), 0);
    EXPECT_LT(d.compare(a), 0);

    // 大于比较
    EXPECT_GT(c.compare(a), 0);
    EXPECT_GT(a.compare(d), 0);
    EXPECT_GT(e.compare(a), 0);
}

// 测试starts_with方法
TEST_F(SliceTest, StartsWith)
{
    char_slice slice("Hello World");
    char_slice prefix1("Hello");
    char_slice prefix2("Hello World");
    char_slice prefix3("Hi");
    char_slice prefix4("Hello World!");

    EXPECT_TRUE(slice.starts_with(prefix1));
    EXPECT_TRUE(slice.starts_with(prefix2));
    EXPECT_FALSE(slice.starts_with(prefix3));
    EXPECT_FALSE(slice.starts_with(prefix4));
}

// 测试相等运算符
TEST_F(SliceTest, EqualityOperator)
{
    char_slice a("test");
    char_slice b("test");
    char_slice c("different");
    char_slice d("test123");

    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a == c);
    EXPECT_FALSE(a == d);
}

// 测试不等运算符
TEST_F(SliceTest, InequalityOperator)
{
    char_slice a("test");
    char_slice b("test");
    char_slice c("different");

    EXPECT_FALSE(a != b);
    EXPECT_TRUE(a != c);
}

// 测试边界情况：空slice
TEST_F(SliceTest, EmptySlice)
{
    char_slice empty;
    char_slice non_empty("test");

    EXPECT_TRUE(empty.empty());
    EXPECT_TRUE(empty.starts_with(empty));      // 空slice以空slice开头
    EXPECT_FALSE(empty.starts_with(non_empty)); // 空slice不以非空slice开头
    EXPECT_TRUE(
        non_empty.starts_with(empty)); // 非空slice以空slice开头（数学定义）

    EXPECT_EQ(empty.compare(empty), 0);
    EXPECT_LT(empty.compare(non_empty), 0);
    EXPECT_GT(non_empty.compare(empty), 0);
}

// 测试边界情况：单字符slice
TEST_F(SliceTest, SingleCharSlice)
{
    char_slice single("A");

    EXPECT_EQ(single.size(), 1u);
    EXPECT_EQ(single[0], 'A');
    EXPECT_FALSE(single.empty());

    char_slice prefix("A");
    EXPECT_TRUE(single.starts_with(prefix));

    char_slice wrong_prefix("B");
    EXPECT_FALSE(single.starts_with(wrong_prefix));
}

// 测试中文和特殊字符
TEST_F(SliceTest, UnicodeAndSpecialChars)
{
    std::string unicode_str = "你好，世界！";
    char_slice unicode_slice(unicode_str);

    EXPECT_EQ(unicode_slice.size(), unicode_str.size());
    EXPECT_EQ(unicode_slice.ToString(), unicode_str);

    // 测试包含特殊字符
    std::string special_str = "Line1\nLine2\tTab";
    char_slice special_slice(special_str);

    EXPECT_EQ(special_slice.size(), special_str.size());
    EXPECT_EQ(special_slice.ToString(), special_str);
}

// 新的测试类：测试slice与arena的集成使用
class SliceArenaTest : public ::testing::Test
{
  protected:
    // 移除SetUp方法中的reset()调用
    // arena在每次测试时会自动创建新的实例
    object_arena arena;
};

// 测试使用arena分配内存创建slice
TEST_F(SliceArenaTest, ArenaAllocatedSlice)
{
    // 使用arena分配内存并创建字符串
    const char *original_str = "Hello, Arena!";
    size_t str_len = strlen(original_str);

    // 在arena中分配内存并复制字符串
    char *arena_memory = arena.allocate(str_len + 1); // +1 for null terminator
    memcpy(arena_memory, original_str, str_len);
    arena_memory[str_len] = '\0'; // 添加null终止符

    // 使用arena分配的内存创建slice
    char_slice slice(arena_memory, str_len);

    // 验证slice内容正确
    EXPECT_STREQ(slice.data(), original_str);
    EXPECT_EQ(slice.size(), str_len);
    EXPECT_FALSE(slice.empty());

    // 验证ToString方法正常工作
    EXPECT_EQ(slice.ToString(), std::string(original_str));
}

// 测试多个slice共享arena内存
TEST_F(SliceArenaTest, MultipleSlicesShareArena)
{
    const char *str1 = "First string";
    const char *str2 = "Second string";

    // 在arena中分配并复制第一个字符串
    char *mem1 = arena.allocate(strlen(str1) + 1);
    memcpy(mem1, str1, strlen(str1));
    mem1[strlen(str1)] = '\0';

    // 在arena中分配并复制第二个字符串
    char *mem2 = arena.allocate(strlen(str2) + 1);
    memcpy(mem2, str2, strlen(str2));
    mem2[strlen(str2)] = '\0';

    // 创建两个slice
    char_slice slice1(mem1, strlen(str1));
    char_slice slice2(mem2, strlen(str2));

    // 验证两个slice都正确
    EXPECT_STREQ(slice1.data(), str1);
    EXPECT_STREQ(slice2.data(), str2);
    EXPECT_NE(slice1.data(), slice2.data()); // 确保指向不同内存

    // 验证比较操作
    EXPECT_TRUE(slice1 != slice2);
    EXPECT_FALSE(slice1 == slice2);
}

// 测试slice的修改操作与arena的配合
TEST_F(SliceArenaTest, SliceModificationWithArena)
{
    // 创建原始字符串
    const char *original = "Hello World";
    char *arena_mem = arena.allocate(strlen(original) + 1);
    memcpy(arena_mem, original, strlen(original));
    arena_mem[strlen(original)] = '\0';

    char_slice slice(arena_mem, strlen(original));

    // 测试remove_prefix
    EXPECT_TRUE(slice.starts_with(char_slice("Hello")));
    slice.remove_prefix(6); // 移除"Hello "
    EXPECT_STREQ(slice.data(), "World");
    EXPECT_EQ(slice.size(), 5u);

    // 测试clear
    slice.clear();
    EXPECT_TRUE(slice.empty());
    EXPECT_EQ(slice.size(), 0u);
}

// 测试大内存分配与slice
TEST_F(SliceArenaTest, LargeAllocation)
{
    // 分配较大的内存块
    const size_t large_size = 8192; // 8KB
    char *large_mem = arena.allocate(large_size);

    // 用测试数据填充
    for (size_t i = 0; i < large_size; ++i) {
        large_mem[i] = static_cast<char>('A' + (i % 26));
    }

    // 创建slice
    char_slice slice(large_mem, large_size);

    // 验证slice正确
    EXPECT_EQ(slice.size(), large_size);
    EXPECT_FALSE(slice.empty());

    // 验证前几个字符
    EXPECT_EQ(slice[0], 'A');
    EXPECT_EQ(slice[1], 'B');
    EXPECT_EQ(slice[25], 'Z');
    EXPECT_EQ(slice[26], 'A'); // 循环

    // 验证ToString不会崩溃（虽然可能不实用）
    std::string result = slice.ToString();
    EXPECT_EQ(result.size(), large_size);
}

// 测试混合大小的分配
TEST_F(SliceArenaTest, MixedSizeAllocations)
{
    // 分配不同大小的内存块
    const char *small_str = "small";
    const char *medium_str = "medium sized string";
    const char *large_str =
        "this is a relatively larger string for testing purposes";

    // 分配并复制
    char *small_mem = arena.allocate(strlen(small_str) + 1);
    memcpy(small_mem, small_str, strlen(small_str));
    small_mem[strlen(small_str)] = '\0';

    char *medium_mem = arena.allocate(strlen(medium_str) + 1);
    memcpy(medium_mem, medium_str, strlen(medium_str));
    medium_mem[strlen(medium_str)] = '\0';

    char *large_mem = arena.allocate(strlen(large_str) + 1);
    memcpy(large_mem, large_str, strlen(large_str));
    large_mem[strlen(large_str)] = '\0';

    // 创建slices
    char_slice small_slice(small_mem, strlen(small_str));
    char_slice medium_slice(medium_mem, strlen(medium_str));
    char_slice large_slice(large_mem, strlen(large_str));

    // 验证所有slice都正确
    EXPECT_STREQ(small_slice.data(), small_str);
    EXPECT_STREQ(medium_slice.data(), medium_str);
    EXPECT_STREQ(large_slice.data(), large_str);

    // 验证比较操作
    EXPECT_TRUE(small_slice != medium_slice);
    EXPECT_TRUE(medium_slice != large_slice);
    EXPECT_TRUE(small_slice.compare(large_slice) < 0);
}

// 测试arena内存重用与slice
TEST_F(SliceArenaTest, ArenaMemoryReuse)
{
    // 第一次分配和使用
    const char *str1 = "First allocation";
    char *mem1 = arena.allocate(strlen(str1) + 1);
    memcpy(mem1, str1, strlen(str1));
    mem1[strlen(str1)] = '\0';

    char_slice slice1(mem1, strlen(str1));
    EXPECT_STREQ(slice1.data(), str1);

    // 注意：arena没有reset方法，所以这里不能重置
    // 但我们可以继续使用同一个arena进行新的分配
    const char *str2 = "Second allocation";
    char *mem2 = arena.allocate(strlen(str2) + 1);
    memcpy(mem2, str2, strlen(str2));
    mem2[strlen(str2)] = '\0';

    char_slice slice2(mem2, strlen(str2));
    EXPECT_STREQ(slice2.data(), str2);

    // 验证两个slice都正常工作
    EXPECT_STREQ(slice1.data(), str1);
    EXPECT_STREQ(slice2.data(), str2);

    // 注意：slice1仍然指向有效的内存，因为arena不会释放已分配的内存
    // 这是arena设计的正常行为
}

} // namespace wf::utils

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}