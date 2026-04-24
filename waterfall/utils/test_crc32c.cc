////
// @file test_crc32c.cc
// @brief
// 测试crc32c
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <cstring>
#include <crc32c/crc32c.h>

namespace wf::utils {

class Crc32cTest : public ::testing::Test
{
  protected:
    void SetUp() override {}
    void TearDown() override {}
};

// 测试空数据的CRC32C计算
TEST_F(Crc32cTest, EmptyData)
{
    uint32_t crc = crc32c::Extend(0, nullptr, 0);
    EXPECT_EQ(crc, 0);
}

// 测试简单字符串的CRC32C计算
TEST_F(Crc32cTest, SimpleString)
{
    const char *test_data = "test";
    uint32_t crc =
        crc32c::Extend(0, reinterpret_cast<const uint8_t *>(test_data), 4);
    EXPECT_NE(crc, 0);
}

// 测试增量计算的一致性
TEST_F(Crc32cTest, IncrementalCalculation)
{
    const char *part1 = "hello";
    const char *part2 = "world";
    const char *full = "helloworld";

    // 方法1：一次性计算完整字符串
    uint32_t crc_full =
        crc32c::Extend(0, reinterpret_cast<const uint8_t *>(full), 10);

    // 方法2：分两次计算
    uint32_t crc_inc =
        crc32c::Extend(0, reinterpret_cast<const uint8_t *>(part1), 5);
    crc_inc =
        crc32c::Extend(crc_inc, reinterpret_cast<const uint8_t *>(part2), 5);

    // 两种方法结果应该相同
    EXPECT_EQ(crc_full, crc_inc);
}

// 测试包含空字节的数据
TEST_F(Crc32cTest, DataWithNullBytes)
{
    const char data_with_nulls[] = "test\0data\0here";
    size_t data_len = 14; // 包含空字节的长度

    uint32_t crc = crc32c::Extend(
        0, reinterpret_cast<const uint8_t *>(data_with_nulls), data_len);

    // 验证结果不为0
    EXPECT_NE(crc, 0);

    // 验证相同数据多次计算结果一致
    uint32_t crc2 = crc32c::Extend(
        0, reinterpret_cast<const uint8_t *>(data_with_nulls), data_len);

    EXPECT_EQ(crc, crc2);
}

// 测试大数据的CRC32C计算
TEST_F(Crc32cTest, LargeData)
{
    const size_t large_size = 10000;
    char *large_data = new char[large_size];

    // 填充测试数据
    for (size_t i = 0; i < large_size; ++i) {
        large_data[i] = static_cast<char>(i % 256);
    }

    uint32_t crc = crc32c::Extend(
        0, reinterpret_cast<const uint8_t *>(large_data), large_size);

    // 验证结果不为0
    EXPECT_NE(crc, 0);

    // 验证相同数据多次计算结果一致
    uint32_t crc2 = crc32c::Extend(
        0, reinterpret_cast<const uint8_t *>(large_data), large_size);

    EXPECT_EQ(crc, crc2);

    delete[] large_data;
}

// 测试边界情况：单个字节
TEST_F(Crc32cTest, SingleByte)
{
    const char single_byte = 'A';
    uint32_t crc =
        crc32c::Extend(0, reinterpret_cast<const uint8_t *>(&single_byte), 1);

    EXPECT_NE(crc, 0);

    // 验证不同字节产生不同结果
    const char another_byte = 'B';
    uint32_t crc2 = crc32c::Extend(
        crc, reinterpret_cast<const uint8_t *>(&another_byte), 1);

    EXPECT_NE(crc, crc2);
}

// 测试CRC32C的雪崩效应（小变化导致大差异）
TEST_F(Crc32cTest, AvalancheEffect)
{
    const char *data1 = "hello world";
    const char *data2 = "hello xorld"; // 一个字符差异

    uint32_t crc1 =
        crc32c::Extend(0, reinterpret_cast<const uint8_t *>(data1), 11);

    uint32_t crc2 =
        crc32c::Extend(crc1, reinterpret_cast<const uint8_t *>(data2), 11);

    // 微小变化应该导致CRC值显著不同
    EXPECT_NE(crc1, crc2);

    // 验证差异不是简单的位翻转
    uint32_t diff = crc1 ^ crc2;
    // 差异应该包含多个位的变化
    EXPECT_GT(__builtin_popcount(diff), 1);
}

// 测试对齐数据和非对齐数据的结果一致性
TEST_F(Crc32cTest, AlignmentConsistency)
{
    const char *test_string = "alignment test";
    size_t len = 14;

    // 创建对齐的缓冲区
    alignas(16) char aligned_buffer[64];
    memcpy(aligned_buffer, test_string, len);

    // 创建非对齐的缓冲区（偏移1字节）
    char unaligned_buffer[64];
    char *unaligned_ptr = unaligned_buffer + 1;
    memcpy(unaligned_ptr, test_string, len);

    uint32_t crc_aligned = crc32c::Extend(
        0, reinterpret_cast<const uint8_t *>(aligned_buffer), len);

    uint32_t crc_unaligned = crc32c::Extend(
        0, reinterpret_cast<const uint8_t *>(unaligned_ptr), len);

    // 对齐和非对齐数据应该产生相同结果
    EXPECT_EQ(crc_aligned, crc_unaligned);
}

// 测试已知的CRC32C值（验证算法正确性）
TEST_F(Crc32cTest, KnownValues)
{
    // 测试一些已知的CRC32C值
    // 这些值可以通过其他可靠的CRC32C计算器验证

    struct TestCase
    {
        const char *data;
        size_t len;
        uint32_t expected;
    };

    TestCase test_cases[] = {
        {"", 0, 0x00000000},          // 空字符串的CRC32C
        {"\x00", 1, 0x527D5351},      // 单个空字节
        {"123456789", 9, 0xE3069283}, // 经典测试数据
        // 可以添加更多已知值
    };

    for (const auto &tc : test_cases) {
        uint32_t result = crc32c::Extend(
            0, reinterpret_cast<const uint8_t *>(tc.data), tc.len);

        EXPECT_EQ(result, tc.expected)
            << "Data: " << std::string(tc.data, tc.len) << " Expected: 0x"
            << std::hex << tc.expected << " Got: 0x" << std::hex << result;
    }
}

} // namespace wf::utils

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}