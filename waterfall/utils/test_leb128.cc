////
// @file test_leb128.cc
// @brief
// 测试leb128
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include "leb128.h"

namespace wf::utils {

class LEB128Test : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // 初始化测试缓冲区
        memset(buffer, 0, sizeof(buffer));
    }

    unsigned char buffer[16];
};

// 测试ULEB128编码和解码
TEST_F(LEB128Test, ULEB128EncodeDecode)
{
    // 测试小数值
    unsigned long value1 = 0;
    unsigned int len1 = encode_uleb128(value1, buffer);
    EXPECT_EQ(len1, 1);
    EXPECT_EQ(buffer[0], 0x00);

    unsigned long decoded1 = decode_uleb128(buffer);
    EXPECT_EQ(decoded1, value1);

    // 测试中等数值
    unsigned long value2 = 127;
    unsigned int len2 = encode_uleb128(value2, buffer);
    EXPECT_EQ(len2, 1);
    EXPECT_EQ(buffer[0], 0x7F);

    unsigned long decoded2 = decode_uleb128(buffer);
    EXPECT_EQ(decoded2, value2);

    // 测试需要多个字节的数值
    unsigned long value3 = 128;
    unsigned int len3 = encode_uleb128(value3, buffer);
    EXPECT_EQ(len3, 2);
    EXPECT_EQ(buffer[0], 0x80);
    EXPECT_EQ(buffer[1], 0x01);

    unsigned long decoded3 = decode_uleb128(buffer);
    EXPECT_EQ(decoded3, value3);

    // 测试更大的数值
    unsigned long value4 = 624485; // 0x98985
    unsigned int len4 = encode_uleb128(value4, buffer);
    EXPECT_EQ(len4, 3);
    EXPECT_EQ(buffer[0], 0xE5);
    EXPECT_EQ(buffer[1], 0x8E);
    EXPECT_EQ(buffer[2], 0x26);

    unsigned long decoded4 = decode_uleb128(buffer);
    EXPECT_EQ(decoded4, value4);
}

// 测试ULEB128填充功能
TEST_F(LEB128Test, ULEB128Padding)
{
    unsigned long value = 127;

    // 测试填充到3字节
    unsigned int len = encode_uleb128(value, buffer, 3);
    EXPECT_EQ(len, 3);
    EXPECT_EQ(buffer[0], 0xFF); // 修正：127 | 0x80 = 0xFF
    EXPECT_EQ(buffer[1], 0x80); // 修正：0 | 0x80 = 0x80
    EXPECT_EQ(buffer[2], 0x00); // 修正：0 = 0x00

    unsigned long decoded = decode_uleb128(buffer);
    EXPECT_EQ(decoded, value);

    // 测试填充到5字节
    len = encode_uleb128(value, buffer, 5);
    EXPECT_EQ(len, 5);
    EXPECT_EQ(buffer[0], 0xFF); // 修正：127 | 0x80 = 0xFF
    EXPECT_EQ(buffer[1], 0x80); // 修正：0 | 0x80 = 0x80
    EXPECT_EQ(buffer[2], 0x80); // 修正：填充字节
    EXPECT_EQ(buffer[3], 0x80); // 修正：填充字节
    EXPECT_EQ(buffer[4], 0x00); // 修正：填充终止字节

    decoded = decode_uleb128(buffer);
    EXPECT_EQ(decoded, value);
}

// 测试SLEB128编码和解码
TEST_F(LEB128Test, SLEB128EncodeDecode)
{
    // 测试0
    long value1 = 0;
    unsigned int len1 = encode_sleb128(value1, buffer);
    EXPECT_EQ(len1, 1);
    EXPECT_EQ(buffer[0], 0x00);

    long decoded1 = decode_sleb128(buffer);
    EXPECT_EQ(decoded1, value1);

    // 测试正数
    long value2 = 63;
    unsigned int len2 = encode_sleb128(value2, buffer);
    EXPECT_EQ(len2, 1);
    EXPECT_EQ(buffer[0], 0x3F);

    long decoded2 = decode_sleb128(buffer);
    EXPECT_EQ(decoded2, value2);

    // 测试负数
    long value3 = -64;
    unsigned int len3 = encode_sleb128(value3, buffer);
    EXPECT_EQ(len3, 1);
    EXPECT_EQ(buffer[0], 0x40);

    long decoded3 = decode_sleb128(buffer);
    EXPECT_EQ(decoded3, value3);

    // 测试需要多个字节的正数
    long value4 = 128;
    unsigned int len4 = encode_sleb128(value4, buffer);
    EXPECT_EQ(len4, 2);
    EXPECT_EQ(buffer[0], 0x80);
    EXPECT_EQ(buffer[1], 0x01);

    long decoded4 = decode_sleb128(buffer);
    EXPECT_EQ(decoded4, value4);

    // 测试需要多个字节的负数
    long value5 = -128;
    unsigned int len5 = encode_sleb128(value5, buffer);
    EXPECT_EQ(len5, 2);
    EXPECT_EQ(buffer[0], 0x80);
    EXPECT_EQ(buffer[1], 0x7F);

    long decoded5 = decode_sleb128(buffer);
    EXPECT_EQ(decoded5, value5);
}

// 测试SLEB128填充功能
TEST_F(LEB128Test, SLEB128Padding)
{
    // 测试正数填充
    long value1 = 63;
    unsigned int len1 = encode_sleb128(value1, buffer, 3);
    EXPECT_EQ(len1, 3);
    EXPECT_EQ(buffer[0], 0xBF); // 修正：63 | 0x80 = 0xBF
    EXPECT_EQ(buffer[1], 0x80); // 修正：填充字节
    EXPECT_EQ(buffer[2], 0x00); // 修正：填充终止字节

    long decoded1 = decode_sleb128(buffer);
    EXPECT_EQ(decoded1, value1);

    // 测试负数填充
    long value2 = -64;
    unsigned int len2 = encode_sleb128(value2, buffer, 4);
    EXPECT_EQ(len2, 4);
    EXPECT_EQ(buffer[0], 0xC0); // 修正：-64 & 0x7F | 0x80 = 0x40 | 0x80 = 0xC0
    EXPECT_EQ(buffer[1], 0xFF); // 修正：填充字节
    EXPECT_EQ(buffer[2], 0xFF); // 修正：填充字节
    EXPECT_EQ(buffer[3], 0x7F); // 修正：填充终止字节

    long decoded2 = decode_sleb128(buffer);
    EXPECT_EQ(decoded2, value2);
}

// 测试错误处理
TEST_F(LEB128Test, ErrorHandling)
{
    const char *error = nullptr;
    const unsigned char *end = buffer + 1;

    // 测试缓冲区不足
    buffer[0] = 0x80; // 需要更多字节
    unsigned long uresult = decode_uleb128(buffer, nullptr, end, &error);
    EXPECT_EQ(uresult, 0);
    EXPECT_STREQ(error, "malformed uleb128, extends past end");

    long sresult = decode_sleb128(buffer, nullptr, end, &error);
    EXPECT_EQ(sresult, 0);
    EXPECT_STREQ(error, "malformed sleb128, extends past end");
}

// 测试边界值
TEST_F(LEB128Test, BoundaryValues)
{
    // 测试ULEB128最大值附近的值
    unsigned long max_uleb128 = 0xFFFFFFFFFFFFFFFF;
    unsigned int len = encode_uleb128(max_uleb128, buffer);
    EXPECT_GT(len, 0);

    unsigned long decoded = decode_uleb128(buffer);
    EXPECT_EQ(decoded, max_uleb128);

    // 测试SLEB128边界值
    long max_sleb128 = 0x7FFFFFFFFFFFFFFF;
    len = encode_sleb128(max_sleb128, buffer);
    EXPECT_GT(len, 0);

    long decoded_s = decode_sleb128(buffer);
    EXPECT_EQ(decoded_s, max_sleb128);

    long min_sleb128 = -0x8000000000000000;
    len = encode_sleb128(min_sleb128, buffer);
    EXPECT_GT(len, 0);

    decoded_s = decode_sleb128(buffer);
    EXPECT_EQ(decoded_s, min_sleb128);
}

// 测试长度参数
TEST_F(LEB128Test, LengthParameter)
{
    unsigned int n = 0;

    // 测试ULEB128长度返回
    unsigned long value = 624485;
    encode_uleb128(value, buffer);
    unsigned long decoded = decode_uleb128(buffer, &n);
    EXPECT_EQ(decoded, value);
    EXPECT_EQ(n, 3);

    // 测试SLEB128长度返回
    long svalue = -128;
    encode_sleb128(svalue, buffer);
    long sdecoded = decode_sleb128(buffer, &n);
    EXPECT_EQ(sdecoded, svalue);
    EXPECT_EQ(n, 2);
}

// 测试ULEB128长度计算
TEST_F(LEB128Test, ULEB128LengthCalculation)
{
    // 测试基本边界值
    EXPECT_EQ(uleb128_length(0), 1);
    EXPECT_EQ(uleb128_length(1), 1);
    EXPECT_EQ(uleb128_length(126), 1);
    EXPECT_EQ(uleb128_length(127), 1);
    EXPECT_EQ(uleb128_length(128), 2);
    EXPECT_EQ(uleb128_length(129), 2);
    EXPECT_EQ(uleb128_length(16382), 2);
    EXPECT_EQ(uleb128_length(16383), 2);
    EXPECT_EQ(uleb128_length(16384), 3);
    EXPECT_EQ(uleb128_length(16385), 3);
    EXPECT_EQ(uleb128_length(2097150), 3);
    EXPECT_EQ(uleb128_length(2097151), 3);
    EXPECT_EQ(uleb128_length(2097152), 4);

    // 测试大数值边界
    EXPECT_EQ(uleb128_length(268435454), 4);
    EXPECT_EQ(uleb128_length(268435455), 4);
    EXPECT_EQ(uleb128_length(268435456), 5);
    EXPECT_EQ(uleb128_length(268435457), 5);
    EXPECT_EQ(uleb128_length(34359738366), 5);
    EXPECT_EQ(uleb128_length(34359738367), 5);
    EXPECT_EQ(uleb128_length(34359738368), 6);
    EXPECT_EQ(uleb128_length(34359738369), 6);
    EXPECT_EQ(uleb128_length(4398046511103), 6);
    EXPECT_EQ(uleb128_length(4398046511104), 7);
    EXPECT_EQ(uleb128_length(562949953421311), 7);
    EXPECT_EQ(uleb128_length(562949953421312), 8);
    EXPECT_EQ(uleb128_length(72057594037927935), 8);
    EXPECT_EQ(uleb128_length(72057594037927936), 9);

    // 测试极大值和特殊值
    EXPECT_EQ(
        uleb128_length(ULONG_MAX - 2), encode_uleb128(ULONG_MAX - 2, buffer));
    EXPECT_EQ(
        uleb128_length(ULONG_MAX - 1), encode_uleb128(ULONG_MAX - 1, buffer));
    EXPECT_EQ(uleb128_length(ULONG_MAX), encode_uleb128(ULONG_MAX, buffer));

    // 测试随机值
    EXPECT_EQ(uleb128_length(12345), encode_uleb128(12345, buffer));
    EXPECT_EQ(uleb128_length(987654), encode_uleb128(987654, buffer));
    EXPECT_EQ(uleb128_length(123456789), encode_uleb128(123456789, buffer));
    EXPECT_EQ(uleb128_length(9876543210), encode_uleb128(9876543210, buffer));

    // 验证长度计算与编码结果一致（扩展测试集）
    unsigned long test_values[] = {
        0,
        1,
        126,
        127,
        128,
        129,
        16382,
        16383,
        16384,
        16385,
        2097150,
        2097151,
        2097152,
        268435455,
        268435456,
        34359738367,
        34359738368,
        4398046511103,
        4398046511104,
        562949953421311,
        562949953421312,
        72057594037927935,
        72057594037927936,
        ULONG_MAX - 1,
        ULONG_MAX,
        12345,
        987654,
        123456789,
        9876543210};

    for (auto value : test_values) {
        unsigned int encoded_len = encode_uleb128(value, buffer);
        unsigned int calculated_len = uleb128_length(value);
        EXPECT_EQ(encoded_len, calculated_len)
            << "Value: " << value << ", Encoded: " << encoded_len
            << ", Calculated: " << calculated_len;
    }
}

// 测试SLEB128长度计算
TEST_F(LEB128Test, SLEB128LengthCalculation)
{
    // 测试正数边界按照字节扩展阈值
    EXPECT_EQ(sleb128_length(0), encode_sleb128(0, buffer));
    EXPECT_EQ(sleb128_length(1), encode_sleb128(1, buffer));
    EXPECT_EQ(sleb128_length(62), encode_sleb128(62, buffer));
    EXPECT_EQ(sleb128_length(63), encode_sleb128(63, buffer));
    EXPECT_EQ(sleb128_length(64), encode_sleb128(64, buffer));
    EXPECT_EQ(sleb128_length(65), encode_sleb128(65, buffer));
    EXPECT_EQ(sleb128_length(8190), encode_sleb128(8190, buffer));
    EXPECT_EQ(sleb128_length(8191), encode_sleb128(8191, buffer));
    EXPECT_EQ(sleb128_length(8192), encode_sleb128(8192, buffer));
    EXPECT_EQ(sleb128_length(8193), encode_sleb128(8193, buffer));
    EXPECT_EQ(sleb128_length(1048574), encode_sleb128(1048574, buffer));
    EXPECT_EQ(sleb128_length(1048575), encode_sleb128(1048575, buffer));
    EXPECT_EQ(sleb128_length(1048576), encode_sleb128(1048576, buffer));
    EXPECT_EQ(sleb128_length(1048577), encode_sleb128(1048577, buffer));
    EXPECT_EQ(sleb128_length(134217726), encode_sleb128(134217726, buffer));
    EXPECT_EQ(sleb128_length(134217727), encode_sleb128(134217727, buffer));
    EXPECT_EQ(sleb128_length(134217728), encode_sleb128(134217728, buffer));
    EXPECT_EQ(sleb128_length(134217729), encode_sleb128(134217729, buffer));
    EXPECT_EQ(
        sleb128_length(17179869182LL), encode_sleb128(17179869182LL, buffer));
    EXPECT_EQ(
        sleb128_length(17179869183LL), encode_sleb128(17179869183LL, buffer));
    EXPECT_EQ(
        sleb128_length(17179869184LL), encode_sleb128(17179869184LL, buffer));
    EXPECT_EQ(
        sleb128_length(17179869185LL), encode_sleb128(17179869185LL, buffer));
    EXPECT_EQ(
        sleb128_length(2199023255550LL),
        encode_sleb128(2199023255550LL, buffer));
    EXPECT_EQ(
        sleb128_length(2199023255551LL),
        encode_sleb128(2199023255551LL, buffer));
    EXPECT_EQ(
        sleb128_length(2199023255552LL),
        encode_sleb128(2199023255552LL, buffer));
    EXPECT_EQ(
        sleb128_length(2199023255553LL),
        encode_sleb128(2199023255553LL, buffer));
    EXPECT_EQ(
        sleb128_length(281474976710654LL),
        encode_sleb128(281474976710654LL, buffer));
    EXPECT_EQ(
        sleb128_length(281474976710655LL),
        encode_sleb128(281474976710655LL, buffer));
    EXPECT_EQ(
        sleb128_length(281474976710656LL),
        encode_sleb128(281474976710656LL, buffer));
    EXPECT_EQ(
        sleb128_length(281474976710657LL),
        encode_sleb128(281474976710657LL, buffer));
    EXPECT_EQ(
        sleb128_length(36028797018963966LL),
        encode_sleb128(36028797018963966LL, buffer));
    EXPECT_EQ(
        sleb128_length(36028797018963967LL),
        encode_sleb128(36028797018963967LL, buffer));
    EXPECT_EQ(
        sleb128_length(36028797018963968LL),
        encode_sleb128(36028797018963968LL, buffer));
    EXPECT_EQ(
        sleb128_length(36028797018963969LL),
        encode_sleb128(36028797018963969LL, buffer));

    // 测试负数边界
    EXPECT_EQ(sleb128_length(-1), encode_sleb128(-1, buffer));
    EXPECT_EQ(sleb128_length(-2), encode_sleb128(-2, buffer));
    EXPECT_EQ(sleb128_length(-63), encode_sleb128(-63, buffer));
    EXPECT_EQ(sleb128_length(-64), encode_sleb128(-64, buffer));
    EXPECT_EQ(sleb128_length(-65), encode_sleb128(-65, buffer));
    EXPECT_EQ(sleb128_length(-66), encode_sleb128(-66, buffer));
    EXPECT_EQ(sleb128_length(-8191), encode_sleb128(-8191, buffer));
    EXPECT_EQ(sleb128_length(-8192), encode_sleb128(-8192, buffer));
    EXPECT_EQ(sleb128_length(-8193), encode_sleb128(-8193, buffer));
    EXPECT_EQ(sleb128_length(-8194), encode_sleb128(-8194, buffer));
    EXPECT_EQ(sleb128_length(-1048575), encode_sleb128(-1048575, buffer));
    EXPECT_EQ(sleb128_length(-1048576), encode_sleb128(-1048576, buffer));
    EXPECT_EQ(sleb128_length(-1048577), encode_sleb128(-1048577, buffer));
    EXPECT_EQ(sleb128_length(-1048578), encode_sleb128(-1048578, buffer));
    EXPECT_EQ(sleb128_length(-134217727), encode_sleb128(-134217727, buffer));
    EXPECT_EQ(sleb128_length(-134217728), encode_sleb128(-134217728, buffer));
    EXPECT_EQ(sleb128_length(-134217729), encode_sleb128(-134217729, buffer));
    EXPECT_EQ(sleb128_length(-134217730), encode_sleb128(-134217730, buffer));
    EXPECT_EQ(
        sleb128_length(-17179869183LL), encode_sleb128(-17179869183LL, buffer));
    EXPECT_EQ(
        sleb128_length(-17179869184LL), encode_sleb128(-17179869184LL, buffer));
    EXPECT_EQ(
        sleb128_length(-17179869185LL), encode_sleb128(-17179869185LL, buffer));
    EXPECT_EQ(
        sleb128_length(-17179869186LL), encode_sleb128(-17179869186LL, buffer));
    EXPECT_EQ(
        sleb128_length(-2199023255551LL),
        encode_sleb128(-2199023255551LL, buffer));
    EXPECT_EQ(
        sleb128_length(-2199023255552LL),
        encode_sleb128(-2199023255552LL, buffer));
    EXPECT_EQ(
        sleb128_length(-2199023255553LL),
        encode_sleb128(-2199023255553LL, buffer));
    EXPECT_EQ(
        sleb128_length(-2199023255554LL),
        encode_sleb128(-2199023255554LL, buffer));
    EXPECT_EQ(
        sleb128_length(-281474976710655LL),
        encode_sleb128(-281474976710655LL, buffer));
    EXPECT_EQ(
        sleb128_length(-281474976710656LL),
        encode_sleb128(-281474976710656LL, buffer));
    EXPECT_EQ(
        sleb128_length(-281474976710657LL),
        encode_sleb128(-281474976710657LL, buffer));
    EXPECT_EQ(
        sleb128_length(-281474976710658LL),
        encode_sleb128(-281474976710658LL, buffer));
    EXPECT_EQ(
        sleb128_length(-36028797018963967LL),
        encode_sleb128(-36028797018963967LL, buffer));
    EXPECT_EQ(
        sleb128_length(-36028797018963968LL),
        encode_sleb128(-36028797018963968LL, buffer));
    EXPECT_EQ(
        sleb128_length(-36028797018963969LL),
        encode_sleb128(-36028797018963969LL, buffer));
    EXPECT_EQ(
        sleb128_length(-36028797018963970LL),
        encode_sleb128(-36028797018963970LL, buffer));

    // 测试边界值和特殊值
    EXPECT_EQ(
        sleb128_length(LONG_MAX - 1), encode_sleb128(LONG_MAX - 1, buffer));
    EXPECT_EQ(sleb128_length(LONG_MAX), encode_sleb128(LONG_MAX, buffer));
    EXPECT_EQ(
        sleb128_length(LONG_MIN + 1), encode_sleb128(LONG_MIN + 1, buffer));
    EXPECT_EQ(sleb128_length(LONG_MIN), encode_sleb128(LONG_MIN, buffer));

    // 测试随机值
    EXPECT_EQ(sleb128_length(12345), encode_sleb128(12345, buffer));
    EXPECT_EQ(sleb128_length(-12345), encode_sleb128(-12345, buffer));
    EXPECT_EQ(sleb128_length(987654), encode_sleb128(987654, buffer));
    EXPECT_EQ(sleb128_length(-987654), encode_sleb128(-987654, buffer));
    EXPECT_EQ(sleb128_length(123456789), encode_sleb128(123456789, buffer));
    EXPECT_EQ(sleb128_length(-123456789), encode_sleb128(-123456789, buffer));

    // 综合验证：测试大量随机值
    long test_values[] = {
        // 正数边界
        0,
        1,
        62,
        63,
        64,
        65,
        8190,
        8191,
        8192,
        8193,
        1048574,
        1048575,
        1048576,
        1048577,
        134217726,
        134217727,
        134217728,
        134217729,
        17179869182LL,
        17179869183LL,
        17179869184LL,
        17179869185LL,
        2199023255550LL,
        2199023255551LL,
        2199023255552LL,
        2199023255553LL,
        281474976710654LL,
        281474976710655LL,
        281474976710656LL,
        281474976710657LL,
        36028797018963966LL,
        36028797018963967LL,
        36028797018963968LL,
        36028797018963969LL,

        // 负数边界
        -1,
        -2,
        -63,
        -64,
        -65,
        -66,
        -8191,
        -8192,
        -8193,
        -8194,
        -1048575,
        -1048576,
        -1048577,
        -1048578,
        -134217727,
        -134217728,
        -134217729,
        -134217730,
        -17179869183LL,
        -17179869184LL,
        -17179869185LL,
        -17179869186LL,
        -2199023255551LL,
        -2199023255552LL,
        -2199023255553LL,
        -2199023255554LL,
        -281474976710655LL,
        -281474976710656LL,
        -281474976710657LL,
        -281474976710658LL,
        -36028797018963967LL,
        -36028797018963968LL,
        -36028797018963969LL,
        -36028797018963970LL,

        // 特殊值
        LONG_MAX - 1,
        LONG_MAX,
        LONG_MIN + 1,
        LONG_MIN,

        // 随机值
        12345,
        -12345,
        987654,
        -987654,
        123456789,
        -123456789};

    for (auto value : test_values) {
        unsigned int encoded_len = encode_sleb128(value, buffer);
        unsigned int calculated_len = sleb128_length(value);
        EXPECT_EQ(encoded_len, calculated_len)
            << "Value: " << value << ", Encoded: " << encoded_len
            << ", Calculated: " << calculated_len;
    }
}

} // namespace wf::utils

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}