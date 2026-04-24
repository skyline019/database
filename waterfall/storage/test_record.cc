////
// @file test_record.cc
// @brief
// 测试record
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include "record.h"
#include <waterfall/utils/leb128.h>

namespace wf::storage {

class RecordPayloadTest : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // 初始化测试缓冲区
        buffer_size = 1024;
        buffer = new unsigned char[buffer_size];
        memset(buffer, 0, buffer_size);

        // 初始化arena
        arena = new wf::utils::object_arena();
    }

    void TearDown() override
    {
        delete[] buffer;
        delete arena;
    }

    unsigned char *buffer;
    size_t buffer_size;
    wf::utils::object_arena *arena;
};

// 测试单个字段记录
TEST_F(RecordPayloadTest, SingleFieldRecord)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 添加一个字段
    wf::utils::char_slice slice1("hello", 5);
    tuple.emplace_back(slice1);

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证记录长度应该大于字段长度（包含编码信息）
    EXPECT_GT(payload.get_length(), slice1.size());
}

// 测试多个字段记录
TEST_F(RecordPayloadTest, MultipleFieldsRecord)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 添加多个字段
    wf::utils::char_slice slice1("field1", 6);
    wf::utils::char_slice slice2("field2", 6);
    wf::utils::char_slice slice3("field3", 6);

    tuple.emplace_back(slice1);
    tuple.emplace_back(slice2);
    tuple.emplace_back(slice3);

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证记录长度应该大于所有字段长度之和
    size_t total_fields_size = slice1.size() + slice2.size() + slice3.size();
    EXPECT_GT(payload.get_length(), total_fields_size);
}

// 测试大字段记录
TEST_F(RecordPayloadTest, LargeFieldRecord)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建大字段
    std::string large_data(500, 'X');
    wf::utils::char_slice large_slice(large_data);
    tuple.emplace_back(large_slice);

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证记录长度
    EXPECT_GT(payload.get_length(), large_slice.size());
}

// 测试缓冲区不足的情况
TEST_F(RecordPayloadTest, InsufficientBuffer)
{
    // 创建很小的缓冲区
    unsigned char small_buffer[10];
    record_payload payload;
    payload.attatch_payload(small_buffer);
    payload.set_length(10);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 添加一个较大的字段
    wf::utils::char_slice slice("this is too long", 16);
    tuple.emplace_back(slice);

    // 编码应该失败
    error_info result = payload.encode_tuple(tuple);
    EXPECT_NE(result, utils::OK);
}

// 测试空缓冲区
TEST_F(RecordPayloadTest, NullBuffer)
{
    record_payload payload;
    payload.set_length(100);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    wf::utils::char_slice slice("test", 4);
    tuple.emplace_back(slice);

    // 编码应该失败
    error_info result = payload.encode_tuple(tuple);
    EXPECT_NE(result, utils::OK);
}

// 测试不同大小的字段组合
TEST_F(RecordPayloadTest, MixedSizeFields)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 添加不同大小的字段 - 确保所有字符串数据在测试期间有效
    wf::utils::char_slice small("a", 1);
    wf::utils::char_slice medium("medium field", 12);

    // 使用arena分配字符串创建大字段
    char *large_data = arena->allocate(160);
    ::memset(large_data, 'L', 160);
    wf::utils::char_slice large(large_data, 160);

    tuple.emplace_back(small);
    tuple.emplace_back(medium);
    tuple.emplace_back(large);

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证记录长度
    size_t total_size = small.size() + medium.size() + large.size();
    EXPECT_GT(payload.get_length(), total_size);
}

// 测试包含空字段的记录
TEST_F(RecordPayloadTest, EmptyFieldsRecord)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 添加空字段和正常字段
    wf::utils::char_slice normal("normal", 6);
    wf::utils::char_slice empty1;
    wf::utils::char_slice empty2("", 0);

    tuple.emplace_back(normal);
    tuple.emplace_back(empty1);
    tuple.emplace_back(empty2);

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);
}

// 第1个字段不能为空
TEST_F(RecordPayloadTest, EmptyFieldsRecord2)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 添加空字段和正常字段
    wf::utils::char_slice normal("normal", 6);
    wf::utils::char_slice empty1;
    wf::utils::char_slice empty2("", 0);

    tuple.emplace_back(empty1);
    tuple.emplace_back(normal);
    tuple.emplace_back(empty2);

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, kErrorFirstFieldIsEmpty);
}

// 测试记录边界情况
TEST_F(RecordPayloadTest, BoundaryConditions)
{
    // 测试刚好能容纳的缓冲区
    constexpr size_t exact_size = 100;
    unsigned char exact_buffer[exact_size];
    record_payload payload;
    payload.attatch_payload(exact_buffer);
    payload.set_length(exact_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建一个刚好能容纳的字段
    std::string exact_data(exact_size - 10, 'E'); // 留出空间给编码信息
    wf::utils::char_slice exact_slice(exact_data);
    tuple.emplace_back(exact_slice);

    // 编码应该成功
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);
}

// 详细测试encode_tuple的编码过程
TEST_F(RecordPayloadTest, EncodeTupleDetailed)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建测试字段
    wf::utils::char_slice field1("field1", 6);
    wf::utils::char_slice field2("field2_data", 11);
    wf::utils::char_slice field3("field3_long_data", 16);

    tuple.emplace_back(field1);
    tuple.emplace_back(field2);
    tuple.emplace_back(field3);

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 1. 验证总长编码成uleb128
    unsigned char *p = payload.get_payload();
    unsigned int length_bytes;
    size_t decoded_length = utils::decode_uleb128(p, &length_bytes);

    // 计算预期的总长度（包含所有编码长度）
    size_t fields_total_size = field1.size() + field2.size() + field3.size();

    // 计算字段偏移量的编码长度（n个字段有n-1个偏移量）
    size_t offset1 = 0;
    size_t offset2 = field1.size();
    size_t offset3 = field1.size() + field2.size();
    size_t offsets_encoding_length = utils::uleb128_length(offset1) +
                                     utils::uleb128_length(offset2) +
                                     utils::uleb128_length(offset3);

    // 总长度 = 字段数据总长度 + 偏移量编码长度 + 总长度编码长度
    size_t expected_total_length = fields_total_size + offsets_encoding_length +
                                   utils::uleb128_length(fields_total_size);

    EXPECT_EQ(decoded_length, fields_total_size);
    EXPECT_EQ(expected_total_length, payload.get_length());
    EXPECT_EQ(length_bytes, utils::uleb128_length(fields_total_size));

    // 2. 验证各字段偏移量倒排（不包含最后一个字段的偏移量）
    p += length_bytes; // 跳过长度编码

    // 对于3个字段，应该有2个偏移量（field1结束位置和field1+field2结束位置）
    size_t xoffset1, xoffset2, xoffset3;
    unsigned int offset1_bytes, offset2_bytes, offset3_bytes;
    xoffset3 = utils::decode_uleb128(p, &offset3_bytes);
    p += offset3_bytes;
    xoffset2 = utils::decode_uleb128(p, &offset2_bytes);
    p += offset2_bytes;
    xoffset1 = utils::decode_uleb128(p, &offset1_bytes);
    p += offset1_bytes;

    // 验证偏移量正确性
    EXPECT_EQ(offset1, xoffset1);
    EXPECT_EQ(offset2, xoffset2);
    EXPECT_EQ(offset3, xoffset3);

    // 验证偏移量编码长度计算正确
    EXPECT_EQ(offset1_bytes, utils::uleb128_length(offset1));
    EXPECT_EQ(offset2_bytes, utils::uleb128_length(offset2));
    EXPECT_EQ(offset3_bytes, utils::uleb128_length(offset3));

    // 3. 验证各字段被正确拷贝
    // 字段1数据应该紧跟在偏移量之后
    EXPECT_EQ(std::memcmp(p, field1.data(), field1.size()), 0);
    p += field1.size();

    // 字段2数据
    EXPECT_EQ(std::memcmp(p, field2.data(), field2.size()), 0);
    p += field2.size();

    // 字段3数据
    EXPECT_EQ(std::memcmp(p, field3.data(), field3.size()), 0);
    p += field3.size();

    // 验证整个记录的长度计算正确
    size_t actual_total_length = p - payload.get_payload();
    EXPECT_EQ(payload.get_length(), actual_total_length);
}

// 测试单字段记录的编码
TEST_F(RecordPayloadTest, EncodeSingleFieldDetailed)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 单字段记录
    wf::utils::char_slice field("single_field", 12);
    EXPECT_EQ(field.size(), 12);
    tuple.emplace_back(field);
    size_t tuple_size = sizeof_tuple(tuple);
    EXPECT_EQ(tuple_size, field.size());

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证编码结构
    unsigned char *p = payload.get_payload();
    unsigned int length_bytes;
    size_t decoded_length = utils::decode_uleb128(p, &length_bytes);

    EXPECT_EQ(decoded_length, field.size());

    // 单字段记录不应该有偏移量（只有总长度）
    p += length_bytes;

    unsigned int offset_bytes;
    size_t offset = utils::decode_uleb128(p, &offset_bytes);
    EXPECT_EQ(offset_bytes, 1);
    EXPECT_EQ(offset, 0);

    p += offset_bytes;

    // 直接验证字段数据
    EXPECT_EQ(std::memcmp(p, field.data(), field.size()), 0);
    p += field.size();
    EXPECT_EQ(p, payload.get_payload() + payload.get_length());
}

// 测试空记录的编码
TEST_F(RecordPayloadTest, EncodeEmptyTupleDetailed)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 空记录
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, kErrorTupleSizeIsZero);
}

// 测试字段偏移量编码的正确性
TEST_F(RecordPayloadTest, FieldOffsetsEncoding)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建不同大小的字段，测试偏移量计算
    wf::utils::char_slice small("a", 1);
    wf::utils::char_slice medium("medium", 6);
    wf::utils::char_slice large("large_field_data", 16);

    tuple.emplace_back(small);
    tuple.emplace_back(medium);
    tuple.emplace_back(large);

    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 手动计算预期的偏移量
    size_t offset1_expected = 0;
    size_t offset2_expected = small.size();                 // 1
    size_t offset3_expected = small.size() + medium.size(); // 1 + 6 = 7

    // 验证编码后的偏移量
    unsigned char *p = payload.get_payload();
    unsigned int length_bytes;
    unsigned long decoded_length = utils::decode_uleb128(p, &length_bytes);
    p += length_bytes; // 跳过长度编码
    EXPECT_EQ(decoded_length, small.size() + medium.size() + large.size());

    unsigned int offset1_bytes, offset2_bytes, offset3_bytes;
    unsigned long offset1_decoded, offset2_decoded, offset3_decoded;
    offset3_decoded = utils::decode_uleb128(p, &offset3_bytes);
    p += offset3_bytes;
    offset2_decoded = utils::decode_uleb128(p, &offset2_bytes);
    p += offset2_bytes;
    offset1_decoded = utils::decode_uleb128(p, &offset1_bytes);
    p += offset1_bytes;

    EXPECT_EQ(offset1_decoded, offset1_expected);
    EXPECT_EQ(offset2_decoded, offset2_expected);
    EXPECT_EQ(offset3_decoded, offset3_expected);

    EXPECT_EQ(
        payload.get_length(),
        length_bytes + offset3_bytes + offset2_bytes + offset1_bytes +
            small.size() + medium.size() + large.size());
}

// 测试包含空字段时的偏移量计算
TEST_F(RecordPayloadTest, EmptyFieldOffsetCalculation)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建包含空字段的测试数据
    wf::utils::char_slice normal_field("normal", 6);
    wf::utils::char_slice empty_field1; // 默认构造的空字段
    wf::utils::char_slice another_field("another", 7);
    wf::utils::char_slice empty_field2("", 0); // 显式创建的空字段

    tuple.emplace_back(normal_field);
    tuple.emplace_back(empty_field1);
    tuple.emplace_back(another_field);
    tuple.emplace_back(empty_field2);

    // 编码元组
    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证编码结构
    unsigned char *p = payload.get_payload();
    unsigned int length_bytes;
    unsigned long decoded_length = utils::decode_uleb128(p, &length_bytes);
    p += length_bytes;
    EXPECT_EQ(
        decoded_length,
        empty_field1.size() + normal_field.size() + empty_field2.size() +
            another_field.size());
    EXPECT_EQ(length_bytes, utils::uleb128_length(decoded_length));

    // 计算预期的偏移量
    size_t offset1_expected = 0;
    EXPECT_EQ(offset1_expected, 0);
    size_t offset2_expected = normal_field.size();
    size_t offset3_expected = normal_field.size() + empty_field1.size();
    EXPECT_EQ(offset2_expected, offset3_expected);
    size_t offset4_expected =
        normal_field.size() + empty_field1.size() + another_field.size();

    // 验证偏移量编码
    unsigned int offset1_types, offset2_types, offset3_types, offset4_types;
    unsigned long offset1, offset2, offset3, offset4;
    offset4 = utils::decode_uleb128(p, &offset4_types);
    p += offset4_types;
    EXPECT_EQ(offset4, offset4_expected);
    EXPECT_EQ(offset4_types, utils::uleb128_length(offset4));
    offset3 = utils::decode_uleb128(p, &offset3_types);
    p += offset3_types;
    EXPECT_EQ(offset3, offset3_expected);
    EXPECT_EQ(offset3_types, utils::uleb128_length(offset3));
    offset2 = utils::decode_uleb128(p, &offset2_types);
    p += offset2_types;
    EXPECT_EQ(offset2, offset2_expected);
    EXPECT_EQ(offset2_types, utils::uleb128_length(offset2));
    offset1 = utils::decode_uleb128(p, &offset1_types);
    p += offset1_types;
    EXPECT_EQ(offset1, offset1_expected);
    EXPECT_EQ(offset1_types, utils::uleb128_length(offset1));

    // 验证字段数据
    // 字段1（正常字段）
    EXPECT_EQ(std::memcmp(p, normal_field.data(), normal_field.size()), 0);
    p += normal_field.size();

    // 字段2（空字段）
    EXPECT_EQ(std::memcmp(p, empty_field1.data(), empty_field1.size()), 0);
    p += empty_field1.size();

    // 字段3（正常字段）
    EXPECT_EQ(std::memcmp(p, another_field.data(), another_field.size()), 0);
    p += another_field.size();

    // 字段4（空字段）
    EXPECT_EQ(std::memcmp(p, empty_field2.data(), empty_field2.size()), 0);
    p += empty_field2.size();

    // 验证总长度计算正确
    size_t fields_total_size = empty_field1.size() + normal_field.size() +
                               empty_field2.size() + another_field.size();
    size_t offsets_encoding_length =
        utils::uleb128_length(offset1) + utils::uleb128_length(offset2) +
        utils::uleb128_length(offset3) + utils::uleb128_length(offset4);
    size_t expected_total_length =
        fields_total_size + offsets_encoding_length + length_bytes;

    EXPECT_EQ(payload.get_length(), expected_total_length);
}

// 测试大数值的uleb128编码
TEST_F(RecordPayloadTest, LargeValueUleb128Encoding)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建需要多字节uleb128编码的字段（大于127字节）
    char *large_field_data = arena->allocate(200);
    ::memset(large_field_data, 'X', 200);
    wf::utils::char_slice large_field(large_field_data, 200);

    tuple.emplace_back(large_field);

    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证总长度使用多字节uleb128编码
    unsigned char *p = payload.get_payload();
    unsigned int length_bytes;
    unsigned long decoded_length = utils::decode_uleb128(p, &length_bytes);

    EXPECT_EQ(decoded_length, 200);
    EXPECT_GT(length_bytes, 1); // 应该使用多字节编码
}

// 测试字段数据完整性
TEST_F(RecordPayloadTest, FieldDataIntegrity)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建包含特殊字符的字段
    const char *special_data = "field\0with\0null\0bytes";
    size_t special_size = 21; // 包含空字节的长度
    wf::utils::char_slice special_field(special_data, special_size);

    tuple.emplace_back(special_field);

    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证字段数据完整拷贝（包括空字节）
    unsigned char *p = payload.get_payload();
    unsigned int length_bytes;
    unsigned long decoded_length = utils::decode_uleb128(p, &length_bytes);
    p += length_bytes; // 跳过总长度
    EXPECT_EQ(decoded_length, special_size);

    unsigned int offset_bytes;
    unsigned long offset_decoded = utils::decode_uleb128(p, &offset_bytes);
    p += offset_bytes; // 跳过偏移量编码
    EXPECT_EQ(offset_decoded, 0);

    // 直接比较内存内容
    EXPECT_EQ(::memcmp(p, special_data, special_size), 0);
}

// 测试记录格式的边界情况
TEST_F(RecordPayloadTest, RecordFormatBoundary)
{
    // 测试刚好能容纳记录的缓冲区
    size_t field_size = 160;
    size_t header_size = utils::uleb128_length(field_size); // 总长度编码大小
    size_t total_size = header_size + field_size + utils::uleb128_length(0);

    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    char *field_data = arena->allocate(field_size);
    memset(field_data, 'B', field_size);
    wf::utils::char_slice field(field_data, field_size);
    tuple.emplace_back(field);

    error_info result = payload.encode_tuple(tuple);
    EXPECT_EQ(result, utils::OK);

    // 验证记录长度计算正确
    EXPECT_EQ(payload.get_length(), total_size);
}

// 测试decode_record的基本功能
TEST_F(RecordPayloadTest, DecodeRecordBasic)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建测试字段并编码
    wf::utils::char_slice field1("field1", 6);
    wf::utils::char_slice field2("field2_data", 11);
    wf::utils::char_slice field3("field3_long", 11);

    tuple.emplace_back(field1);
    tuple.emplace_back(field2);
    tuple.emplace_back(field3);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 解码记录
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, utils::OK);

    // 验证解码后的字段数量和内容
    EXPECT_EQ(decoded_tuple.size(), tuple.size());

    // 验证每个字段的内容
    EXPECT_EQ(decoded_tuple[0].ToString(), field1.ToString());
    EXPECT_EQ(decoded_tuple[1].ToString(), field2.ToString());
    EXPECT_EQ(decoded_tuple[2].ToString(), field3.ToString());
}

// 测试单字段记录的解码
TEST_F(RecordPayloadTest, DecodeSingleFieldRecord)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 单字段记录
    wf::utils::char_slice field("single_field_data", 17);
    tuple.emplace_back(field);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 解码记录
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, utils::OK);

    // 验证解码结果
    EXPECT_EQ(decoded_tuple.size(), 1);
    EXPECT_EQ(decoded_tuple[0].ToString(), field.ToString());
}

// 测试包含空字段的记录解码
TEST_F(RecordPayloadTest, DecodeRecordWithEmptyFields)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 包含空字段的记录
    wf::utils::char_slice normal("normal_data", 11);
    wf::utils::char_slice empty1;
    wf::utils::char_slice another("another", 7);
    wf::utils::char_slice empty2("", 0);

    tuple.emplace_back(normal);
    tuple.emplace_back(empty1);
    tuple.emplace_back(another);
    tuple.emplace_back(empty2);
    EXPECT_EQ(tuple.size(), 4);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 解码记录
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, utils::OK);

    // 验证解码结果
    EXPECT_EQ(decoded_tuple.size(), tuple.size());

    // 验证正常字段
    EXPECT_EQ(decoded_tuple[0].ToString(), normal.ToString());

    // 验证空字段
    EXPECT_TRUE(decoded_tuple[1].empty());
    EXPECT_EQ(decoded_tuple[1].size(), 0);

    // 验证最后一个字段
    EXPECT_EQ(decoded_tuple[2].ToString(), another.ToString());

    // 验证另一个空字段
    EXPECT_TRUE(decoded_tuple[3].empty());
    EXPECT_EQ(decoded_tuple[3].size(), 0);
}

// 测试大字段记录的解码
TEST_F(RecordPayloadTest, DecodeLargeFieldRecord)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建大字段
    char *large_data = arena->allocate(512);
    memset(large_data, 'L', 512);
    wf::utils::char_slice large_field(large_data, 512);
    tuple.emplace_back(large_field);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 解码记录
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, utils::OK);

    // 验证解码结果
    EXPECT_EQ(decoded_tuple.size(), 1);
    EXPECT_EQ(decoded_tuple[0].size(), 512);

    // 验证大字段内容
    const char *decoded_data = decoded_tuple[0].data();
    for (size_t i = 0; i < 512; ++i) {
        EXPECT_EQ(decoded_data[i], 'L');
    }
}

// 测试多字段混合大小的记录解码
TEST_F(RecordPayloadTest, DecodeMixedSizeFields)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 混合大小的字段
    wf::utils::char_slice small("a", 1);
    wf::utils::char_slice medium("medium_field", 12);
    wf::utils::char_slice large("large_field_data_here", 21);

    tuple.emplace_back(small);
    tuple.emplace_back(medium);
    tuple.emplace_back(large);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 解码记录
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, utils::OK);

    // 验证解码结果
    EXPECT_EQ(decoded_tuple.size(), 3);
    EXPECT_EQ(decoded_tuple[0].ToString(), small.ToString());
    EXPECT_EQ(decoded_tuple[1].ToString(), medium.ToString());
    EXPECT_EQ(decoded_tuple[2].ToString(), large.ToString());
}

// 测试错误情况：空payload
TEST_F(RecordPayloadTest, DecodeNullPayload)
{
    record_payload payload;
    payload.attatch_payload(nullptr);
    payload.set_length(100);
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 解码空payload应该失败
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, kErrorNullPayload);
}

// 测试编码解码的往返一致性
TEST_F(RecordPayloadTest, EncodeDecodeRoundTrip)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);

    // 创建复杂的测试数据
    logical_tuple original_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    wf::utils::char_slice field1("first_field", 11);
    wf::utils::char_slice field2("", 0); // 空字段
    wf::utils::char_slice field3("third_field_with_data", 22);
    wf::utils::char_slice field4("last", 4);

    original_tuple.emplace_back(field1);
    original_tuple.emplace_back(field2);
    original_tuple.emplace_back(field3);
    original_tuple.emplace_back(field4);

    // 编码
    error_info encode_result = payload.encode_tuple(original_tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 解码
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, utils::OK);

    // 验证往返一致性
    EXPECT_EQ(decoded_tuple.size(), original_tuple.size());

    for (size_t i = 0; i < original_tuple.size(); ++i) {
        EXPECT_EQ(decoded_tuple[i].ToString(), original_tuple[i].ToString());
        EXPECT_EQ(decoded_tuple[i].size(), original_tuple[i].size());
    }
}

// 测试包含特殊字符的字段解码
TEST_F(RecordPayloadTest, DecodeRecordWithSpecialCharacters)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 包含特殊字符的字段
    const char *special_data = "field\0with\0null\0bytes";
    size_t special_size = 21; // 包含空字节的长度
    wf::utils::char_slice special_field(special_data, special_size);
    tuple.emplace_back(special_field);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 解码记录
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, utils::OK);

    // 验证包含特殊字符的字段
    EXPECT_EQ(decoded_tuple.size(), 1);
    EXPECT_EQ(decoded_tuple[0].size(), special_size);

    // 验证内存内容一致（包括空字节）
    EXPECT_EQ(
        std::memcmp(decoded_tuple[0].data(), special_data, special_size), 0);
}

// 测试retrieve_field的基本功能
TEST_F(RecordPayloadTest, RetrieveFieldBasic)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建测试字段
    wf::utils::char_slice field1("first_field", 11);
    wf::utils::char_slice field2("second_field_data", 17);
    wf::utils::char_slice field3("third_field", 11);

    tuple.emplace_back(field1);
    tuple.emplace_back(field2);
    tuple.emplace_back(field3);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 测试检索各个字段
    wf::utils::char_slice retrieved_field;

    // 检索第一个字段
    error_info result1 = payload.retrieve_field(0, retrieved_field, *arena);
    EXPECT_EQ(result1, utils::OK);
    EXPECT_EQ(retrieved_field.ToString(), field1.ToString());
    EXPECT_EQ(retrieved_field.size(), field1.size());

    // 检索第二个字段
    error_info result2 = payload.retrieve_field(1, retrieved_field, *arena);
    EXPECT_EQ(result2, utils::OK);
    EXPECT_EQ(retrieved_field.ToString(), field2.ToString());
    EXPECT_EQ(retrieved_field.size(), field2.size());

    // 检索第三个字段
    error_info result3 = payload.retrieve_field(2, retrieved_field, *arena);
    EXPECT_EQ(result3, utils::OK);
    EXPECT_EQ(retrieved_field.ToString(), field3.ToString());
    EXPECT_EQ(retrieved_field.size(), field3.size());
}

// 测试retrieve_field的边界情况：索引越界
TEST_F(RecordPayloadTest, RetrieveFieldIndexOutOfBounds)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建单字段记录
    wf::utils::char_slice field("test_field", 10);
    tuple.emplace_back(field);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 测试索引越界
    wf::utils::char_slice retrieved_field;

    // 有效索引：0
    error_info valid_result =
        payload.retrieve_field(0, retrieved_field, *arena);
    EXPECT_EQ(valid_result, utils::OK);

    // 无效索引：1（越界）
    error_info invalid_result =
        payload.retrieve_field(1, retrieved_field, *arena);
    EXPECT_NE(invalid_result, utils::OK);

    // 无效索引：非常大的索引
    error_info large_index_result =
        payload.retrieve_field(1000, retrieved_field, *arena);
    EXPECT_NE(large_index_result, utils::OK);
}

// 测试retrieve_field处理空字段
TEST_F(RecordPayloadTest, RetrieveFieldWithEmptyFields)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建包含空字段的记录
    wf::utils::char_slice normal("normal_data", 11);
    wf::utils::char_slice empty1;
    wf::utils::char_slice empty2;

    tuple.emplace_back(normal);
    tuple.emplace_back(empty1);
    tuple.emplace_back(empty2);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 测试检索空字段
    wf::utils::char_slice retrieved_field;

    // 检索正常字段
    error_info result2 = payload.retrieve_field(0, retrieved_field, *arena);
    EXPECT_EQ(result2, utils::OK);
    EXPECT_EQ(retrieved_field.ToString(), normal.ToString());

    // 检索第一个空字段
    error_info result1 = payload.retrieve_field(1, retrieved_field, *arena);
    EXPECT_EQ(result1, utils::OK);
    EXPECT_TRUE(retrieved_field.empty());
    EXPECT_EQ(retrieved_field.size(), 0);

    // 检索第二个空字段
    error_info result3 = payload.retrieve_field(2, retrieved_field, *arena);
    EXPECT_EQ(result3, utils::OK);
    EXPECT_TRUE(retrieved_field.empty());
    EXPECT_EQ(retrieved_field.size(), 0);
}

// 测试retrieve_field处理大字段
TEST_F(RecordPayloadTest, RetrieveFieldLargeField)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建大字段
    char *large_data = arena->allocate(1000);
    memset(large_data, 'X', 1000);
    wf::utils::char_slice large_field(large_data, 1000);
    tuple.emplace_back(large_field);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 检索大字段
    wf::utils::char_slice retrieved_field;
    error_info result = payload.retrieve_field(0, retrieved_field, *arena);
    EXPECT_EQ(result, utils::OK);

    // 验证大字段内容
    EXPECT_EQ(retrieved_field.size(), 1000);
    const char *retrieved_data = retrieved_field.data();
    for (size_t i = 0; i < 1000; ++i) {
        EXPECT_EQ(retrieved_data[i], 'X');
    }
}

// 测试retrieve_field与decode_record的一致性
TEST_F(RecordPayloadTest, RetrieveFieldConsistencyWithDecode)
{
    record_payload payload;
    payload.attatch_payload(buffer);
    payload.set_length(buffer_size);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建复杂测试数据
    wf::utils::char_slice field1("field1", 6);
    wf::utils::char_slice field2("", 0); // 空字段
    wf::utils::char_slice field3("field3_with_data", 16);
    wf::utils::char_slice field4("last", 4);

    tuple.emplace_back(field1);
    tuple.emplace_back(field2);
    tuple.emplace_back(field3);
    tuple.emplace_back(field4);

    // 编码元组
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, utils::OK);

    // 方法1：使用retrieve_field逐个检索
    wf::utils::char_slice retrieved_field;

    error_info result1 = payload.retrieve_field(0, retrieved_field, *arena);
    EXPECT_EQ(result1, utils::OK);
    EXPECT_EQ(retrieved_field.ToString(), field1.ToString());

    error_info result2 = payload.retrieve_field(1, retrieved_field, *arena);
    EXPECT_EQ(result2, utils::OK);
    EXPECT_TRUE(retrieved_field.empty());

    error_info result3 = payload.retrieve_field(2, retrieved_field, *arena);
    EXPECT_EQ(result3, utils::OK);
    EXPECT_EQ(retrieved_field.ToString(), field3.ToString());

    error_info result4 = payload.retrieve_field(3, retrieved_field, *arena);
    EXPECT_EQ(result4, utils::OK);
    EXPECT_EQ(retrieved_field.ToString(), field4.ToString());

    // 方法2：使用decode_record整体解码
    logical_tuple decoded_tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};
    error_info decode_result = payload.decode_record(decoded_tuple);
    EXPECT_EQ(decode_result, utils::OK);

    // 验证两种方法结果一致
    EXPECT_EQ(decoded_tuple.size(), 4);
    EXPECT_EQ(decoded_tuple[0].ToString(), field1.ToString());
    EXPECT_TRUE(decoded_tuple[1].empty());
    EXPECT_EQ(decoded_tuple[2].ToString(), field3.ToString());
    EXPECT_EQ(decoded_tuple[3].ToString(), field4.ToString());
}

// 测试retrieve_field的错误情况：空payload
TEST_F(RecordPayloadTest, RetrieveFieldNullPayload)
{
    record_payload payload;
    payload.attatch_payload(nullptr);
    payload.set_length(100);
    wf::utils::char_slice retrieved_field;

    // 检索空payload应该失败
    error_info result = payload.retrieve_field(0, retrieved_field, *arena);
    EXPECT_NE(result, kErrorTupleSizeIsZero);
}

// 测试retrieve_field的错误情况：payload长度不足
TEST_F(RecordPayloadTest, RetrieveFieldPayloadLengthNotEnough)
{
    // 创建一个小缓冲区
    unsigned char small_buffer[10];
    record_payload payload;
    payload.attatch_payload(small_buffer);
    payload.set_length(10);
    logical_tuple tuple{
        wf::utils::arena_allocator<wf::utils::char_slice>(arena)};

    // 创建一个需要更大空间的字段
    wf::utils::char_slice large_field("this_field_is_too_large", 22);
    tuple.emplace_back(large_field);

    // 编码应该成功（因为encode_tuple不检查缓冲区大小）
    error_info encode_result = payload.encode_tuple(tuple);
    EXPECT_EQ(encode_result, kErrorPayloadLengthNotEnough);
}

} // namespace wf::storage

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}