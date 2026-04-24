////
// @file record.h
// @brief
// 定义记录record，用于存储元组tuple
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <stddef.h>
#include <assert.h>
#include <vector>
#include <waterfall/utils/slice.h>
#include <waterfall/utils/arena.h>
#include <waterfall/schema/scn.h>
#include "tuple.h"
#include "page.h"
#include "error.h"

namespace wf::storage {

static const size_t kRecordAlignment = 8; // 记录对齐字节数

// 记录头1B
// deleted
static const unsigned short kRecordHeaderTomstone = 0b1000000000000000;
// overflow
static const unsigned short kRecordHeaderOverflow = 0b0100000000000000;
// version
static const unsigned short kRecordHeaderVersioned = 0b0010000000000000;
// btree
static const unsigned short kRecordHeaderBtree = 0b0001000000000000;

// record_id，记录ID，用于堆存储
struct record_id_t
{
    page_offset_t offset; // 记录偏移量
    page_id_t page_id;    // 页面ID
};

// 记录载荷分为三部分：
// 1. [总长度(uleb128)]
// 2. [字段n偏移量(uleb128)] ... [字段1偏移量(uleb128)]
// 3. [字段1数据] ... [字段n数据]
// NOTE: 不允许空tuple，并且第1个字段不能为空
class record_payload
{
  private:
    unsigned char *payload_; // 指向记录载荷位置
    size_t length_;          // 载荷大小

  public:
    record_payload() = default;
    void clear();

    void attatch_payload(unsigned char *payload) { payload_ = payload; }
    unsigned char *get_payload() const { return payload_; }

    void set_length(size_t length) { length_ = length; }
    size_t get_length() const { return length_; }

    static error_info required_size(const logical_tuple &t, size_t &size);
    error_info encode_tuple(const logical_tuple &t);
    error_info decode_record(logical_tuple &t);
    error_info
    retrieve_field(size_t index, char_slice &field, utils::object_arena &arena);

  private:
    error_info decode_fields_(
        unsigned char *&p,                  // 字段起始位置
        std::vector<size_t> &field_offsets, // 字段倒排偏移量
        std::vector<size_t> &field_sizes,   // 所有字段长度，倒排
        size_t &offset_length,              // 偏移量数组总长度
        size_t total_fields_size);          // 所有字段总长度
};

// 物理记录，可选overflow，可选versioned
// 1. 记录头部2B
// 2. overflow(6B) = offset(2B) + page_id(4B)
// 3. version(8B)
// 4. payload
class physical_record
{
  private:
    unsigned char *record_; // 指向页面中的记录数据
    size_t size_;           // 记录大小

  public:
    physical_record() = default;

    void clear();

    error_info attach_record(unsigned char *record);
    const unsigned char *get_record() const { return record_; }

    void set_size(size_t size) { size_ = size; }
    size_t get_size() const { return size_; }

    void set_header(unsigned short header);
    unsigned short get_header() const;

    // clang-format off
    bool is_tomstone() const { return (get_header() & kRecordHeaderTomstone); }
    bool is_overflow() const { return (get_header() & kRecordHeaderOverflow); }
    bool is_versioned() const { return (get_header() & kRecordHeaderVersioned); }
    bool is_btree() const { return (get_header() & kRecordHeaderBtree); }

    void set_tomstone() { set_header(get_header() | kRecordHeaderTomstone); }
    void set_overflow() { set_header(get_header() | kRecordHeaderOverflow); }
    void set_versioned() { set_header(get_header() | kRecordHeaderVersioned); }
    void set_btree() { set_header(get_header() | kRecordHeaderBtree); }
    // clang-format on

    error_info get_overflow(record_id_t &overflow);
    error_info set_overflow(const record_id_t &overflow);

    error_info get_version(schema::scn_t &version);
    error_info set_version(schema::scn_t version);

    // 获取记录载荷
    unsigned char *get_payload() const;
    // 计算元组所需大小
    static error_info
    required_size(unsigned short header, const logical_tuple &t, size_t &size);
};

} // namespace wf::storage
