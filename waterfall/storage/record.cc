////
// @file record.cc
// @brief
// 实现记录
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <endian.h>
#include <waterfall/utils/leb128.h>
#include "error.h"
#include "record.h"

namespace wf::storage {

void record_payload::clear()
{
    payload_ = nullptr;
    length_ = 0;
}

error_info record_payload::required_size(const logical_tuple &t, size_t &length)
{
    if (t.size() == 0) return kErrorTupleSizeIsZero;
    if (t[0].size() == 0) return kErrorFirstFieldIsEmpty;

    // 所有字段长度
    length = sizeof_tuple(t);

    // 加上所有字段长度的编码长度
    length += utils::uleb128_length(length);

    // 加上所有字段偏移量的编码长度
    size_t offset = 0;
    size_t offset_length = utils::uleb128_length(offset); // 第1个字段偏移量为0
    auto end = t.end();
    --end; // 最后一个字段
    for (auto it = t.begin(); it != end; ++it) {
        offset += it->size();
        offset_length += utils::uleb128_length(offset);
    }
    length += offset_length;
    return utils::OK;
}

error_info record_payload::encode_tuple(const logical_tuple &t)
{
    if (payload_ == nullptr) return kErrorNullPayload;
    if (t.size() == 0) return kErrorTupleSizeIsZero;
    if (t[0].size() == 0) return kErrorFirstFieldIsEmpty;

    // 所有字段长度
    size_t length = sizeof_tuple(t);
    // 加上所有字段长度的编码长度
    size_t tuple_length = length;
    length += utils::uleb128_length(length);
    // 加上所有字段偏移量的编码长度
    size_t offset = 0;
    size_t offset_length = utils::uleb128_length(offset); // 第1个字段偏移量为0
    auto end = t.end();
    --end; // 最后一个字段
    for (auto it = t.begin(); it != end; ++it) {
        offset += it->size();
        offset_length += utils::uleb128_length(offset);
    }
    length += offset_length;
    if (length > length_) return kErrorPayloadLengthNotEnough;

    unsigned char *p = payload_;
    // 编码所有字段长度
    p += utils::encode_uleb128(tuple_length, p);
    // 编码所有字段偏移量
    p += utils::encode_uleb128(offset, p);
    auto rit = t.rbegin();
    ++rit; // 去掉最后一个字段
    for (; rit != t.rend(); ++rit) {
        offset -= rit->size();
        p += utils::encode_uleb128(offset, p);
    }
    // 拷贝所有字段数据
    for (const auto &slice : t) {
        ::memcpy(p, slice.data(), slice.size());
        p += slice.size();
    }

    length_ = length;
    return utils::OK;
}

error_info record_payload::decode_fields_(
    unsigned char *&p,
    std::vector<size_t> &field_offsets,
    std::vector<size_t> &field_sizes,
    size_t &offset_length,
    size_t total_fields_size)
{
    error_info err = utils::OK;
    unsigned int n = 0;
    size_t offset;

    // 解码所有字段偏移量
    do {
        p += n;
        offset = utils::decode_uleb128(p, &n, nullptr, &err);
        if (err != utils::OK) return err;
        offset_length += n;
        field_offsets.emplace_back(offset);
    } while (offset); // 第1个字段偏移量为0
    ++p; // 第1个字段的偏移量

    // 计算各字段大小
    size_t last_size = total_fields_size - field_offsets[0];
    field_sizes.emplace_back(last_size);
    auto it = field_offsets.begin();
    auto next = it;
    ++next;
    for (; next != field_offsets.end(); ++it, ++next)
        field_sizes.emplace_back(*it - *next);

    return err;
}

error_info record_payload::decode_record(logical_tuple &t)
{
    if (payload_ == nullptr) return kErrorNullPayload;
    t.clear(); // 清空元组

    // 解码payload总长度
    unsigned char *p = payload_;
    unsigned int n = 0;
    error_info err = utils::OK;
    size_t fields_total_size = utils::decode_uleb128(p, &n, nullptr, &err);
    if (err != utils::OK) return err;
    size_t field_total_size_length = n;
    p += n;

    // 解码所有字段长度
    std::vector<size_t> field_offsets; // 倒排所有字段偏移量
    std::vector<size_t> field_sizes;   // 所有字段大小，倒排
    size_t offset_length = 0;
    err = decode_fields_(
        p, field_offsets, field_sizes, offset_length, fields_total_size);
    if (err != utils::OK) return err;

    // 检查payload长度是否足够
    size_t length = fields_total_size + offset_length + field_total_size_length;
    if (length > length_)
        return kErrorPayloadLengthNotEnough;
    else
        length_ = length; // 将payload长度更新为实际长度

    // 拷贝所有字段数据
    auto rit1 = field_offsets.rbegin();
    auto rit2 = field_sizes.rbegin();
    for (; rit1 != field_offsets.rend(); ++rit1, ++rit2) {
        char_slice slice((char *) p + *rit1, *rit2);
        t.emplace_back(slice);
    }

    return utils::OK;
}

error_info record_payload::retrieve_field(
    size_t index,
    char_slice &field,
    utils::object_arena &arena)
{
    if (payload_ == nullptr) return kErrorNullPayload;

    // 解码payload总长度
    unsigned char *p = payload_;
    unsigned int n = 0;
    error_info err = utils::OK;
    size_t fields_total_size = utils::decode_uleb128(p, &n, nullptr, &err);
    if (err != utils::OK) return err;
    size_t field_total_size_length = n;
    p += n;

    // 解码所有字段长度
    std::vector<size_t> field_offsets; // 倒排所有字段偏移量
    std::vector<size_t> field_sizes;   // 所有字段大小，倒排
    size_t offset_length = 0;
    err = decode_fields_(
        p, field_offsets, field_sizes, offset_length, fields_total_size);
    if (err != utils::OK) return err;

    // 检查payload长度是否足够
    size_t length = fields_total_size + offset_length + field_total_size_length;
    if (length > length_) return kErrorPayloadLengthNotEnough;

    // 检查索引是否合法
    if (index >= field_offsets.size()) return kErrorInvalidIndex;
    index = field_offsets.size() - 1 - index; // 转换为倒排位置

    // 拷贝字段数据
    if (field_sizes[index]) { // 仅当非空字段时才拷贝数据
        char *field_data = arena.allocate(field_sizes[index]);
        ::memcpy(
            field_data, (char *) p + field_offsets[index], field_sizes[index]);
        field = char_slice(field_data, field_sizes[index]);
    } else
        field.clear(); // 空字段，清空slice

    return utils::OK;
}

void physical_record::clear()
{
    record_ = nullptr;
    size_ = 0;
}

error_info physical_record::attach_record(unsigned char *record)
{
    if (reinterpret_cast<uintptr_t>(record) % kRecordAlignment != 0)
        return kErrorUnalignedRecord;
    record_ = record;
    return utils::OK;
}

void physical_record::set_header(unsigned short header)
{
    unsigned short *p = (unsigned short *) record_;
    p[0] = htole16(header); // 主机到小序
}

unsigned short physical_record::get_header() const
{
    unsigned short *p = (unsigned short *) record_;
    return le16toh(p[0]); // 小序到主机
}

error_info physical_record::get_overflow(record_id_t &overflow)
{
    if (!is_overflow()) return kErrorNotOverflow;
    unsigned short *pshort = (unsigned short *) record_;
    overflow.offset = le16toh(pshort[1]);
    ::memcpy(&overflow.page_id, &pshort[2], sizeof(page_id_t));
    overflow.page_id = le64toh(overflow.page_id);
    return utils::OK;
}

error_info physical_record::set_overflow(const record_id_t &overflow)
{
    if (!is_overflow()) return kErrorNotOverflow;
    unsigned short *pshort = (unsigned short *) record_;
    pshort[1] = htole16(overflow.offset);
    page_id_t page = htole32(overflow.page_id);
    ::memcpy(&pshort[2], &page, sizeof(page_id_t));
    return utils::OK;
}

error_info physical_record::get_version(schema::scn_t &version)
{
    if (!is_versioned()) return kErrorNotVersioned;
    size_t offset = sizeof(unsigned short);
    if (is_overflow()) offset += sizeof(unsigned short) + sizeof(page_id_t);
    ::memcpy(&version, record_ + offset, sizeof(schema::scn_t));
    version = le64toh(version);
    return utils::OK;
}

error_info physical_record::set_version(schema::scn_t version)
{
    if (!is_versioned()) return kErrorNotVersioned;
    size_t offset = sizeof(unsigned short);
    if (is_overflow()) offset += sizeof(unsigned short) + sizeof(page_id_t);
    version = htole64(version);
    ::memcpy(record_ + offset, &version, sizeof(schema::scn_t));
    return utils::OK;
}

unsigned char *physical_record::get_payload() const
{
    size_t offset = sizeof(unsigned short);
    if (is_overflow()) offset += sizeof(unsigned short) + sizeof(page_id_t);
    if (is_versioned()) offset += sizeof(schema::scn_t);
    return record_ + offset;
}

error_info physical_record::required_size(
    unsigned short header,
    const logical_tuple &t,
    size_t &size)
{
    // header
    size = sizeof(unsigned short);

    // payload
    size_t payload_size;
    error_info err = record_payload::required_size(t, payload_size);
    if (err != utils::OK) return 0;
    size += payload_size;

    // overflow
    if (header & kRecordHeaderOverflow)
        size += sizeof(unsigned short) + sizeof(page_id_t);

    // version
    if (header & kRecordHeaderVersioned) size += sizeof(schema::scn_t);

    // 对齐8B
    size = (size + kRecordAlignment - 1) & ~(kRecordAlignment - 1);

    return utils::OK;
}

} // namespace wf::storage