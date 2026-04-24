////
// @file segment.h
// @brief
// 段
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <roaring/roaring.h>
#include <waterfall/utils/slice.h>
#include <waterfall/utils/arena.h>
#include "error.h"
#include "page.h"

namespace wf::storage {

// 段位图
class segment_bitmap
{
  private:
    roaring_bitmap_t *bitmap_; // 页面位图

  public:
#if 0
    // 序列化到record
    error_info serialize_to_record(record_payload &record)
    {

    }

    // 从record反序列化
    error_info deserialize_from_record(const record_payload &record)
    {
        if (bitmap_ != nullptr) { roaring_bitmap_free(bitmap_); }

        // 从record读取数据
        const char *data = record.get_data();
        size_t size = record.get_length();

        if (data == nullptr || size == 0) {
            bitmap_ = roaring_bitmap_create();
            return utils::OK;
        }

        // 反序列化
        bitmap_ = roaring_bitmap_deserialize(data);
        if (bitmap_ == nullptr) { return kErrorDeserializationFailed; }

        return utils::OK;
    }
#endif
    // clang-format off
    void add_page(page_id_t page_id) { ::roaring_bitmap_add(bitmap_, page_id); }
    void del_page(page_id_t page_id) { ::roaring_bitmap_remove(bitmap_, page_id); }
    // clang-format on

    utils::char_slice
    serialize_to_slice(error_info &error, utils::object_arena &arena);
    error_info deserialize_from_slice(const utils::char_slice &slice);
};

} // namespace wf::storage