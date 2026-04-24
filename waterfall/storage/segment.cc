////
// @file segment.cc
// @brief
// 实现段
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include "segment.h"

namespace wf::storage {

utils::char_slice segment_bitmap::serialize_to_slice(
    error_info &error,
    utils::object_arena &arena)
{
    if (bitmap_ == nullptr) {
        error = kErrorNullBitmap;
        return {};
    }

    // 计算序列化后的大小
    size_t size = ::roaring_bitmap_size_in_bytes(bitmap_);
    // 分配缓冲区
    char *buffer = arena.allocate(size);

    // 序列化
    ::roaring_bitmap_serialize(bitmap_, buffer);

    // 存储到slice
    return {buffer, size};
}

} // namespace wf::storage
