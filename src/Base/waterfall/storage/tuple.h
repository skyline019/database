////
// @file tuple.h
// @brief
// 逻辑元组
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <waterfall/utils/slice.h>
#include <waterfall/utils/arena.h>

namespace wf::storage {

using char_slice = wf::utils::char_slice;

// 逻辑元组
using logical_tuple = std::vector<
    char_slice,                              // slice作为元组的元素
    wf::utils::arena_allocator<char_slice>>; // 元组的元素分配器

// 添加一个字段到元组
inline void append_tuple(logical_tuple &tuple, char_slice slice)
{
    tuple.emplace_back(slice);
}

// 计算元组的大小
inline size_t sizeof_tuple(const logical_tuple &tuple)
{
    size_t size = 0;
    for (const auto &slice : tuple)
        size += slice.size();
    return size;
}

} // namespace wf::storage