/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License Version 2.0 with LLVM Exceptions
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *   https://llvm.org/LICENSE.txt
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <type_traits>
#include <functional>
#include "static_hash.h"

namespace wf::meta {

struct type_index final
{
    // 编译期获取类型名称
    constexpr const char *name() const noexcept { return index_; }

    // 编译期哈希值，确保跨编译单元一致性
    constexpr std::size_t hash_code() const noexcept
    {
        return hash_string(index_);
    }

  private:
    const char *index_;

    // 标记构造函数为constexpr
    explicit constexpr type_index(const char *index) noexcept
        : index_(index)
    {}

    // 标记所有比较操作符为constexpr
    friend constexpr bool operator==(type_index lhs, type_index rhs) noexcept
    {
        // 使用字符串内容比较而不是指针比较
        // 这样即使__PRETTY_FUNCTION__不同，只要类型名称相同就认为相等
        return hash_string(lhs.index_) == hash_string(rhs.index_);
    }
    friend constexpr bool operator!=(type_index lhs, type_index rhs) noexcept
    {
        return !(lhs == rhs);
    }
    friend constexpr bool operator<(type_index lhs, type_index rhs) noexcept
    {
        // 使用字符串比较而不是指针比较，确保编译期可用
        return hash_string(lhs.index_) < hash_string(rhs.index_);
    }
    friend constexpr bool operator>(type_index lhs, type_index rhs) noexcept
    {
        return rhs < lhs;
    }
    friend constexpr bool operator<=(type_index lhs, type_index rhs) noexcept
    {
        return !(lhs > rhs);
    }
    friend constexpr bool operator>=(type_index lhs, type_index rhs) noexcept
    {
        return !(rhs < lhs);
    }

    template <typename T>
    friend constexpr type_index type_id() noexcept;
};

template <typename T>
constexpr type_index type_id() noexcept
{
    // __PRETTY_FUNCTION__指向的是type_id<T>()函数
    return type_index{__PRETTY_FUNCTION__};
}

} // namespace wf::meta

namespace std {

template <>
struct hash<wf::meta::type_index>
{
    constexpr std::size_t operator()(wf::meta::type_index index) const noexcept
    {
        return index.hash_code();
    }
};

} // namespace std