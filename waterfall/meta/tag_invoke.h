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

#include "quote.h"

namespace wf::meta {

// 标签调用总入口
namespace _tag_invoke {
void tag_invoke(); // 防止上溯到wf::meta名字空间

template <typename CPO, typename... Args>
concept _is_cpo_operator_v =
    requires(CPO &&cpo, Args &&...args) { cpo((Args &&) args...); };

struct _fn
{
    template <typename CPO, typename... Args>
        requires (_is_cpo_operator_v<CPO, Args...>)
    constexpr auto operator()(CPO cpo, Args &&...args) const
        noexcept(noexcept(cpo((Args &&) args...)))
            -> decltype(cpo((Args &&) args...))
    {
        return cpo((Args &&) args...);
    }
    template <typename CPO, typename... Args>
        requires (!_is_cpo_operator_v<CPO, Args...>)
    constexpr auto operator()(CPO cpo, Args &&...args) const
        noexcept(noexcept(tag_invoke((CPO &&) cpo, (Args &&) args...)))
            -> decltype(tag_invoke((CPO &&) cpo, (Args &&) args...))
    {
        return tag_invoke((CPO &&) cpo, (Args &&) args...);
    }
};

namespace _cpo {
inline constexpr _fn tag_invoke{};
} // namespace _cpo

} // namespace _tag_invoke
using namespace _tag_invoke::_cpo;

template <auto &CPO>
using tag_t = std::remove_cvref_t<decltype(CPO)>;

// 检查CPO是否可调用tag_invoke
template <typename CPO, typename... Args>
concept is_tag_invocable_v =
    std::is_invocable_v<decltype(tag_invoke), CPO, Args...>;
// 检查CPO调用tag_invoke是否为nothrow
template <typename CPO, typename... Args>
concept is_tag_nothrow_invocable_v =
    std::is_nothrow_invocable_v<decltype(tag_invoke), CPO, Args...>;
// 直接使用CPO的可调用性检查，而不是通过tag_invoke转发
template <typename CPO, typename... Args>
using tag_invoke_result_t =
    std::invoke_result_t<decltype(tag_invoke), CPO, Args...>;
// 绑定CPO，成为一个元函数
template <typename CPO>
struct meta_tag_invoke_result
{
    template <typename... Args>
    using apply = tag_invoke_result_t<CPO, Args...>;
};

// 判断cpo是否是算子
template <typename CPO, typename... Args>
concept is_cpo_operator_v = _tag_invoke::_is_cpo_operator_v<CPO, Args...>;

} // namespace wf::meta
