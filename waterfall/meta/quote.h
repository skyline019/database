////
// @file quote.h
// @brief
// 引用模板成为高阶元函数 - 统一版本
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <type_traits>

namespace wf::meta {

// 统一的引用模板，将任意模板转换成一个高阶元函数
// 支持以下模板模式：
// - template <typename...> class F
// - template <typename, typename...> class F
// - template <typename, typename, typename...> class F
// - template <typename, typename, typename, typename...> class F
template <template <typename...> class F>
struct quote
{
    // 全参数apply - 支持任意数量的参数
    template <typename... As>
        requires std::is_void_v<std::void_t<F<As...>>>
    struct apply
    {
        using type = F<As...>;
    };

    // 前绑定apply - 支持任意数量的前绑定参数
    template <typename... As>
    struct bind_front
    {
        template <typename... Bs>
            requires std::is_void_v<std::void_t<F<As..., Bs...>>>
        struct apply
        {
            using type = F<As..., Bs...>;
        };
    };

    // 后绑定apply - 支持任意数量的后绑定参数
    template <typename... Bs>
    struct bind_back
    {
        template <typename... As>
            requires std::is_void_v<std::void_t<F<As..., Bs...>>>
        struct apply
        {
            using type = F<As..., Bs...>;
        };
    };
};

} // namespace wf::meta