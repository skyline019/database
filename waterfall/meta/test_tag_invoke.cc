////
// @file test_tag_invoke.cc
// @brief
// 测试tag_invoke.h
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <type_traits>
#include <concepts>
#include "tag_invoke.h"

namespace wf::meta::test {

// 定义一个简单的CPO
struct my_cpo
{
    constexpr auto operator()(int x) const -> int { return x * 2; }
};

// 测试用例1: 基本tag_invoke功能测试
TEST(TagInvokeTest, BasicTagInvoke)
{
    constexpr my_cpo my_cpo_instance{};

    // 测试直接调用CPO
    static_assert(my_cpo_instance(5) == 10);

    // 测试通过tag_invoke调用
    static_assert(meta::tag_invoke(my_cpo_instance, 5) == 10);
}

struct custom_type
{};

struct my_cpo2
{
    // 默认实现
    constexpr auto operator()(int x) const -> int { return x + 1; }
};

// 为custom_type定制tag_invoke
constexpr auto tag_invoke(my_cpo2, custom_type) -> int { return 42; }

// 测试用例2: 定制tag_invoke实现
TEST(TagInvokeTest, CustomTagInvoke)
{
    constexpr my_cpo2 my_cpo2_instance{};

    // 测试默认实现
    static_assert(meta::tag_invoke(my_cpo2_instance, 10) == 11);

    // 测试定制实现
    static_assert(meta::tag_invoke(my_cpo2_instance, custom_type{}) == 42);
}

struct simple_cpo
{
    constexpr auto operator()(int) const -> int { return 0; }
};
// constexpr simple_cpo simple_cpo_instance{};

// 测试用例3: 概念检查
TEST(TagInvokeTest, ConceptChecks)
{
    // 测试is_tag_invocable_v概念
    static_assert(is_tag_invocable_v<simple_cpo, int>);
    static_assert(is_tag_invocable_v<simple_cpo, double>); // 隐式转换
    static_assert(!is_tag_invocable_v<simple_cpo, std::string>);

    // 测试is_cpo_operator_v概念
    static_assert(is_cpo_operator_v<simple_cpo, int>);
    static_assert(is_cpo_operator_v<simple_cpo, double>); // 隐式转换
    static_assert(!is_cpo_operator_v<simple_cpo, std::string>);
}

struct identity_cpo
{
    template <typename T>
    constexpr auto operator()(T &&t) const -> T &&
    {
        return std::forward<T>(t);
    }
};
inline constexpr identity_cpo identity_cpo_instance{};

// 测试用例4: 元函数绑定
TEST(TagInvokeTest, MetaFunctionBinding)
{
    // 测试tag_t
    using tag_type = tag_t<identity_cpo_instance>;
    static_assert(std::is_same_v<tag_type, identity_cpo>);

    // 测试meta_tag_invoke_result
    using result_type = meta_tag_invoke_result<identity_cpo>::apply<int>;
    static_assert(std::is_same_v<result_type, int &&>);
}

struct noexcept_cpo
{
    constexpr auto operator()() const noexcept -> int { return 100; }
};

// struct throwing_cpo
// {
//     constexpr auto operator()() const -> int
//     {
//         throw std::runtime_error("test");
//         return 0;
//     }
// };

inline constexpr noexcept_cpo noexcept_cpo_instance{};
// inline constexpr throwing_cpo throwing_cpo_instance{};

// 测试用例5: 异常安全测试
TEST(TagInvokeTest, ExceptionSafety)
{
    // 测试is_tag_nothrow_invocable_v
    static_assert(is_tag_nothrow_invocable_v<noexcept_cpo>);
    // static_assert(!is_tag_nothrow_invocable_v<throwing_cpo>);
}

struct forwarding_cpo
{
    template <typename T>
    constexpr auto operator()(T &&t) const -> T &&
    {
        return std::forward<T>(t);
    }
};

inline constexpr forwarding_cpo forwarding_cpo_instance{};
// 测试用例6: 复杂类型和转发
TEST(TagInvokeTest, ComplexTypesAndForwarding)
{
    // 测试左值转发
    int lvalue = 42;
    auto &&result1 = meta::tag_invoke(forwarding_cpo_instance, lvalue);
    static_assert(std::is_same_v<decltype(result1), int &>);

    // 测试右值转发
    auto &&result2 = meta::tag_invoke(forwarding_cpo_instance, 100);
    static_assert(std::is_same_v<decltype(result2), int &&>);
}

struct add_cpo
{
    constexpr auto operator()(int a, int b) const -> int { return a + b; }
};

inline constexpr add_cpo add_cpo_instance{};
// 测试用例7: 多参数支持
TEST(TagInvokeTest, MultipleArguments)
{
    // 测试多参数调用
    static_assert(meta::tag_invoke(add_cpo_instance, 10, 20) == 30);

    // 测试概念检查
    static_assert(is_tag_invocable_v<add_cpo, int, int>);
    static_assert(!is_tag_invocable_v<add_cpo, int>);
    static_assert(is_tag_invocable_v<add_cpo, int, double>); // 隐式转换
    static_assert(!is_tag_invocable_v<add_cpo, int, std::string>);
}

} // namespace wf::meta::test

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}