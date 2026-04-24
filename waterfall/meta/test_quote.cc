////
// @file test_quote.cc
// @brief
// 测试模板quote - 重新设计版本，确保quote引用的模板必须是全变参的
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <type_traits>
#include <vector>
#include <map>
#include <set>
#include <tuple>
#include "quote.h"

namespace wf::meta {

// 全变参模板定义 - 符合quote要求的模板形式

// 1. 简单的全变参模板
template <typename... Ts>
struct VariadicType
{
    static constexpr size_t size = sizeof...(Ts);
};

// 2. 元组包装器 - 全变参
template <typename... Ts>
struct TupleWrapper
{
    using type = std::tuple<Ts...>;
};

// 3. 类型列表 - 全变参
template <typename... Ts>
struct TypeList
{
    static constexpr bool empty = (sizeof...(Ts) == 0);
};

// 4. 类型转换器 - 全变参（只使用第一个类型）
template <typename T, typename... Rest>
struct FirstTypeConverter
{
    using type = T;
};

// 5. 类型计数器 - 全变参
template <typename... Ts>
struct TypeCounter
{
    static constexpr size_t count = sizeof...(Ts);
};

// 6. 类型选择器 - 全变参（根据索引选择类型）
template <size_t Index, typename... Ts>
struct TypeSelector
{
    // 实际实现需要更复杂的逻辑，这里简化
    static_assert(Index < sizeof...(Ts), "Index out of range");
};

// quote1专用的模板定义 - 符合template <typename, typename...> class F形式

// 13. 至少两个参数的模板 - 符合quote1要求
template <typename First, typename Second, typename... Rest>
struct AtLeastTwoArgs
{
    static constexpr size_t total_args = 2 + sizeof...(Rest);
    using first_type = First;
    using second_type = Second;
};

// 14. 键值映射包装器 - 至少两个参数（键类型，值类型），其余为映射参数
template <typename Key, typename Value, typename... MapParams>
struct KeyValueMapWrapper
{
    using key_type = Key;
    using value_type = Value;
    using map_type = std::map<Key, Value, MapParams...>;
};

// 15. 函数指针包装器 - 至少两个参数（返回类型，第一个参数类型），其余为参数类型
template <typename ReturnType, typename FirstArg, typename... RestArgs>
struct FunctionPointerWrapper
{
    using return_type = ReturnType;
    using first_arg_type = FirstArg;
    using function_type = ReturnType (*)(FirstArg, RestArgs...);
};

// 16. 对包装器 - 至少两个参数（第一个类型，第二个类型），其余为包装参数
template <typename T1, typename T2, typename... PairParams>
struct PairWrapper
{
    using first_type = T1;
    using second_type = T2;
    using pair_type = std::pair<T1, T2>;
};

// 17. 条件类型包装器 - 至少两个参数（条件类型，真类型），其余为假类型参数
template <typename Condition, typename TrueType, typename... FalseTypes>
struct ConditionalWrapper
{
    using condition_type = Condition;
    using true_type = TrueType;
    using conditional_type = std::conditional_t<
        std::is_same_v<Condition, std::true_type>,
        TrueType,
        std::tuple<FalseTypes...>>;
};

// 18. 转换包装器 - 至少两个参数（源类型，目标类型），其余为转换参数
template <typename From, typename To, typename... ConversionParams>
struct ConversionWrapper
{
    using from_type = From;
    using to_type = To;
};

// quote3专用的模板定义 - 符合template <typename, typename, typename,
// typename...> class F形式

// 19. 至少三个参数的模板 - 符合quote3要求
template <typename First, typename Second, typename Third, typename... Rest>
struct AtLeastThreeArgs
{
    static constexpr size_t total_args = 3 + sizeof...(Rest);
    using first_type = First;
    using second_type = Second;
    using third_type = Third;
};

// 20. 三元组包装器 - 至少三个参数（三个类型），其余为包装参数
template <typename T1, typename T2, typename T3, typename... TupleParams>
struct TripleWrapper
{
    using first_type = T1;
    using second_type = T2;
    using third_type = T3;
    using triple_type = std::tuple<T1, T2, T3>;
};

// 21. 函数包装器 - 至少三个参数（返回类型，前两个参数类型），其余为参数类型
template <
    typename ReturnType,
    typename FirstArg,
    typename SecondArg,
    typename... RestArgs>
struct FunctionWrapper
{
    using return_type = ReturnType;
    using first_arg_type = FirstArg;
    using second_arg_type = SecondArg;
    using function_type = ReturnType (*)(FirstArg, SecondArg, RestArgs...);
};

// 22. 条件选择包装器 - 至少三个参数（条件类型，真类型，假类型），其余为备选类型
template <
    typename Condition,
    typename TrueType,
    typename FalseType,
    typename... AltTypes>
struct ConditionalSelectWrapper
{
    using condition_type = Condition;
    using true_type = TrueType;
    using false_type = FalseType;
    using selected_type = std::conditional_t<
        std::is_same_v<Condition, std::true_type>,
        TrueType,
        FalseType>;
};

// 23. 映射包装器 - 至少三个参数（键类型，值类型，比较器），其余为分配器等
template <
    typename Key,
    typename Value,
    typename Compare,
    typename... Allocators>
struct MapWrapper
{
    using key_type = Key;
    using value_type = Value;
    using compare_type = Compare;
    using map_type = std::map<Key, Value, Compare, Allocators...>;
};

// 24. 转换链包装器 - 至少三个参数（源类型，中间类型，目标类型），其余为转换参数
template <
    typename From,
    typename Intermediate,
    typename To,
    typename... ConversionParams>
struct ConversionChainWrapper
{
    using from_type = From;
    using intermediate_type = Intermediate;
    using to_type = To;
};

// quote_exact1专用的模板定义 - 符合template <typename> class F形式

// 25. 单参数包装器 - 符合quote_exact1要求
template <typename T>
struct SingleTypeWrapper
{
    using type = T;
    static constexpr bool is_single = true;
};

// 26. 指针包装器 - 单参数
template <typename T>
struct PointerWrapper
{
    using type = T *;
    using pointer_type = T *;
};

// 27. 引用包装器 - 单参数
template <typename T>
struct ReferenceWrapper
{
    using type = T &;
    using reference_type = T &;
};

// 28. 常量包装器 - 单参数
template <typename T>
struct ConstWrapper
{
    using type = const T;
    using const_type = const T;
};

// 29. 移除常量包装器 - 单参数
template <typename T>
struct RemoveConstWrapper
{
    using type = std::remove_const_t<T>;
};

// 30. 移除引用包装器 - 单参数
template <typename T>
struct RemoveReferenceWrapper
{
    using type = std::remove_reference_t<T>;
};

// quote_exact2专用的模板定义 - 符合template <typename, typename> class F形式

// 31. 双类型包装器 - 双参数模板
template <typename T1, typename T2>
struct DoubleTypeWrapper
{
    static constexpr bool is_double = true;
    using first_type = T1;
    using second_type = T2;
};

// 32. 键值对包装器 - 双参数模板
template <typename Key, typename Value>
struct KeyValuePairWrapper
{
    using key_type = Key;
    using value_type = Value;
    using pair_type = std::pair<Key, Value>;
};

// 33. 函数指针包装器 - 双参数模板（返回类型，参数类型）
template <typename ReturnType, typename ArgType>
struct SimpleFunctionPointerWrapper
{
    using return_type = ReturnType;
    using arg_type = ArgType;
    using function_type = ReturnType (*)(ArgType);
};

// 34. 转换包装器 - 双参数模板（源类型，目标类型）
template <typename From, typename To>
struct SimpleConversionWrapper
{
    using from_type = From;
    using to_type = To;
};

// 35. 条件包装器 - 双参数模板（条件类型，结果类型）
template <typename Condition, typename ResultType>
struct SimpleConditionalWrapper
{
    using condition_type = Condition;
    using result_type = ResultType;
    using conditional_type = std::conditional_t<
        std::is_same_v<Condition, std::true_type>,
        ResultType,
        void>;
};

// 36. 指针对包装器 - 双参数模板
template <typename T1, typename T2>
struct PointerPairWrapper
{
    using first_pointer = T1 *;
    using second_pointer = T2 *;
};

// 37. 三类型包装器 - 三参数模板
template <typename T1, typename T2, typename T3>
struct TripleTypeWrapper
{
    static constexpr bool is_triple = true;
    using first_type = T1;
    using second_type = T2;
    using third_type = T3;
};

// 38. 三元组包装器 - 三参数模板
template <typename T1, typename T2, typename T3>
struct ExactTripleWrapper
{
    using first_type = T1;
    using second_type = T2;
    using third_type = T3;
    using triple_type = std::tuple<T1, T2, T3>;
};

// 39. 函数包装器 - 三参数模板（返回类型，参数类型1，参数类型2）
template <typename ReturnType, typename ArgType1, typename ArgType2>
struct SimpleTripleFunctionWrapper
{
    using return_type = ReturnType;
    using first_arg_type = ArgType1;
    using second_arg_type = ArgType2;
    using function_type = ReturnType (*)(ArgType1, ArgType2);
};

// 40. 转换包装器 - 三参数模板（源类型，中间类型，目标类型）
template <typename From, typename Intermediate, typename To>
struct TripleConversionWrapper
{
    using from_type = From;
    using intermediate_type = Intermediate;
    using to_type = To;
};

// 41. 条件包装器 - 三参数模板（条件类型，真类型，假类型）
template <typename Condition, typename TrueType, typename FalseType>
struct TripleConditionalWrapper
{
    using condition_type = Condition;
    using true_type = TrueType;
    using false_type = FalseType;
    using conditional_type = std::conditional_t<
        std::is_same_v<Condition, std::true_type>,
        TrueType,
        FalseType>;
};

// 42. 指针对包装器 - 三参数模板
template <typename T1, typename T2, typename T3>
struct TriplePointerWrapper
{
    using first_pointer = T1 *;
    using second_pointer = T2 *;
    using third_pointer = T3 *;
};

// 测试quote的基本功能
TEST(QuoteTest, BasicFunctionality)
{
    // 创建quote实例 - 测试全变参模板
    quote<VariadicType> q_variadic;
    quote<TupleWrapper> q_tuple;
    quote<TypeList> q_typelist;

    // 测试quote本身的大小（应该是空结构）
    EXPECT_EQ(sizeof(q_variadic), 1);
    EXPECT_EQ(sizeof(q_tuple), 1);
    EXPECT_EQ(sizeof(q_typelist), 1);

    // 测试quote是空结构
    static_assert(std::is_empty_v<quote<VariadicType>>);
    static_assert(std::is_empty_v<quote<TupleWrapper>>);
    static_assert(std::is_empty_v<quote<TypeList>>);
}

// 测试quote的apply功能
TEST(QuoteTest, ApplyFunctionality)
{
    // 测试apply类型别名 - 全变参模板
    using variadic_empty = quote<VariadicType>::apply<>::type;
    using variadic_int = quote<VariadicType>::apply<int>::type;
    using variadic_int_double = quote<VariadicType>::apply<int, double>::type;

    // 验证apply结果
    static_assert(variadic_empty::size == 0);
    static_assert(variadic_int::size == 1);
    static_assert(variadic_int_double::size == 2);

    // 测试TupleWrapper
    using tuple_int = quote<TupleWrapper>::apply<int>::type;
    using tuple_int_double = quote<TupleWrapper>::apply<int, double>::type;

    static_assert(std::is_same_v<typename tuple_int::type, std::tuple<int>>);
    static_assert(std::is_same_v<
                  typename tuple_int_double::type,
                  std::tuple<int, double>>);

    // 测试SFINAE特性
    static_assert(std::is_void_v<std::void_t<quote<VariadicType>::apply<>>>);
    static_assert(std::is_void_v<std::void_t<quote<TupleWrapper>::apply<int>>>);
}

// 测试quote的bind_front功能
TEST(QuoteTest, BindFrontFunctionality)
{
    // 测试bind_front结构 - 全变参模板
    using bind_variadic_int = quote<VariadicType>::bind_front<int>;
    using bind_tuple_int = quote<TupleWrapper>::bind_front<int>;
    using bind_typelist_int = quote<TypeList>::bind_front<int>;

    // 验证bind_front结构的大小
    EXPECT_EQ(sizeof(bind_variadic_int), 1);
    EXPECT_EQ(sizeof(bind_tuple_int), 1);
    EXPECT_EQ(sizeof(bind_typelist_int), 1);

    // 测试bind_front的apply功能
    using variadic_int = bind_variadic_int::apply<>::type;
    using variadic_int_double = bind_variadic_int::apply<double>::type;

    using tuple_int = bind_tuple_int::apply<>::type;
    using tuple_int_double = bind_tuple_int::apply<double>::type;

    using typelist_int = bind_typelist_int::apply<>::type;
    using typelist_int_double = bind_typelist_int::apply<double>::type;

    // 验证bind_front apply结果
    static_assert(variadic_int::size == 1);
    static_assert(variadic_int_double::size == 2);

    static_assert(std::is_same_v<typename tuple_int::type, std::tuple<int>>);
    static_assert(std::is_same_v<
                  typename tuple_int_double::type,
                  std::tuple<int, double>>);

    static_assert(!typelist_int::empty);
    static_assert(!typelist_int_double::empty);
}

// 测试quote的bind_back功能
TEST(QuoteTest, BindBackFunctionality)
{
    // 测试bind_back结构 - 全变参模板
    using bind_variadic_int = quote<VariadicType>::bind_back<int>;
    using bind_tuple_int = quote<TupleWrapper>::bind_back<int>;
    using bind_typelist_int = quote<TypeList>::bind_back<int>;

    // 验证bind_back结构的大小
    EXPECT_EQ(sizeof(bind_variadic_int), 1);
    EXPECT_EQ(sizeof(bind_tuple_int), 1);
    EXPECT_EQ(sizeof(bind_typelist_int), 1);

    // 测试bind_back的apply功能
    using variadic_int = bind_variadic_int::apply<>::type;
    using variadic_double_int = bind_variadic_int::apply<double>::type;

    using tuple_int = bind_tuple_int::apply<>::type;
    using tuple_double_int = bind_tuple_int::apply<double>::type;

    using typelist_int = bind_typelist_int::apply<>::type;
    using typelist_double_int = bind_typelist_int::apply<double>::type;

    // 验证bind_back apply结果
    static_assert(variadic_int::size == 1);
    static_assert(variadic_double_int::size == 2);

    static_assert(std::is_same_v<typename tuple_int::type, std::tuple<int>>);
    static_assert(std::is_same_v<
                  typename tuple_double_int::type,
                  std::tuple<double, int>>);

    static_assert(!typelist_int::empty);
    static_assert(!typelist_double_int::empty);
}

// 测试quote作为高阶元函数的特性
TEST(QuoteTest, HigherOrderMetaFunction)
{
    // 测试quote本身是类模板
    static_assert(std::is_class_v<quote<VariadicType>>);
    static_assert(std::is_class_v<quote<TupleWrapper>>);
    static_assert(std::is_class_v<quote<TypeList>>);

    // 测试quote的apply是类型别名模板
    static_assert(std::is_same_v<
                  typename quote<VariadicType>::apply<int>::type,
                  VariadicType<int>>);

    // 测试quote的bind_front是嵌套类模板
    static_assert(std::is_class_v<quote<VariadicType>::bind_front<int>>);
    static_assert(std::is_class_v<quote<TupleWrapper>::bind_front<int>>);

    // 测试quote的bind_back是嵌套类模板
    static_assert(std::is_class_v<quote<VariadicType>::bind_back<int>>);
    static_assert(std::is_class_v<quote<TupleWrapper>::bind_back<int>>);
}

// 测试quote与标准库模板结合使用（必须是全变参的）
TEST(QuoteTest, WithStdTemplates)
{
    // 测试std::tuple - 全变参模板
    using tuple_int = quote<std::tuple>::apply<int>::type;
    using tuple_int_double = quote<std::tuple>::apply<int, double>::type;

    static_assert(std::is_same_v<tuple_int, std::tuple<int>>);
    static_assert(std::is_same_v<tuple_int_double, std::tuple<int, double>>);

    using variant_int_double = quote<std::variant>::apply<int, double>::type;
    static_assert(
        std::is_same_v<variant_int_double, std::variant<int, double>>);

    // 测试bind_front与std::tuple
    using bind_tuple_int =
        quote<std::tuple>::bind_front<int, double, char>::apply<>;
    using tuple_from_bind = bind_tuple_int::type;
    static_assert(
        std::is_same_v<tuple_from_bind, std::tuple<int, double, char>>);

    // 测试bind_back与std::tuple - bind_back需要提供A、B、C参数
    using bind_tuple_back = quote<std::tuple>::bind_back<>;
    using tuple_from_bind_back =
        bind_tuple_back::apply<int, double, char>::type;
    static_assert(
        std::is_same_v<tuple_from_bind_back, std::tuple<int, double, char>>);
}

// 测试quote的SFINAE特性
TEST(QuoteTest, SFINAEProperties)
{
    // 测试有效的模板实例化
    static_assert(std::is_void_v<std::void_t<quote<VariadicType>::apply<>>>);
    static_assert(std::is_void_v<std::void_t<quote<TupleWrapper>::apply<int>>>);

    // 测试bind_front的SFINAE
    using bind_variadic = quote<VariadicType>::bind_front<int>;
    static_assert(std::is_void_v<std::void_t<bind_variadic::apply<>>>);

    // 测试bind_back的SFINAE
    using bind_tuple = quote<TupleWrapper>::bind_back<int>;
    static_assert(std::is_void_v<std::void_t<bind_tuple::apply<>>>);
}

} // namespace wf::meta

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}