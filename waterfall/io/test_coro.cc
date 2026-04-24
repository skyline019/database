////
// @file test_coro.cc
// @brief
// 测试coro.h
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#include <gtest/gtest.h>
#include <coroutine>
#include <waterfall/meta/type_index.h>
#include "reactor.h"
#include "scheduler.h"
#include "thread_pool.h"
#include "service.h"
#include "coro.h"

namespace wf::io::await_resume {

// 这个测试用例用于验证：
// initial_suspend.await_ready = true，await_resume不会被调用
int anchar = 0;
struct TestCoroutine
{
    struct awaiter_type
    {
        bool await_ready() const noexcept
        {
            std::cout << "[LOG] await_ready() called" << std::endl;
            return true;
        }
        void await_suspend(std::coroutine_handle<>) noexcept
        {
            std::cout << "[LOG] await_suspend() called" << std::endl;
        }
        void await_resume() noexcept
        {
            std::cout << "[LOG] await_resume() called (anchar=1)" << std::endl;
            anchar = 1;
        }
    };

    struct promise_type
    {
        TestCoroutine get_return_object()
        {
            return TestCoroutine{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        awaiter_type initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
        void return_void() {}
    };

    std::coroutine_handle<promise_type> handle;

    TestCoroutine(std::coroutine_handle<promise_type> h)
        : handle(h)
    {}

    ~TestCoroutine()
    {
        std::cout << "[LOG] ~TestCoroutine() start" << std::endl;
        if (handle) {
            std::cout << "[LOG] handle.destroy() called" << std::endl;
            handle.destroy();
        }
        std::cout << "[LOG] ~TestCoroutine() end" << std::endl;
    }
    bool done() const { return handle.done(); }
};

TEST(CoroutineVerify, await_resume_called_initial_suspend_ready)
{
    auto test_coroutine = []() -> TestCoroutine { co_return; };
    auto coro = test_coroutine();
    // 验证await_resume未被调用
    EXPECT_TRUE(coro.done());
    EXPECT_EQ(anchar, 1);
    std::cout << "initial_suspend.await_ready = true, await_resume is called"
              << std::endl;
}

// 这个测试用例中，co_await coro; coro的intial_suspend.await_ready =
// true，不会挂起。caller只有等coro运行完毕才能拿到立即返回值。
// caller如果resume，coro会销毁，TestCoroutine2不能再销毁coro
struct TestCoroutine2
{
    struct awaiter_type
    {
        bool await_ready() const noexcept
        {
            std::cout << "[LOG] initial_suspend.await_ready() called"
                      << std::endl;
            return false; // 改为false，让协程在initial_suspend挂起
        }
        void await_suspend(std::coroutine_handle<>) noexcept
        {
            std::cout << "[LOG] initial_suspend.await_suspend() called"
                      << std::endl;
        }
        void await_resume() noexcept
        {
            std::cout
                << "[LOG] initial_suspend.await_resume() called (anchar=2)"
                << std::endl;
            anchar = 2;
        }
    };

    struct promise_type
    {
        TestCoroutine2 get_return_object()
        {
            return TestCoroutine2{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        awaiter_type initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
        void return_void() {}
    };

    std::coroutine_handle<promise_type> handle;

    TestCoroutine2(std::coroutine_handle<promise_type> h)
        : handle(h)
    {}

    ~TestCoroutine2()
    {
        std::cout << "[LOG] ~TestCoroutine2()" << std::endl;
        if (handle) {
            EXPECT_TRUE(handle.done());
            handle.destroy(); // 添加这行来修复内存泄漏
        }
    }

    auto operator co_await() noexcept
    {
        struct awaiter
        {
            TestCoroutine2 &coro_;
            bool await_ready() const noexcept { return false; }
            void await_suspend(std::coroutine_handle<>) noexcept
            {
                coro_.handle.resume(); // 按正常逻辑，这里应该恢复子协程执行
            }
            void await_resume() noexcept {}
        };
        std::cout << "[LOG] TestCoroutine2::operator co_await() called"
                  << std::endl;
        return awaiter{*this};
    }
};

TEST(CoroutineVerify, await_resume_destroy)
{
    auto caller = []() -> TestCoroutine {
        auto callee = []() -> TestCoroutine2 { co_return; };
        co_await callee(); // caller挂起，callee已完成并被销毁
        co_return;
    };
    auto coro = caller();
    coro.handle.resume();
    // 验证await_resume未被调用
    EXPECT_TRUE(coro.done());
    EXPECT_EQ(anchar, 2);
}

} // namespace wf::io::await_resume

namespace wf::io::concept_test {

// 测试符合is_awaiter_v概念的类型
struct valid_awaiter
{
    bool await_ready() const noexcept { return true; }
    void await_suspend(std::coroutine_handle<>) noexcept {}
    int await_resume() noexcept { return 42; }
};

// 测试返回bool的awaiter
struct bool_awaiter
{
    bool await_ready() const noexcept { return false; }
    bool await_suspend(std::coroutine_handle<>) noexcept { return true; }
    void await_resume() noexcept {}
};

// 测试返回coroutine_handle的awaiter
struct handle_awaiter
{
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) noexcept
    {
        return std::noop_coroutine();
    }
    void await_resume() noexcept {}
};

// 测试不符合is_awaiter_v概念的类型（缺少await_ready）
struct invalid_awaiter_missing_ready
{
    void await_suspend(std::coroutine_handle<>) noexcept {}
    void await_resume() noexcept {}
};

// 测试不符合is_awaiter_v概念的类型（await_ready返回类型错误）
struct invalid_awaiter_wrong_ready
{
    int await_ready() const noexcept { return 1; }
    void await_suspend(std::coroutine_handle<>) noexcept {}
    void await_resume() noexcept {}
};

// 测试不符合is_awaiter_v概念的类型（await_suspend参数错误）
struct invalid_awaiter_wrong_suspend
{
    bool await_ready() const noexcept { return true; }
    void await_suspend(int) noexcept {}
    void await_resume() noexcept {}
};

TEST(ConceptTest, IsAwaiterValidCases)
{
    // 测试有效的awaiter类型
    static_assert(_awaitable::cpo::is_awaiter_v<valid_awaiter>);
    static_assert(_awaitable::cpo::is_awaiter_v<bool_awaiter>);
    static_assert(_awaitable::cpo::is_awaiter_v<handle_awaiter>);

    // 测试标准库的suspend_always和suspend_never
    static_assert(_awaitable::cpo::is_awaiter_v<std::suspend_always>);
    static_assert(_awaitable::cpo::is_awaiter_v<std::suspend_never>);
}

TEST(ConceptTest, IsAwaiterInvalidCases)
{
    // 测试无效的awaiter类型
    static_assert(
        !_awaitable::cpo::is_awaiter_v<invalid_awaiter_missing_ready>);
    static_assert(!_awaitable::cpo::is_awaiter_v<invalid_awaiter_wrong_ready>);
    static_assert(
        !_awaitable::cpo::is_awaiter_v<invalid_awaiter_wrong_suspend>);

    // 测试基本类型
    static_assert(!_awaitable::cpo::is_awaiter_v<int>);
    static_assert(!_awaitable::cpo::is_awaiter_v<std::string>);
    static_assert(!_awaitable::cpo::is_awaiter_v<void *>);
}

// 测试_await_suspend_possibble_result_v概念
TEST(ConceptTest, AwaitSuspendPossibleResult)
{
    // 测试void返回值
    static_assert(_awaitable::cpo::_await_suspend_possibble_result_v<void>);

    // 测试bool返回值
    static_assert(_awaitable::cpo::_await_suspend_possibble_result_v<bool>);

    // 测试coroutine_handle返回值
    static_assert(_awaitable::cpo::_await_suspend_possibble_result_v<
                  std::coroutine_handle<>>);

    // 测试无效返回值
    static_assert(!_awaitable::cpo::_await_suspend_possibble_result_v<int>);
    static_assert(
        !_awaitable::cpo::_await_suspend_possibble_result_v<std::string>);
}

} // namespace wf::io::concept_test

namespace wf::io::await_transform_test {

// 简单的promise，不修改它
struct simple_promise
{
    // 没有自定义await_transform
};
struct simple_awaiter
{
    int value;
    bool await_ready() const noexcept { return true; }
    void await_suspend(std::coroutine_handle<>) noexcept {}
    int await_resume() noexcept { return value * 3; } // 自定义逻辑：乘以3
};

simple_awaiter await_transform(simple_promise &promise, int value)
{
    return simple_awaiter{value};
}

// 测试基本的await_transform功能
TEST(AwaitTransformTest, BasicFunctionality)
{
    simple_promise promise;

    // 使用项目中的await_transform函数
    auto result = _awaitable::await_transform(promise, 5);
    static_assert(_awaitable::cpo::is_awaiter_v<decltype(result)>);

    // 验证返回的awaiter功能正常
    EXPECT_TRUE(result.await_ready());
    EXPECT_EQ(result.await_resume(), 15); // 默认行为：直接返回值
}

// 测试分支1：直接awaiter（is_awaiter_v为true）
TEST(AwaitTransformTest, DirectAwaiterBranch)
{
    simple_promise promise;

    // 创建符合is_awaiter_v概念的awaiter
    struct direct_awaiter
    {
        bool await_ready() const noexcept { return true; }
        void await_suspend(std::coroutine_handle<>) noexcept {}
        int await_resume() noexcept { return 100; }
    };

    direct_awaiter awaiter;

    // 应该走第一个分支：直接返回awaiter
    auto result = _awaitable::await_transform(promise, awaiter);
    static_assert(_awaitable::cpo::is_awaiter_v<decltype(result)>);
}

// 测试分支2：有成员operator co_await的可等待对象
TEST(AwaitTransformTest, MemberOperatorCoAwaitBranch)
{
    simple_promise promise;

    // 创建有成员operator co_await的可等待对象
    struct with_member_co_await
    {
        int value = 200;

        auto operator co_await()
        {
            struct awaiter
            {
                int value;
                bool await_ready() const noexcept { return true; }
                void await_suspend(std::coroutine_handle<>) noexcept {}
                int await_resume() noexcept { return value; }
            };
            return awaiter{value};
        }
    };

    with_member_co_await awaitable;

    // 应该走第二个分支：调用operator co_await()
    auto result = _awaitable::await_transform(promise, awaitable);
    static_assert(_awaitable::cpo::is_awaiter_v<decltype(result)>);
}

// 创建有自由operator co_await的可等待对象
struct with_free_co_await
{
    int value = 300;
};
// 定义自由operator co_await函数
auto operator co_await(with_free_co_await obj)
{
    struct awaiter
    {
        int value;
        bool await_ready() const noexcept { return true; }
        void await_suspend(std::coroutine_handle<>) noexcept {}
        int await_resume() noexcept { return value; }
    };
    return awaiter{obj.value};
}

// 测试分支3：有自由operator co_await的可等待对象
TEST(AwaitTransformTest, FreeOperatorCoAwaitBranch)
{
    simple_promise promise;

    with_free_co_await awaitable;

    // 应该走第三个分支：调用自由operator co_await
    auto result = _awaitable::await_transform(promise, awaitable);
    static_assert(_awaitable::cpo::is_awaiter_v<decltype(result)>);
}

} // namespace wf::io::await_transform_test

namespace wf::io::promise_base_test {

// 测试promise_base的基本功能
TEST(PromiseBaseTest, BasicFunctionality)
{
    // 直接测试promise_base的功能
    promise_base promise;

    EXPECT_EQ(promise.get_executor(), nullptr);
    EXPECT_EQ(promise.get_continuation(), nullptr);

    // 测试状态设置
    promise.set_offline();
    EXPECT_TRUE(promise.state_ & kCoroStateOffline);
    promise.set_inline();
    EXPECT_FALSE(promise.state_ & kCoroStateOffline);

    EXPECT_FALSE(promise.state_ & kCoroStateDetached);
    promise.set_detached();
    EXPECT_TRUE(promise.state_ & kCoroStateDetached);
}

// 测试promise_base的continuation链
TEST(PromiseBaseTest, ContinuationChain)
{
    // 直接使用promise_base测试链条功能
    promise_base promise1;
    promise_base promise2;
    promise_base promise3;

    // 设置链条: promise1 -> promise2 -> promise3
    promise1.set_continuation(&promise2);
    promise2.set_continuation(&promise3);

    // 验证链条设置正确
    EXPECT_EQ(promise1.get_continuation(), &promise2);
    EXPECT_EQ(promise2.get_continuation(), &promise3);
    EXPECT_EQ(promise3.get_continuation(), nullptr);

    // 测试链条断开
    promise2.set_continuation(nullptr);
    EXPECT_EQ(promise1.get_continuation(), &promise2);
    EXPECT_EQ(promise2.get_continuation(), nullptr);
}

class PromiseBaseTestx : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        registry_ = std::make_unique<service_registry>();
        thread_pool_ = registry_->use_service<thread_pool>();
        ASSERT_NE(thread_pool_, nullptr);
        scheduler_ = registry_->use_service<scheduler_service>(registry_.get());
        ASSERT_NE(scheduler_, nullptr);
        reactor_ = registry_->has_service<reactor_service>();
        ASSERT_NE(reactor_, nullptr);
    }

    void TearDown() override
    {
        registry_->shutdown_services();
        registry_.reset();
    }

    void start_threads(unsigned short n)
    {
        for (unsigned short i = 0; i < n; ++i)
            thread_pool_->execute([this, i]() { scheduler_->run(i); });
    }
    void join_all() { thread_pool_->join_all(); }

    std::unique_ptr<service_registry> registry_;
    scheduler_service *scheduler_;
    reactor_service *reactor_;
    thread_pool *thread_pool_;
};

// 使用夹具测试resume功能
TEST_F(PromiseBaseTestx, BasicResume)
{
    start_threads(4);

    // 缺省协程
    auto coro = []() -> coroutine_type<int> { co_return 42; }();
    EXPECT_FALSE(coro.handle_.done());

    EXPECT_FALSE(coro.promise().state_ & kCoroStateOffline);
    EXPECT_FALSE(coro.promise().state_ & kCoroStateDetached);
    EXPECT_TRUE(coro.promise().executor_ == nullptr);

    // inline resume
    coro.promise().resume();
    EXPECT_TRUE(coro.handle_.done());

    // NOTE: 捕获this，asan会报错，改为通过参数传递scheduler指针
    auto coro2 = [](scheduler_service *scheduler) -> coroutine_type<int> {
        scheduler->stop();
        co_return 42;
    }(scheduler_);
    EXPECT_FALSE(coro2.handle_.done());
    coro2.promise().set_offline();
    EXPECT_TRUE(coro2.promise().state_ & kCoroStateOffline);
    // offline resume
    coro2.promise().resume();

    join_all();
    EXPECT_TRUE(coro2.handle_.done());
}

TEST_F(PromiseBaseTestx, BasicDetach)
{
    // 缺省协程
    auto coro = []() -> coroutine_type<int> { co_return 42; }();
    EXPECT_FALSE(coro.handle_.done());

    coro.promise().set_detached();
    EXPECT_TRUE(coro.promise().state_ & kCoroStateDetached);
    coroutine_type<int>::promise_type &promise = coro.promise();
    coro.handle_ = nullptr; // 清掉coro的handle，以防析构动作

    promise.resume();

    start_threads(4);

    // NOTE: 捕获this，asan会报错，改为通过参数传递scheduler指针
    auto coro2 = [](scheduler_service *scheduler) -> coroutine_type<int> {
        scheduler->stop();
        co_return 42;
    }(scheduler_);
    EXPECT_FALSE(coro2.handle_.done());
    coro2.promise().set_offline();
    coro2.promise().set_detached();
    EXPECT_TRUE(coro2.promise().state_ & kCoroStateDetached);
    coroutine_type<int>::promise_type &promise2 = coro2.promise();
    coro2.handle_ = nullptr; // 清掉coro的handle，以防析构动作

    // offline resume
    promise2.resume();

    join_all();
}

} // namespace wf::io::promise_base_test

namespace wf::io::return_base_test {

// 测试return_base的基本功能
TEST(ReturnBaseTest, BasicFunctionality)
{
    // 测试非void类型的return_base
    return_base<int> promise;

    // 初始状态检查
    EXPECT_EQ(promise.get_value(), 0); // 默认构造的int为0

    // 测试return_value
    promise.return_value(42);
    EXPECT_EQ(promise.get_value(), 42);

    // 测试移动语义
    promise.return_value(100);
    EXPECT_EQ(promise.get_value(), 100);
}

// 测试return_base的void特化
TEST(ReturnBaseTest, VoidSpecialization)
{
    return_base<void> promise;

    // void特化没有return_value_成员
    // 主要测试编译通过和基本功能
    promise.return_void(); // 应该编译通过
}

// 测试return_base的不同类型
TEST(ReturnBaseTest, DifferentTypes)
{
    // 测试int类型
    return_base<int> promise1;
    promise1.return_value(123);
    EXPECT_EQ(promise1.get_value(), 123);

    // 测试std::string类型
    return_base<std::string> promise2;
    promise2.return_value("hello");
    EXPECT_EQ(promise2.get_value(), "hello");

    // 测试移动语义
    std::string str = "world";
    promise2.return_value(std::move(str));
    EXPECT_EQ(promise2.get_value(), "world");
}

// 测试yield_value功能
TEST(ReturnBaseTest, YieldValue)
{
    return_base<int> promise;

    // 测试yield_value
    auto awaiter = promise.yield_value(999);
    static_assert(std::is_same_v<decltype(awaiter), yield_awaiter<int>>);

    // 验证yield_value设置了返回值
    EXPECT_EQ(promise.get_value(), 999);

    // 验证返回的awaiter类型正确
    // 这里可以验证awaiter的基本功能
    EXPECT_FALSE(awaiter.await_ready());
}

} // namespace wf::io::return_base_test

namespace wf::io::coroutine_type_test {

// 测试coroutine_type的基本功能
TEST(CoroutineTypeTest, BasicFunctionality)
{
    // 创建简单的协程函数
    auto coro = []() -> coroutine_type<int> { co_return 42; }();

    // 初始状态检查
    EXPECT_FALSE(coro.handle_.done());
    EXPECT_TRUE(coro.handle_);

    // 恢复协程
    coro.promise().resume();

    // 验证协程完成
    EXPECT_TRUE(coro.handle_.done());

    // 验证返回值
    EXPECT_EQ(coro.promise().get_value(), 42);
}

// 测试coroutine_type的移动语义
TEST(CoroutineTypeTest, MoveSemantics)
{
    // 测试移动构造
    auto coro1 = []() -> coroutine_type<std::string> { co_return "hello"; }();
    auto coro2 = std::move(coro1);

    // 原协程应该无效
    EXPECT_FALSE(coro1.handle_);
    EXPECT_TRUE(coro2.handle_);

    // 测试移动赋值
    coro1 = std::move(coro2);
    EXPECT_TRUE(coro1.handle_);
    EXPECT_FALSE(coro2.handle_);

    // 完成协程
    coro1.promise().resume();
    EXPECT_EQ(coro1.promise().get_value(), "hello");
}

// 测试coroutine_type的void返回类型
TEST(CoroutineTypeTest, VoidReturnType)
{
    auto coro = []() -> coroutine_type<void> { co_return; }();

    // 恢复协程
    coro.promise().resume();

    // 验证协程完成
    EXPECT_TRUE(coro.handle_.done());

    // void类型没有get_result()方法
    // 这里主要验证编译通过和基本功能
}

// 测试coroutine_type的promise访问
TEST(CoroutineTypeTest, PromiseAccess)
{
    auto coro = []() -> coroutine_type<int> { co_return 100; }();

    // 测试promise访问
    auto &promise = coro.promise();

    // 验证promise类型正确
    EXPECT_FALSE(promise.state_ & kCoroStateDetached);
    EXPECT_FALSE(promise.state_ & kCoroStateOffline);

    // 通过promise设置状态
    promise.set_detached();
    EXPECT_TRUE(promise.state_ & kCoroStateDetached);
    coro.handle_ = nullptr; // 清掉coro的handle，以防析构动作

    // 恢复协程
    promise.resume();
}

// 测试coroutine_type的co_await运算符
TEST(CoroutineTypeTest, CoAwaitOperator)
{
    auto inner_coro = []() -> coroutine_type<int> {
        printf("Inner coroutine running\n");
        co_return 200;
    };
    auto outer_coro = [inner_coro]() -> coroutine_type<int> {
        int result = co_await inner_coro();
        EXPECT_EQ(result, 200);
        co_return result + 50;
    };

    auto coro = outer_coro();
    coro.promise().resume();

    // 验证结果
    EXPECT_TRUE(coro.handle_.done());
    EXPECT_EQ(coro.promise().get_value(), 250); // 200 + 50 = 250
}

// 测试coroutine_type的不同返回类型
TEST(CoroutineTypeTest, DifferentReturnTypes)
{
    // 测试int类型
    auto coro1 = []() -> coroutine_type<int> { co_return 123; }();
    coro1.promise().resume();
    EXPECT_EQ(coro1.promise().get_value(), 123);

    // 测试double类型
    auto coro2 = []() -> coroutine_type<double> { co_return 3.14; }();
    coro2.promise().resume();
    EXPECT_DOUBLE_EQ(coro2.promise().get_value(), 3.14);

    // 测试自定义类型
    struct point
    {
        int x, y;
    };
    auto coro3 = []() -> coroutine_type<point> { co_return point{10, 20}; }();
    coro3.promise().resume();
    auto result = coro3.promise().get_value();
    EXPECT_EQ(result.x, 10);
    EXPECT_EQ(result.y, 20);
}

} // namespace wf::io::coroutine_type_test

namespace wf::io::coroutine_wrapper_test {

// 测试coroutine_wrapper的基本功能
TEST(CoroutineWrapperTest, BasicFunctionality)
{
    coroutine_wrapper<int> wrapper;
    wrapper.create([]() -> coroutine_type<int> { co_return 42; });

    EXPECT_TRUE(wrapper.done());
    EXPECT_EQ(wrapper.get_value(), 42);
}

// 测试coroutine_wrapper的移动语义
TEST(CoroutineWrapperTest, MoveSemantics)
{
    coroutine_wrapper<std::string> wrapper1;
    wrapper1.create([]() -> coroutine_type<std::string> { co_return "hello"; });

    coroutine_wrapper<std::string> wrapper2(std::move(wrapper1));

    // 原wrapper应该无效
    EXPECT_FALSE(static_cast<bool>(wrapper1));
    EXPECT_TRUE(static_cast<bool>(wrapper2));

    // 测试移动赋值
    wrapper1 = std::move(wrapper2);
    EXPECT_TRUE(static_cast<bool>(wrapper1));
    EXPECT_FALSE(static_cast<bool>(wrapper2));

    // 验证功能
    EXPECT_TRUE(wrapper1.done());
    EXPECT_EQ(wrapper1.get_value(), "hello");
}

// 测试coroutine_wrapper的void返回类型
TEST(CoroutineWrapperTest, VoidReturnType)
{
    auto wrapper = coroutine_wrapper<void>().create(
        []() -> coroutine_type<void> { co_return; });
    EXPECT_TRUE(wrapper.done());
}

// 测试coroutine_wrapper的配置接口
TEST(CoroutineWrapperTest, ConfigurationInterface)
{
    auto wrapper = coroutine_wrapper<int>()
                       .detach() // 设置为分离模式
                       .create([]() -> coroutine_type<int> { co_return 100; });
}

#if 0
// 测试coroutine_wrapper的get和unwrap接口
TEST(CoroutineWrapperTest, GetAndUnwrap)
{
    auto coro_func = []() -> coroutine_type<int> { co_return 200; };

    coroutine_wrapper<int> wrapper(coro_func());

    // 测试get()接口
    auto &coro_ref = wrapper.get();
    EXPECT_TRUE(coro_ref.done());
    EXPECT_EQ(coro_ref.get_result(), 200);

    // 测试unwrap()接口（移动语义）
    auto unwrapped_coro = std::move(wrapper).unwrap();
    EXPECT_TRUE(unwrapped_coro.done());
    EXPECT_EQ(unwrapped_coro.get_result(), 200);

    // 原wrapper应该无效
    EXPECT_FALSE(static_cast<bool>(wrapper));
}

// 测试coroutine_wrapper的隐式转换
TEST(CoroutineWrapperTest, ImplicitConversion)
{
    auto coro_func = []() -> coroutine_type<int> { co_return 300; };

    coroutine_wrapper<int> wrapper(coro_func());

    // 测试隐式转换为coroutine_type&
    coroutine_type<int> &coro_ref = wrapper;
    EXPECT_TRUE(coro_ref.done());
    EXPECT_EQ(coro_ref.get_result(), 300);

    // 测试隐式转换为const coroutine_type&
    const coroutine_type<int> &const_coro_ref = wrapper;
    EXPECT_TRUE(const_coro_ref.done());

    // 测试移动转换
    coroutine_type<int> moved_coro = std::move(wrapper);
    EXPECT_TRUE(moved_coro.done());
    EXPECT_EQ(moved_coro.get_result(), 300);
}

// 测试coroutine_wrapper的resume接口
TEST(CoroutineWrapperTest, ResumeInterface)
{
    // 创建一个需要多次resume的协程
    int resume_count = 0;
    auto complex_coro = [&resume_count]() -> coroutine_type<int> {
        resume_count++;
        co_await std::suspend_always{};
        resume_count++;
        co_return 400;
    };

    // 使用coroutine_wrapper
    coroutine_wrapper<int> wrapper(complex_coro());

    // 第一次resume由构造函数自动完成
    EXPECT_EQ(resume_count, 1);
    EXPECT_FALSE(wrapper.done());

    // 手动resume
    wrapper.resume();
    EXPECT_EQ(resume_count, 2);
    EXPECT_TRUE(wrapper.done());
    EXPECT_EQ(wrapper.get_value(), 400);
}

// 测试coroutine_wrapper的默认构造
TEST(CoroutineWrapperTest, DefaultConstruction)
{
    // 默认构造的wrapper应该无效
    coroutine_wrapper<int> wrapper;

    EXPECT_FALSE(static_cast<bool>(wrapper));
    EXPECT_TRUE(wrapper.done());

    // 尝试操作无效wrapper应该安全
    wrapper.resume(); // 应该不会崩溃
}

// 测试coroutine_wrapper的不同返回类型
TEST(CoroutineWrapperTest, DifferentReturnTypes)
{
    // 测试int类型
    auto int_coro = []() -> coroutine_type<int> { co_return 123; };
    coroutine_wrapper<int> wrapper1(int_coro());
    EXPECT_EQ(wrapper1.get_value(), 123);

    // 测试double类型
    auto double_coro = []() -> coroutine_type<double> { co_return 3.14; };
    coroutine_wrapper<double> wrapper2(double_coro());
    EXPECT_DOUBLE_EQ(wrapper2.get_value(), 3.14);

    // 测试自定义类型
    struct point
    {
        int x, y;
    };
    auto point_coro = []() -> coroutine_type<point> {
        co_return point{10, 20};
    };
    coroutine_wrapper<point> wrapper3(point_coro());
    auto result = wrapper3.get_value();
    EXPECT_EQ(result.x, 10);
    EXPECT_EQ(result.y, 20);
}
#endif
} // namespace wf::io::coroutine_wrapper_test

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}