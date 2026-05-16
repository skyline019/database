////
// @file coro.h
// @brief
// 协程
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <assert.h>
#include <utility>
#include <coroutine>
#include <type_traits>
#include <waterfall/meta/tag_invoke.h>
#include "task.h"

namespace wf::io {
using meta::tag_t;
template <typename Return>
struct final_awaiter;
template <typename Return>
struct yield_awaiter;
template <typename Return>
struct await_awaiter;
template <typename Return>
class coroutine_wrapper;

// clang-format off
static constexpr unsigned char kCoroStateOffline =   0b00000001; // 离线
static constexpr unsigned char kCoroStateDetached =  0b00000010; // final分离
// clang-format on

// 定义await_transform的cpo，便于用户扩展awaitable
namespace _awaitable {
namespace cpo {

// await_suspend可能的返回值
template <typename R>
concept _await_suspend_possibble_result_v =
    std::same_as<R, void> ||                  // void
    std::same_as<R, bool> ||                  // bool
    std::same_as<R, std::coroutine_handle<>>; // coroutine_handle

// awaiter概念
template <typename A>
concept is_awaiter_v = requires {
    { std::declval<A>().await_ready() } -> std::same_as<bool>;
    {
        std::declval<A>().await_suspend(std::declval<std::coroutine_handle<>>())
    } -> _await_suspend_possibble_result_v;
    { std::declval<A>().await_resume() };
};

// 检查对象内部是否重载co_await运算符
template <typename Awaitable, typename = void>
constexpr bool has_member_operator_co_await_v = false;
template <typename Awaitable>
constexpr bool has_member_operator_co_await_v<
    Awaitable,
    std::void_t<decltype(std::declval<Awaitable>().operator co_await())>> =
    true;

// 检查全局是否重载对象的co_await运算符
template <typename Awaitable, typename = void>
constexpr bool has_free_operator_co_await_v = false;
template <typename Awaitable>
constexpr bool has_free_operator_co_await_v<
    Awaitable,
    std::void_t<decltype(operator co_await(std::declval<Awaitable>()))>> = true;

struct _fn
{
    template <typename Promise, typename Awaitable>
    inline constexpr decltype(auto)
    operator()(Promise &promise, Awaitable &&awaitable) const
    {
        // 优先使用co_await运算符重载返回的awaiter，这符合c++规范。其次调用
        // 用户重载的await_transform函数。
        if constexpr (is_awaiter_v<Awaitable>) {
            // 直接awaiter，无需await_transform
            return static_cast<Awaitable &&>(awaitable);
        } else if constexpr (has_member_operator_co_await_v<Awaitable>) {
            // 内置co_await运算符
            return static_cast<Awaitable &&>(awaitable).operator co_await();
        } else if constexpr (has_free_operator_co_await_v<Awaitable>) {
            // 自由co_await运算符
            return operator co_await(static_cast<Awaitable &&>(awaitable));
        } else {
            // await_transform
            return await_transform(
                promise, static_cast<Awaitable &&>(awaitable));
        }
    }
};
} // namespace cpo
// await_transform函数在promise中被调用
inline constexpr cpo::_fn await_transform{};
} // namespace _awaitable

// 协程promise基类
class promise_base : public async_task
{
  protected:
    promise_base *continuation_; // 指向下一个协程
    unsigned char state_;        // 协程状态

  public:
    promise_base() noexcept
        : async_task(task_resume)
        , continuation_(nullptr)
        , state_(0) // 缺省为inline
    {}

    // 禁止抛出异常
    void unhandled_exception() noexcept { assert(false); }

    // 恢复协程，根据状态调用inline_resume或offline_resume
    void resume() noexcept;
    void set_offline() noexcept { state_ |= kCoroStateOffline; }
    void set_inline() noexcept { state_ &= ~kCoroStateOffline; }
    void set_detached() noexcept { state_ |= kCoroStateDetached; }

    // 有协程等待，恢复下一个协程
    void resume_continuation() noexcept;
    void set_continuation(promise_base *continuation) noexcept;
    promise_base *get_continuation() const noexcept { return continuation_; }

  private:
    // 在线恢复promise
    void inline_resume() noexcept;
    // 离线恢复promise，先用executor调度，然后调度器调用do_resume恢复
    void offline_resume() noexcept;
    static void task_resume(void *owner, async_task *task, void *);

    template <typename Return>
    friend struct final_awaiter;
    template <typename Return>
    friend struct await_awaiter;
};

// 返回值
template <typename Return>
class return_base : public promise_base
{
  public:
    using return_type = Return;

  protected:
    return_type return_value_;

  public:
    return_base()
        : return_value_()
    {}

    inline void return_value(return_type &&value)
    {
        return_value_ = std::forward<return_type>(value);
    }
    auto yield_value(return_type &&value);

    return_type get_value() const noexcept { return std::move(return_value_); }
};
template <>
class return_base<void> : public promise_base
{
  public:
    using return_type = void;

    inline constexpr void return_void() {}
    yield_awaiter<void> yield_value();
};

// 异步协程promise
template <typename Return>
class async_promise : public return_base<Return>
{
  public:
    using return_type = Return;
    using handle_type = std::coroutine_handle<async_promise>;

  public:
    async_promise()
        : return_base<Return>()
    {}

    auto get_return_object();
    std::suspend_always initial_suspend() noexcept { return {}; }
    final_awaiter<return_type> final_suspend() noexcept { return {this}; }

    template <typename Awaitable>
    auto await_transform(Awaitable &&awaitable)
    {
        return _awaitable::await_transform(
            *this, std::forward<Awaitable>(awaitable));
    }
};

// awaiter基类
struct awaiter_base
{
    promise_base *promise_; // 指向协程promise的指针

    // 默认构造
    awaiter_base()
        : promise_(nullptr)
    {}
    // 显式构造
    explicit awaiter_base(promise_base *promise) noexcept
        : promise_(promise)
    {}

    // 禁止拷贝
    awaiter_base(const awaiter_base &) = delete;
    awaiter_base &operator=(const awaiter_base &) = delete;

    // 允许移动
    awaiter_base(awaiter_base &&other) noexcept
        : promise_(other.promise_)
    {}
    awaiter_base &operator=(awaiter_base &&other) noexcept
    {
        if (this != &other) promise_ = other.promise_;
        return *this;
    }

    ~awaiter_base() = default;
};

// final awaiter
template <typename Return>
struct final_awaiter : awaiter_base
{
    final_awaiter(async_promise<Return> *promise) noexcept
        : awaiter_base(promise)
    {}
    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<async_promise<Return>>) noexcept;
    void await_resume() const noexcept {}
};

// co_yield awaiter
template <typename Return>
struct yield_awaiter : awaiter_base
{
    yield_awaiter(async_promise<Return> *promise) noexcept
        : awaiter_base(promise)
    {}
    constexpr bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<async_promise<Return>>) noexcept;
    Return await_resume() noexcept;
};

// co_await awaiter
template <typename Return>
struct await_awaiter : awaiter_base
{
    await_awaiter(async_promise<Return> *promise) noexcept
        : awaiter_base(promise) // promise指向callee
    {}
    constexpr bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<async_promise<Return>>) noexcept;
    Return await_resume() noexcept;
};

// 协程直接返回对象
template <typename Return>
class coroutine_type
{
  public:
    using return_type = Return;
    using promise_type = async_promise<Return>;
    using handle_type = std::coroutine_handle<promise_type>;

  private:
    handle_type handle_;

  public:
    // 缺省构造与显式构造
    coroutine_type() = default;
    explicit coroutine_type(handle_type h) noexcept
        : handle_(h)
    {}
    // 支持移动构造
    coroutine_type(coroutine_type &&other) noexcept
        : handle_(other.handle_)
    {
        other.handle_ = nullptr;
    }
    coroutine_type &operator=(coroutine_type &&other) noexcept
    {
        if (this != &other) {
            if (handle_) handle_.destroy();
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }
    // 关闭拷贝构造
    coroutine_type(const coroutine_type &) = delete;
    coroutine_type &operator=(const coroutine_type &) = delete;
    // 析构函数销毁协程
    ~coroutine_type()
    {
        if (handle_) handle_.destroy();
    }

    // 获取协程promise
    promise_type &promise() noexcept { return handle_.promise(); }

    // 重载co_await运算符，co_await expr，expr返回coroutine_type类型，
    // 此时coroutine_type对应于callee。
    // caller等待callee完成，挂起，
    auto operator co_await() noexcept
    {
        return await_awaiter<return_type>{&promise()};
    }
};

// coroutine_type封装
// auto wrapper = coroutine_wrapper<int>()
//     .detach()                    // 设置为分离模式
//     .offline()                   // 设置为离线执行
//     .executor(some_executor)     // 设置执行器
//     .create([]() -> coroutine_type<int> {  // 最后创建协程
//         // 协程函数体
//         co_return 42;
//     });
template <typename Return>
class coroutine_wrapper
{
  private:
    coroutine_type<Return> coro_;
    bool detached_ = false;
    bool offline_ = false;
    runtime_executor *executor_ = nullptr;

  public:
    using return_type = Return;
    using promise_type = typename coroutine_type<Return>::promise_type;
    using handle_type = typename coroutine_type<Return>::handle_type;

    // 缺省构造
    coroutine_wrapper() = default;
    // 移动构造
    coroutine_wrapper(coroutine_wrapper &&other) noexcept
        : coro_(std::move(other.coro_))
    {}
    coroutine_wrapper &operator=(coroutine_wrapper &&other) noexcept
    {
        if (this != &other) { coro_ = std::move(other.coro_); }
        return *this;
    }

    // 关闭拷贝构造
    coroutine_wrapper(const coroutine_wrapper &) = delete;
    coroutine_wrapper &operator=(const coroutine_wrapper &) = delete;

    // 析构函数
    ~coroutine_wrapper() = default;

    // 获取内部coroutine_type
    coroutine_type<Return> &get() noexcept { return coro_; }
    const coroutine_type<Return> &get() const noexcept { return coro_; }

    // 转换为coroutine_type
    coroutine_type<Return> unwrap() && noexcept { return std::move(coro_); }
    // 隐式转换操作符
    operator coroutine_type<Return> &() & noexcept { return coro_; }
    operator const coroutine_type<Return> &() const & noexcept { return coro_; }
    operator coroutine_type<Return>() && noexcept { return std::move(coro_); }
    // 布尔转换（检查是否有效）
    explicit operator bool() const noexcept
    {
        return static_cast<bool>(coro_.handle_);
    }

    // 协程操作接口
    bool done() const noexcept { return coro_.handle_.done(); }
    void resume() { coro_.promise().resume(); }
    // 获取返回值
    return_type get_value() { return coro_.promise().get_value(); }

    coroutine_wrapper &detach(bool detached = true) noexcept
    {
        detached_ = detached;
        return *this;
    }

    coroutine_wrapper &offline(bool offline = true) noexcept
    {
        offline_ = offline;
        return *this;
    }

    coroutine_wrapper &executor(runtime_executor *executor) noexcept
    {
        executor_ = executor;
        return *this;
    }

    // 创建协程，移动语义
    template <typename Func>
    coroutine_wrapper &&create(Func &&func)
    {
        // 执行协程
        coro_ = std::forward<Func>(func)();
        // 初始挂起，设置协程特性
        auto &promise = coro_.promise();
        if (detached_) {
            coro_.handle_ = nullptr; // 断开handle，协程结束后自动销毁
            promise.set_detached();
        }
        if (offline_) promise.set_offline();
        if (executor_) promise.set_executor(executor_);
        // 恢复协程执行
        promise.resume();
        return std::move(*this);
    }

    auto operator co_await() noexcept
    {
        return await_awaiter<return_type>{&coro_.promise()};
    }
};

template <typename Return>
bool final_awaiter<Return>::await_ready() const noexcept
{
    return promise_->state_ & kCoroStateDetached;
}

template <typename Return>
void final_awaiter<Return>::await_suspend(
    std::coroutine_handle<async_promise<Return>>) noexcept
{
    promise_->resume_continuation();
}

template <typename Return>
void yield_awaiter<Return>::await_suspend(
    std::coroutine_handle<async_promise<Return>>) noexcept
{
    promise_->resume_continuation();
}

template <typename Return>
Return yield_awaiter<Return>::await_resume() noexcept
{
    if constexpr (!std::is_same_v<Return, void>)
        return std::move(
            static_cast<return_base<Return> *>(promise_)
                ->return_value_); // yield_value设置的值
}

template <typename Return>
void await_awaiter<Return>::await_suspend(
    std::coroutine_handle<async_promise<Return>> handle) noexcept
{
    promise_base *self = &handle.promise();
    promise_->set_continuation(
        static_cast<promise_base *>(self)); // promise指向callee，设置caller
    promise_->resume();                     // 恢复callee执行
}

template <typename Return>
Return await_awaiter<Return>::await_resume() noexcept
{
    if constexpr (!std::is_same_v<Return, void>)
        return std::move(
            static_cast<return_base<Return> *>(promise_)
                ->return_value_); // co_await等待的协程设置的返回值
}

template <typename Return>
auto async_promise<Return>::get_return_object()
{
    return coroutine_type<return_type>(handle_type::from_promise(*this));
}

template <typename Return>
auto return_base<Return>::yield_value(return_type &&value)
{
    if constexpr (!std::is_same_v<return_type, void>)
        this->return_value_ = std::forward<return_type>(value);
    return yield_awaiter<return_type>{
        static_cast<async_promise<return_type> *>(this)};
}

} // namespace wf::io