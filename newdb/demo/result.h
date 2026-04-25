#pragma once

#include <string>
#include <optional>
#include <memory>
#include <stdexcept>
#include <functional>

/**
 * @brief 统一的结果类型，类似于 Rust 的 Result<T, E>
 * 
 * 用于替代传统的 bool + error_message 模式，提供更类型安全的错误处理
 * 
 * @tparam T 成功时的返回值类型
 * @tparam E 错误类型，默认为 std::string
 */
template<typename T, typename E = std::string>
class Result {
public:
    // 禁止默认构造，使用静态工厂方法
    Result() = default;
    Result(const Result&) = default;
    Result& operator=(const Result&) = default;
    
    // 成功结果构造函数
    static Result Ok(T value) {
        Result r;
        r.m_value = std::move(value);
        r.m_is_ok = true;
        return r;
    }
    
    // 失败结果构造函数
    static Result Err(E error) {
        Result r;
        r.m_error = std::move(error);
        r.m_is_ok = false;
        return r;
    }
    
    // 检查是否为成功状态
    bool isOk() const { return m_is_ok; }
    
    // 检查是否为错误状态
    bool isErr() const { return !m_is_ok; }
    
    // 获取成功值，如果失败则抛出异常
    const T& value() const {
        if (!m_is_ok || !m_value.has_value()) {
            throw std::runtime_error("Result: attempted to get value from error state");
        }
        return *m_value;
    }
    
    // 获取成功值，如果失败则返回默认值
    T valueOr(T defaultValue) const {
        if (m_is_ok && m_value.has_value()) {
            return *m_value;
        }
        return defaultValue;
    }
    
    // 获取错误值
    const E& error() const {
        if (m_is_ok || !m_error.has_value()) {
            throw std::runtime_error("Result: attempted to get error from ok state");
        }
        return *m_error;
    }
    
    // 获取错误值，如果成功则返回默认值
    E errorOr(E defaultError) const {
        if (!m_is_ok && m_error.has_value()) {
            return *m_error;
        }
        return defaultError;
    }
    
    // map - 转换成功值
    template<typename F>
    auto map(F&& func) -> Result<decltype(func(std::declval<T>())), E> {
        if (m_is_ok && m_value.has_value()) {
            return Result<decltype(func(std::declval<T>())), E>::Ok(func(*m_value));
        }
        return Result<decltype(func(std::declval<T>())), E>::Err(m_error.value_or(E{}));
    }
    
    // mapErr - 转换错误值
    template<typename F>
    auto mapErr(F&& func) -> Result<T, decltype(func(std::declval<E>()))> {
        if (!m_is_ok && m_error.has_value()) {
            return Result<T, decltype(func(std::declval<E>()))>::Err(func(*m_error));
        }
        return Result<T, decltype(func(std::declval<E>()))>::Ok(m_value.value_or(T{}));
    }
    
    // andThen - 链式调用
    template<typename F>
    auto andThen(F&& func) -> decltype(func(std::declval<T>())) {
        if (m_is_ok && m_value.has_value()) {
            return func(*m_value);
        }
        return decltype(func(std::declval<T>()))::Err(m_error.value_or(E{}));
    }
    
    // orElse - 错误时提供备选方案
    template<typename F>
    auto orElse(F&& func) -> Result<T, E> {
        if (!m_is_ok && m_error.has_value()) {
            return func(*m_error);
        }
        return Result<T, E>::Ok(m_value.value_or(T{}));
    }
    
    // 隐式转换为 bool (仅在成功时为 true)
    explicit operator bool() const { return m_is_ok; }
    
private:
    std::optional<T> m_value;
    std::optional<E> m_error;
    bool m_is_ok = false;
};

// 特化版本：用于 void 成功类型
template<typename E>
class Result<void, E> {
public:
    Result() = default;
    Result(const Result&) = default;
    Result& operator=(const Result&) = default;
    
    static Result Ok() {
        Result r;
        r.m_is_ok = true;
        return r;
    }
    
    static Result Err(E error) {
        Result r;
        r.m_error = std::move(error);
        r.m_is_ok = false;
        return r;
    }
    
    bool isOk() const { return m_is_ok; }
    bool isErr() const { return !m_is_ok; }
    
    const E& error() const {
        if (m_is_ok || !m_error.has_value()) {
            throw std::runtime_error("Result: attempted to get error from ok state");
        }
        return *m_error;
    }
    
    E errorOr(E defaultError) const {
        if (!m_is_ok && m_error.has_value()) {
            return *m_error;
        }
        return defaultError;
    }
    
    template<typename F>
    auto map(F&& func) -> Result<decltype(func()), E> {
        if (m_is_ok) {
            return Result<decltype(func()), E>::Ok(func());
        }
        return Result<decltype(func()), E>::Err(m_error.value_or(E{}));
    }
    
    explicit operator bool() const { return m_is_ok; }
    
private:
    std::optional<E> m_error;
    bool m_is_ok = false;
};

// 简化的 Result<bool> 别名，用于传统 bool + string 模式
using BoolResult = Result<bool, std::string>;

// 成功/失败的便捷函数
template<typename T>
Result<T, std::string> Ok(T value) {
    return Result<T, std::string>::Ok(std::move(value));
}

template<typename T>
Result<T, std::string> ErrStr(const char* error) {
    return Result<T, std::string>::Err(std::string(error));
}

template<typename T>
Result<T, std::string> ErrStr(const std::string& error) {
    return Result<T, std::string>::Err(error);
}

// void 版本的便捷函数
inline Result<void, std::string> Ok() {
    return Result<void, std::string>::Ok();
}

inline Result<void, std::string> ErrStr(const char* error) {
    return Result<void, std::string>::Err(std::string(error));
}

inline Result<void, std::string> ErrStr(const std::string& error) {
    return Result<void, std::string>::Err(error);
}
