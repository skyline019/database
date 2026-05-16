////
// @file static_hash.h
// @brief
// 静态hash
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <array>

namespace wf::meta {

namespace detail {

// FNV-1a 64位哈希算法的基础值
constexpr uint64_t fnv_offset_basis = 14695981039346656037ULL;
constexpr uint64_t fnv_prime = 1099511628211ULL;

} // namespace detail

// 编译期字符串哈希函数
constexpr uint64_t hash_string(const char *str) noexcept
{
    uint64_t hash = detail::fnv_offset_basis;
    while (*str) {
        hash ^= static_cast<uint8_t>(*str++);
        hash *= detail::fnv_prime;
    }
    return hash;
}

// 编译期字符串视图哈希函数
constexpr uint64_t hash_string_view(std::string_view str) noexcept
{
    uint64_t hash = detail::fnv_offset_basis;
    for (char c : str) {
        hash ^= static_cast<uint8_t>(c);
        hash *= detail::fnv_prime;
    }
    return hash;
}

// 编译期字节数组哈希函数
template <size_t N>
constexpr uint64_t hash_bytes(const uint8_t (&bytes)[N]) noexcept
{
    uint64_t hash = detail::fnv_offset_basis;
    for (size_t i = 0; i < N; ++i) {
        hash ^= bytes[i];
        hash *= detail::fnv_prime;
    }
    return hash;
}

// 编译期整数哈希函数
template <typename T>
constexpr uint64_t hash_integer(T value) noexcept
{
    static_assert(std::is_integral_v<T>, "T must be an integral type");

    uint64_t hash = detail::fnv_offset_basis;

    // 使用位操作而不是reinterpret_cast来避免编译期问题
    for (size_t i = 0; i < sizeof(T); ++i) {
        uint8_t byte = static_cast<uint8_t>((value >> (i * 8)) & 0xFF);
        hash ^= byte;
        hash *= detail::fnv_prime;
    }
    return hash;
}

// 编译期std::array哈希函数
template <typename T, size_t N>
constexpr uint64_t hash_array(const std::array<T, N> &arr) noexcept
{
    uint64_t hash = detail::fnv_offset_basis;
    for (size_t i = 0; i < N; ++i) {
        hash ^= static_cast<uint8_t>(arr[i]);
        hash *= detail::fnv_prime;
    }
    return hash;
}

// 编译期组合哈希函数（用于组合多个哈希值）
constexpr uint64_t combine_hashes(uint64_t hash1, uint64_t hash2) noexcept
{
    return hash1 ^ (hash2 + 0x9e3779b9 + (hash1 << 6) + (hash1 >> 2));
}

// 编译期哈希种子函数（用于带种子的哈希）
constexpr uint64_t hash_string_seeded(const char *str, uint64_t seed) noexcept
{
    uint64_t hash = seed;
    while (*str) {
        hash ^= static_cast<uint8_t>(*str++);
        hash *= detail::fnv_prime;
    }
    return hash;
}

} // namespace wf::meta