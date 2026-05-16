// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

namespace wf::utils {

static constexpr size_t kArenaAlignment = 8; // 对齐字节数

class object_arena
{
  public:
    object_arena();

    object_arena(const object_arena &) = delete;
    object_arena &operator=(const object_arena &) = delete;

    ~object_arena();

    // Return a pointer to a newly allocated memory block of "bytes" bytes.
    char *allocate(size_t bytes);

    // Allocate memory with the normal alignment guarantees provided by malloc.
    char *aligned_allocate(size_t bytes);

    // Returns an estimate of the total memory usage of data allocated
    // by the arena.
    size_t usage() const
    {
        return memory_usage_.load(std::memory_order_relaxed);
    }

  private:
    char *allocate_fallback(size_t bytes);
    char *allocate_new_block(size_t block_bytes);

    // Allocation state
    char *alloc_ptr_;
    size_t alloc_bytes_remaining_;

    // Array of new[] allocated memory blocks
    std::vector<char *> blocks_;

    // Total memory usage of the arena.
    //
    // TODO(costan): This member is accessed via atomics, but the others are
    //               accessed without any locking. Is this OK?
    std::atomic<size_t> memory_usage_;
};

inline char *object_arena::allocate(size_t bytes)
{
    // The semantics of what to return are a bit messy if we allow
    // 0-byte allocations, so we disallow them here (we don't need
    // them for our internal use).
    assert(bytes > 0);

    // 确保对齐
    bytes = (bytes + kArenaAlignment - 1) & ~(kArenaAlignment - 1);

    if (bytes <= alloc_bytes_remaining_) {
        char *result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }
    return allocate_fallback(bytes);
}

// 自定义分配器实现
template <typename T>
class arena_allocator
{
  public:
    using value_type = T;
    using pointer = T *;
    using const_pointer = const T *;
    using reference = T &;
    using const_reference = const T &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    struct rebind
    {
        using other = arena_allocator<U>;
    };

    // 默认构造函数（STL容器需要）
    arena_allocator() noexcept
        : arena_(nullptr)
    {}

    // 构造函数
    explicit arena_allocator(object_arena *arena) noexcept
        : arena_(arena)
    {}

    // 拷贝构造函数
    template <typename U>
    arena_allocator(const arena_allocator<U> &other) noexcept
        : arena_(other.arena_)
    {}

    // 分配内存 - 修复类型转换问题
    T *allocate(size_type n)
    {
        if (n == 0) { return nullptr; }
        if (n > max_size()) { return nullptr; }

        // 使用reinterpret_cast进行正确的类型转换
        char *raw_ptr = arena_->allocate(n * sizeof(T));
        if (!raw_ptr) { return nullptr; }

        return reinterpret_cast<T *>(raw_ptr);
    }

    // 释放内存（arena通常不释放单个对象）
    void deallocate(T *p, size_type n) noexcept
    {
        // arena分配器不释放单个对象
    }

    // 最大可分配大小
    size_type max_size() const noexcept
    {
        return std::numeric_limits<size_type>::max() / sizeof(T);
    }

    // 构造对象
    template <typename U, typename... Args>
    void construct(U *p, Args &&...args)
    {
        ::new (static_cast<void *>(p)) U(std::forward<Args>(args)...);
    }

    // 销毁对象
    template <typename U>
    void destroy(U *p)
    {
        p->~U();
    }

    // 获取关联的arena
    object_arena *get_arena() const noexcept { return arena_; }

    // 检查是否有效
    bool is_valid() const noexcept { return arena_ != nullptr; }

  private:
    object_arena *arena_;

    template <typename U>
    friend class arena_allocator;
};

// 分配器比较操作
template <typename T, typename U>
bool operator==(const arena_allocator<T> &a, const arena_allocator<U> &b)
{
    return a.get_arena() == b.get_arena();
}

template <typename T, typename U>
bool operator!=(const arena_allocator<T> &a, const arena_allocator<U> &b)
{
    return !(a == b);
}

} // namespace wf::utils