//
// detail/call_stack.hpp
// ~~~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2025 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
#pragma once

#include "noncopyable.h"

namespace wf::utils {

// 在栈上创建contex标记，在嵌套函数中发现被该函数调用
template <typename Obj>
class call_stack
{
  public:
    // 栈上的标记对象
    class tag : private utils::noncopyable
    {
      private:
        Obj *object_; // 指向value
        tag *prev_;   // 指向栈上的上一个context
        friend class call_stack<Obj>;

      public:
        tag(Obj &o)
            : object_(&o)
            , prev_(call_stack<Obj>::top_)
        {
            call_stack<Obj>::top_ = this;
        }

        ~tag() { call_stack<Obj>::top_ = prev_; }
    };

  private:
    static thread_local tag *top_; // 指向栈顶的context

  public:
    // 返回栈顶context的value
    static Obj *top()
    {
        tag *elem = top_;
        return elem ? elem->object_ : nullptr;
    }
    static void set_top(tag *root)
    {
        // 找到顶部
        tag *current = top_;
        while (current != nullptr)
            current = current->prev_;
        // 将顶部设置为root
        current = root;
    }
    friend class tag;
};

// tls上只有一个指针的大小
template <typename Obj>
thread_local call_stack<Obj>::tag *call_stack<Obj>::top_ = nullptr;

} // namespace wf::utils