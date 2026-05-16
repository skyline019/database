////
// @file noncopyable.h
// @brief
// 不可拷贝基类
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

namespace wf::utils {

class noncopyable
{
  protected:
    noncopyable() = default;
    ~noncopyable() = default;

  public:
    noncopyable(const noncopyable &) = delete;
    noncopyable &operator=(const noncopyable &) = delete;
};

} // namespace wf::utils