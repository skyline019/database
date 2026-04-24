////
// @file user.h
// @brief
// 描述用户
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <string>

namespace wf::schema {

// 描述用户
class user_t
{
  private:
    std::string name_;   // 用户名
    std::string shadow_; // 密码
};

}; // namespace wf::schema