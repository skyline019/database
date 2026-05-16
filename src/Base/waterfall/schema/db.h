////
// @file db.h
// @brief
// 描述数据库
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <string>

namespace wf::schema {

// 描述数据库
class database_t
{
  private:
    std::string name_;      // 库名
    std::string directory_; // 数据库相对目录
};

} // namespace wf::schema
