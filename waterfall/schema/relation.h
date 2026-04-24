////
// @file relation.h
// @brief
// 描述关系
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <string>
#include <vector>
#include "datatype.h"

namespace wf::schema {

// 描述字段
class field_t
{
    std::string name_; // 字段名
    datatype_t type_;  // 数据类型
};

// 描述关系
class relation_t
{
  private:
    std::string name_;                // 关系名
    std::string path_;                // 文件路径
    size_t fields_;                   // 字段数
    size_t key_;                      // 主键下标
    std::vector<std::string> fields_; // 字段名
    // size_t records_;   // 记录数
};

} // namespace wf::schema