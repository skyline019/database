////
// @file system.h
// @brief
// 描述系统
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <string>

namespace wf::schema {

// 描述系统
class system_t
{
  private:
    std::string config_file_; // 系统配置文件
    std::string directory_;   // 系统主目录
    std::string version_;     // 系统版本
    unsigned short port_;     // 服务端口
};

} // namespace wf::schema
