////
// @file error.h
// @brief
// 错误信息
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <string.h>

namespace wf::utils {

// 错误信息
using error_info = const char *;

// 成功错误信息
inline error_info OK = nullptr;

// 比较两个错误信息
inline bool cmp_error_info(error_info a, error_info b)
{
    return ::strcmp(a, b) == 0;
}

// utils模块错误信息
extern error_info kErrorUleb128Malformed;
extern error_info kErrorUleb128Toolong;
extern error_info kErrorSleb128Malformed;
extern error_info kErrorSleb128Toolong;

} // namespace wf::utils