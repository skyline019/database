////
// @file error.h
// @brief
// 模块错误码
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <stdint.h>
#include <waterfall/utils/system_errors.h>

namespace wf::io {

using error_info = wf::utils::error_info;

// 将错误信息指针转换为void*
inline void *info_to_ptr(error_info error) noexcept
{
    return const_cast<void *>(reinterpret_cast<const void *>(error));
}
// 将整数值转换为void*
inline void *int_to_ptr(int value) noexcept
{
    return reinterpret_cast<void *>(static_cast<intptr_t>(value));
}
// 将void*转换回错误信息指针
inline error_info ptr_to_info(void *result) noexcept
{
    return reinterpret_cast<error_info>(result);
}
// 将整数值转换为错误信息指针
inline error_info int_to_info(int value) noexcept
{
    return ptr_to_info(int_to_ptr(value));
}
// 将void*转换回整数值
inline int ptr_to_int(void *result) noexcept
{
    return static_cast<int>(reinterpret_cast<intptr_t>(result));
}
// 将错误信息指针转换为整数值
inline int info_to_int(error_info error) noexcept
{
    return ptr_to_int(info_to_ptr(error));
}

extern error_info kErrorThreadIsRunning;
extern error_info kErrorThreadAttributeInitFailed;
extern error_info kErrorThreadAffinitySetFailed;
extern error_info kErrorThreadCreateFailed;

} // namespace wf::io
