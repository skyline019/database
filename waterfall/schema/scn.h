////
// @file scn.h
// @brief
// 系统逻辑时钟
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

#include <stdint.h>

namespace wf::schema {

using scn_t = uint64_t; // 系统逻辑时钟类型

scn_t generate_scn(); // 生成系统逻辑时钟

} // namespace wf::schema
