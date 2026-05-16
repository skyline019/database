////
// @file config.h
// @brief
// 配置头文件
//
// @author niexw
// @email niexiaowen@uestc.edu.cn
//
#pragma once

// 工程名
constexpr char PROJECT_NAME[] = "waterfall-0.1";

// 分支预测
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

// 编译期gcc/clang字节序检测
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#    define LITTLE_ENDIAN 1
#    define BIG_ENDIAN 0
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#    define LITTLE_ENDIAN 0
#    define BIG_ENDIAN 1
#else
#    define LITTLE_ENDIAN 0
#    define BIG_ENDIAN 0
#endif
