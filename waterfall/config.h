#pragma once

#include <cstdint>

// Resolved relative to this file (`waterfall/config.h`) so targets that only force-include
// `waterfall/config.h` need not add `waterfall/include` to the compile line.
#include "include/waterfall/endian_shim.h"

constexpr char PROJECT_NAME[] = "waterfall-0.1";

#if defined(__GNUC__) || defined(__clang__)
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif
