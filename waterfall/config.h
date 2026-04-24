#pragma once

#include <cstdint>

#if defined(_MSC_VER)
#    if defined(__i386__) || defined(_M_IX86) || defined(_X86_) || defined(__amd64__) || defined(_M_X64) || defined(_M_AMD64)
#        define __BYTE_ORDER__ __ORDER_LITTLE_ENDIAN__
#    else
#        define __BYTE_ORDER__ __ORDER_BIG_ENDIAN__
#    endif
#    ifndef __ORDER_LITTLE_ENDIAN__
#        define __ORDER_LITTLE_ENDIAN__ 1234
#    endif
#    ifndef __ORDER_BIG_ENDIAN__
#        define __ORDER_BIG_ENDIAN__ 4321
#    endif
#endif

constexpr char PROJECT_NAME[] = "waterfall-0.1";

#if defined(__GNUC__) || defined(__clang__)
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

#if !defined(_WIN32)
// Endianness helpers (Linux/macOS/other Unix).
// Some toolchains require explicit includes for htole*/le*toh.
#  if defined(__linux__)
#    include <endian.h>
#  elif defined(__APPLE__)
#    include <libkern/OSByteOrder.h>
#    ifndef htole16
#      define htole16(x) OSSwapHostToLittleInt16((uint16_t)(x))
#    endif
#    ifndef le16toh
#      define le16toh(x) OSSwapLittleToHostInt16((uint16_t)(x))
#    endif
#    ifndef htole32
#      define htole32(x) OSSwapHostToLittleInt32((uint32_t)(x))
#    endif
#    ifndef le32toh
#      define le32toh(x) OSSwapLittleToHostInt32((uint32_t)(x))
#    endif
#    ifndef htole64
#      define htole64(x) OSSwapHostToLittleInt64((uint64_t)(x))
#    endif
#    ifndef le64toh
#      define le64toh(x) OSSwapLittleToHostInt64((uint64_t)(x))
#    endif
#  endif

// Generic fallback if still missing (e.g. musl or unusual libc).
#  ifndef htole16
#    if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#      define htole16(x) ((uint16_t)(x))
#      define le16toh(x) ((uint16_t)(x))
#      define htole32(x) ((uint32_t)(x))
#      define le32toh(x) ((uint32_t)(x))
#      define htole64(x) ((uint64_t)(x))
#      define le64toh(x) ((uint64_t)(x))
#    else
#      define htole16(x) ((uint16_t)__builtin_bswap16((uint16_t)(x)))
#      define le16toh(x) ((uint16_t)__builtin_bswap16((uint16_t)(x)))
#      define htole32(x) ((uint32_t)__builtin_bswap32((uint32_t)(x)))
#      define le32toh(x) ((uint32_t)__builtin_bswap32((uint32_t)(x)))
#      define htole64(x) ((uint64_t)__builtin_bswap64((uint64_t)(x)))
#      define le64toh(x) ((uint64_t)__builtin_bswap64((uint64_t)(x)))
#    endif
#  endif
#endif

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