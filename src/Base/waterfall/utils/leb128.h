//===- llvm/Support/LEB128.h - [SU]LEB128 utility functions -----*- C++ -*-===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
//
// This file declares some utility functions for encoding SLEB128 and
// ULEB128 values.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "error.h"

#if !defined(unlikely)
#  if defined(__GNUC__) || defined(__clang__)
#    define unlikely(x) __builtin_expect(!!(x), 0)
#    define likely(x) __builtin_expect(!!(x), 1)
#  else
#    define unlikely(x) (x)
#    define likely(x) (x)
#  endif
#endif

namespace wf::utils {

/// Utility function to calculate the length in bytes of a ULEB128 encoded
/// value (full 64-bit range; avoids LLP64 `unsigned long` / `size_t` mismatches).
inline unsigned int uleb128_length(std::uint64_t value)
{
    if (value < (1ULL << 7))
        return 1;
    if (value < (1ULL << 14))
        return 2;
    if (value < (1ULL << 21))
        return 3;
    if (value < (1ULL << 28))
        return 4;
    if (value < (1ULL << 35))
        return 5;
    if (value < (1ULL << 42))
        return 6;
    if (value < (1ULL << 49))
        return 7;
    if (value < (1ULL << 56))
        return 8;
    if (value < (1ULL << 63))
        return 9;
    return 10;
}

/// Utility function to encode a ULEB128 value to a buffer. Returns
/// the length in bytes of the encoded value.
inline unsigned int
encode_uleb128(std::uint64_t value, unsigned char *p, unsigned int padto = 0)
{
    unsigned char *orig_p = p;
    unsigned int count = 0;
    do {
        unsigned char Byte = static_cast<unsigned char>(value & 0x7f);
        value >>= 7;
        count++;
        if (value != 0 || count < padto)
            Byte |= 0x80; // Mark this byte to show that more bytes will follow.
        *p++ = Byte;
    } while (value != 0);

    // Pad with 0x80 and emit a null byte at the end.
    if (count < padto) {
        for (; count < padto - 1; ++count)
            *p++ = '\x80';
        *p++ = '\x00';
    }

    return (unsigned int) (p - orig_p);
}

/// Utility function to decode a ULEB128 value.
///
/// If \p error is non-null, it will point to a static error message,
/// if an error occurred. It will not be modified on success.
inline std::uint64_t decode_uleb128(
    const unsigned char *p,
    unsigned int *n = nullptr,
    const unsigned char *end = nullptr,
    const char **error = nullptr)
{
    const unsigned char *orig_p = p;
    std::uint64_t value = 0;
    unsigned int shift = 0;
    do {
        if (unlikely(p == end)) {
            if (error) *error = kErrorUleb128Malformed;
            value = 0;
            break;
        }
        const std::uint64_t Slice = static_cast<std::uint64_t>(*p & 0x7f);
        if (unlikely(shift >= 63) &&
            ((shift == 63 && (Slice << shift >> shift) != Slice) ||
             (shift > 63 && Slice != 0))) {
            if (error) *error = kErrorUleb128Toolong;
            value = 0;
            break;
        }
        value += Slice << shift;
        shift += 7;
    } while (*p++ >= 128);
    if (n) *n = (unsigned) (p - orig_p);
    return value;
}

/// Utility function to calculate the length in bytes of a SLEB128 encoded
/// value. Uses conditional checks based on SLEB128 encoding rules.
inline unsigned int sleb128_length(long value)
{
    // SLEB128 编码阈值表（2^(7n-1)）
    if (value >= -64L && value <= 63L)
        return 1;
    else if (value >= -8192L && value <= 8191L)
        return 2;
    else if (value >= -1048576L && value <= 1048575L)
        return 3;
    else if (value >= -134217728L && value <= 134217727L)
        return 4;
    else if (value >= -17179869184L && value <= 17179869183L)
        return 5;
    else if (value >= -2199023255552L && value <= 2199023255551L)
        return 6;
    else if (value >= -281474976710656L && value <= 281474976710655L)
        return 7;
    else if (value >= -36028797018963968L && value <= 36028797018963967L)
        return 8;
    else if (
        value >= -4611686018427387904L &&
        value <= 4611686018427387903L) // ✅ 2^62
        return 9;
    else
        return 10; // ✅ LONG_MIN ~ -4611686018427387905 或 4611686018427387904
                   // ~ LONG_MAX
}

/// Utility function to encode a SLEB128 value to a buffer. Returns
/// the length in bytes of the encoded value.
inline unsigned int
encode_sleb128(long value, unsigned char *p, unsigned int padto = 0)
{
    unsigned char *orig_p = p;
    unsigned int count = 0;
    bool More;
    do {
        unsigned char Byte = value & 0x7f;
        // NOTE: this assumes that this signed shift is an arithmetic right
        // shift.
        value >>= 7;
        More = value != ((Byte & 0x40) ? -1 : 0);
        count++;
        if (More || count < padto)
            Byte |= 0x80; // Mark this byte to show that more bytes will follow.
        *p++ = Byte;
    } while (More);

    // Pad with 0x80 and emit a terminating byte at the end.
    if (count < padto) {
        unsigned char Padvalue = value < 0 ? 0x7f : 0x00;
        for (; count < padto - 1; ++count)
            *p++ = (Padvalue | 0x80);
        *p++ = Padvalue;
    }
    return (unsigned int) (p - orig_p);
}

/// Utility function to decode a SLEB128 value.
///
/// If \p error is non-null, it will point to a static error message,
/// if an error occurred. It will not be modified on success.
inline long decode_sleb128(
    const unsigned char *p,
    unsigned int *n = nullptr,
    const unsigned char *end = nullptr,
    const char **error = nullptr)
{
    const unsigned char *orig_p = p;
    long value = 0;
    unsigned int shift = 0;
    unsigned char Byte;
    do {
        if (unlikely(p == end)) {
            if (error) *error = kErrorSleb128Malformed;
            if (n) *n = (unsigned int) (p - orig_p);
            return 0;
        }
        Byte = *p;
        unsigned long Slice = Byte & 0x7f;
        if (unlikely(shift >= 63) &&
            ((shift == 63 && Slice != 0 && Slice != 0x7f) ||
             (shift > 63 && Slice != (value < 0 ? 0x7f : 0x00)))) {
            if (error) *error = kErrorSleb128Toolong;
            if (n) *n = (unsigned) (p - orig_p);
            return 0;
        }
        value |= Slice << shift;
        shift += 7;
        ++p;
    } while (Byte >= 128);
    // Sign extend negative numbers if needed.
    if (shift < 64 && (Byte & 0x40))
        value |= (long) 0x7fffffffffffffff << shift;
    if (n) *n = (unsigned int) (p - orig_p);
    return value;
}

} // namespace wf::utils
