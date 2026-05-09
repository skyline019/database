#pragma once

#include <cstddef>
#include <cstdint>

namespace crc32c {

uint32_t Extend(uint32_t init_crc, const uint8_t* data, size_t count);
uint32_t Extend(uint32_t init_crc, const char* data, size_t count);

}  // namespace crc32c
