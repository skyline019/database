#include <crc32c/crc32c.h>

namespace crc32c {

namespace {

uint32_t table_entry(uint32_t index) {
    uint32_t crc = index;
    constexpr uint32_t poly = 0x82F63B78u;  // Castagnoli polynomial (reflected).
    for (int i = 0; i < 8; ++i) {
        crc = (crc & 1u) ? ((crc >> 1u) ^ poly) : (crc >> 1u);
    }
    return crc;
}

const uint32_t* crc_table() {
    static uint32_t table[256] = {};
    static bool inited = false;
    if (!inited) {
        for (uint32_t i = 0; i < 256; ++i) {
            table[i] = table_entry(i);
        }
        inited = true;
    }
    return table;
}

}  // namespace

uint32_t Extend(uint32_t init_crc, const uint8_t* data, size_t count) {
    const uint32_t* table = crc_table();
    uint32_t crc = ~init_crc;

    if (data == nullptr || count == 0) {
        return ~crc;
    }

    for (size_t i = 0; i < count; ++i) {
        const uint8_t idx = static_cast<uint8_t>(crc ^ data[i]);
        crc = table[idx] ^ (crc >> 8u);
    }
    return ~crc;
}

uint32_t Extend(uint32_t init_crc, const char* data, size_t count) {
    return Extend(init_crc, reinterpret_cast<const uint8_t*>(data), count);
}

}  // namespace crc32c
