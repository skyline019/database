#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

struct EqBloomHeader {
    std::uint64_t data_sig{0};
    std::uint64_t attr_sig{0};
    std::uint64_t wal_lsn{0};
    std::uint32_t bits{0};
    std::uint8_t k{0};
};

// Returns false if bloom is missing/invalid. When true, `may_contain` is reliable:
// - `may_contain=false` means definitely not present (no false negatives)
// - `may_contain=true` means possibly present.
bool eq_bloom_may_contain(const std::string& bloom_path,
                          const EqBloomHeader& expected,
                          const std::string& key,
                          bool& may_contain);

// Writes bloom file bound to expected header.
void eq_bloom_write(const std::string& bloom_path,
                    const EqBloomHeader& hdr,
                    const std::vector<std::string>& keys);

