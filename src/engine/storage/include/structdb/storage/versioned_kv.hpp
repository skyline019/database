#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace structdb::storage::versioned_kv {

/// Tombstone value stored in MemTable / SST for deletes.
inline constexpr std::string_view kTomb = "mdb:tomb";
inline constexpr std::string_view kVerPrefix = "mdbver1:";

/// Only `mdb$*` keys participate in commit_seq wrapping (tests use raw keys like `k`).
bool key_versions_persist(std::string_view key);

/// Wraps logical payload as `mdbver1:<16-hex-seq>:payload`.
std::string wrap_payload(std::string_view payload, std::uint64_t commit_seq);

/// `read_max_seq == max()` means "latest visible" (no upper bound on commit seq).
bool unwrap_visible(std::string_view stored, std::uint64_t read_max_seq, std::string* body_out,
                    std::uint64_t* commit_seq_out = nullptr);

inline std::uint64_t read_seq_latest() { return (std::numeric_limits<std::uint64_t>::max)(); }

}  // namespace structdb::storage::versioned_kv
