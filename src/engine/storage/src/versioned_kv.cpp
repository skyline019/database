#include "structdb/storage/versioned_kv.hpp"

#include <sstream>
#include <iomanip>

namespace structdb::storage::versioned_kv {

bool key_versions_persist(std::string_view key) {
  constexpr std::string_view pfx = "mdb$";
  return key.size() >= pfx.size() && key.compare(0, pfx.size(), pfx) == 0;
}

static void write_hex16(std::ostringstream& o, std::uint64_t seq) {
  o << std::hex << std::setfill('0') << std::setw(16) << seq << std::dec;
}

static bool read_hex16(std::string_view s, std::uint64_t* out) {
  if (!out || s.size() != 16) return false;
  std::uint64_t v = 0;
  for (char c : s) {
    v <<= 4;
    if (c >= '0' && c <= '9')
      v |= static_cast<std::uint64_t>(c - '0');
    else if (c >= 'a' && c <= 'f')
      v |= static_cast<std::uint64_t>(10 + c - 'a');
    else if (c >= 'A' && c <= 'F')
      v |= static_cast<std::uint64_t>(10 + c - 'A');
    else
      return false;
  }
  *out = v;
  return true;
}

std::string wrap_payload(std::string_view payload, std::uint64_t commit_seq) {
  std::ostringstream o;
  o << kVerPrefix;
  write_hex16(o, commit_seq);
  o << ':';
  o.write(payload.data(), static_cast<std::streamsize>(payload.size()));
  return o.str();
}

bool unwrap_visible(std::string_view stored, std::uint64_t read_max_seq, std::string* body_out,
                    std::uint64_t* commit_seq_out) {
  if (body_out) body_out->clear();
  if (stored == kTomb) return false;
  if (stored.size() < kVerPrefix.size() + 16 + 1) {
    if (body_out) body_out->assign(stored.begin(), stored.end());
    if (commit_seq_out) *commit_seq_out = 0;
    return true;
  }
  if (stored.compare(0, kVerPrefix.size(), kVerPrefix) != 0) {
    if (body_out) body_out->assign(stored.begin(), stored.end());
    if (commit_seq_out) *commit_seq_out = 0;
    return true;
  }
  const std::size_t hex_start = kVerPrefix.size();
  const std::size_t colon = hex_start + 16;
  if (stored.size() < colon + 1 || stored[colon] != ':') {
    if (body_out) body_out->assign(stored.begin(), stored.end());
    if (commit_seq_out) *commit_seq_out = 0;
    return true;
  }
  std::uint64_t seq = 0;
  if (!read_hex16(stored.substr(hex_start, 16), &seq)) {
    if (body_out) body_out->assign(stored.begin(), stored.end());
    if (commit_seq_out) *commit_seq_out = 0;
    return true;
  }
  if (commit_seq_out) *commit_seq_out = seq;
  const std::uint64_t cap = read_max_seq;
  if (cap != read_seq_latest() && seq > cap) return false;
  if (body_out) body_out->assign(stored.begin() + static_cast<std::ptrdiff_t>(colon + 1), stored.end());
  return true;
}

}  // namespace structdb::storage::versioned_kv
