#include "structdb/storage/wal_replay_applier.hpp"

#include "structdb/storage/storage_engine.hpp"
#include "structdb/storage/versioned_kv.hpp"

#include <cstdint>
#include <string>
#include <vector>

#include "storage_engine_detail.hpp"

namespace sed = structdb::storage::storage_engine_detail;

namespace structdb::storage {

bool WalReplayApplier::apply_line_unlocked(std::string_view line, std::string* error_out) {
  if (line.empty() || line.back() != '\n') {
    if (error_out) *error_out = "wal replay: bad line";
    return false;
  }
  line.remove_suffix(1);
  const auto eq = line.find('=');
  if (eq == std::string_view::npos) {
    if (error_out) *error_out = "wal replay: missing '='";
    return false;
  }
  std::string key(line.substr(0, eq));
  std::string val(line.substr(eq + 1));
  engine_.observe_stored_commit_seq_(val);
  engine_.mem_mgr_.active().put(std::move(key), std::move(val));
  return true;
}

bool WalReplayApplier::apply_batch_unlocked(std::string_view rec, std::string* error_out) {
  constexpr std::string_view kHdr = "STDBBW1\n";
  if (rec.size() < kHdr.size() || rec.compare(0, kHdr.size(), kHdr) != 0) {
    if (error_out) *error_out = "wal replay batch: bad header";
    return false;
  }
  const std::uint8_t* p = reinterpret_cast<const std::uint8_t*>(rec.data()) + kHdr.size();
  const std::uint8_t* end = reinterpret_cast<const std::uint8_t*>(rec.data() + rec.size());
  std::uint32_t ndels = 0;
  if (!sed::read_u32_le(p, end, &ndels)) {
    if (error_out) *error_out = "wal replay batch: ndels";
    return false;
  }
  constexpr std::uint32_t kMaxOps = 16 * 1024 * 1024;
  if (ndels > kMaxOps) {
    if (error_out) *error_out = "wal replay batch: ndels too large";
    return false;
  }
  std::vector<std::string> del_keys;
  del_keys.reserve(static_cast<std::size_t>(ndels));
  for (std::uint32_t i = 0; i < ndels; ++i) {
    std::uint32_t klen = 0;
    if (!sed::read_u32_le(p, end, &klen)) {
      if (error_out) *error_out = "wal replay batch: del klen";
      return false;
    }
    if (static_cast<std::size_t>(end - p) < klen) {
      if (error_out) *error_out = "wal replay batch: del key truncated";
      return false;
    }
    del_keys.emplace_back(reinterpret_cast<const char*>(p), klen);
    p += klen;
  }
  std::uint32_t nputs = 0;
  if (!sed::read_u32_le(p, end, &nputs)) {
    if (error_out) *error_out = "wal replay batch: nputs";
    return false;
  }
  if (nputs > kMaxOps) {
    if (error_out) *error_out = "wal replay batch: nputs too large";
    return false;
  }
  std::vector<std::pair<std::string, std::string>> put_rows;
  put_rows.reserve(static_cast<std::size_t>(nputs));
  for (std::uint32_t i = 0; i < nputs; ++i) {
    std::uint32_t klen = 0;
    if (!sed::read_u32_le(p, end, &klen)) {
      if (error_out) *error_out = "wal replay batch: put klen";
      return false;
    }
    if (static_cast<std::size_t>(end - p) < klen) {
      if (error_out) *error_out = "wal replay batch: put key truncated";
      return false;
    }
    std::string key(reinterpret_cast<const char*>(p), klen);
    p += klen;
    std::uint32_t vlen = 0;
    if (!sed::read_u32_le(p, end, &vlen)) {
      if (error_out) *error_out = "wal replay batch: put vlen";
      return false;
    }
    if (static_cast<std::size_t>(end - p) < vlen) {
      if (error_out) *error_out = "wal replay batch: put val truncated";
      return false;
    }
    std::string val(reinterpret_cast<const char*>(p), vlen);
    p += vlen;
    put_rows.emplace_back(std::move(key), std::move(val));
  }
  if (p != end) {
    if (error_out) *error_out = "wal replay batch: trailing garbage";
    return false;
  }
  for (const auto& dk : del_keys) {
    engine_.mem_mgr_.active().put(std::string(dk), std::string(versioned_kv::kTomb));
  }
  for (auto& pr : put_rows) {
    engine_.observe_stored_commit_seq_(pr.second);
    engine_.mem_mgr_.active().put(std::move(pr.first), std::move(pr.second));
  }
  return true;
}

}  // namespace structdb::storage
