#include "structdb/storage/storage_engine.hpp"

#include "structdb/infra/file_handle.hpp"
#include "structdb/infra/tracer.hpp"
#include "structdb/storage/storage_trace.hpp"
#include "structdb/storage/wal.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "storage_engine_detail.hpp"

namespace structdb::storage {

namespace {
constexpr bool kAppendRedoMirrorWal = false;
constexpr std::size_t kMemtableBulkPutMinKeys = 32;

void mem_apply_puts_unlocked(IMemTable& mt, std::vector<std::pair<std::string, std::string>>& items, bool bulk) {
  if (bulk && items.size() >= kMemtableBulkPutMinKeys) {
    std::sort(items.begin(), items.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    mt.reserve_capacity(items.size());
  }
  for (auto& pr : items) mt.put(std::move(pr.first), std::move(pr.second));
}
}  // namespace

namespace sed = structdb::storage::storage_engine_detail;

bool StorageEngine::lookup_versioned_raw_for_undo_unlocked_(const std::string& key, std::string* prev_raw_out) const {
  if (!prev_raw_out) return false;
  prev_raw_out->clear();
  std::string raw;
  if (mem_layers_get_raw_unlocked_(key, &raw)) {
    if (raw != versioned_kv::kTomb) {
      *prev_raw_out = std::move(raw);
      return true;
    }
    return false;
  }
  std::vector<std::filesystem::path> paths;
  sed::manifest_sst_paths_lookup_order(manifest_, dir_, &paths);
  for (const auto& pth : paths) {
    if (!sed::sst_get_key(pth, key, &raw)) continue;
    if (raw == versioned_kv::kTomb) continue;
    *prev_raw_out = std::move(raw);
    return true;
  }
  return false;
}

bool StorageEngine::append_versioned_undo_with_prev_unlocked_(const std::string& key, const std::string& prev_raw) {
  if (!versioned_kv::key_versions_persist(key)) return true;
  const std::uint64_t frame_start = checkpoint_undo_coordinator_.undo_logical_stream_total_bytes_unlocked();
  if (!undo_.append_versioned_prev_snapshot(key, prev_raw, false)) return false;
  std::string roll_err;
  if (!checkpoint_undo_coordinator_.undo_maybe_roll_after_append_unlocked(&roll_err)) return false;
  undo_stack_.push_back(UndoStackEntry{key, prev_raw, frame_start});
  return true;
}

bool StorageEngine::append_versioned_undo_if_needed_unlocked_(const std::string& key) {
  if (!versioned_kv::key_versions_persist(key)) return true;
  std::string prev;
  if (!lookup_versioned_raw_for_undo_unlocked_(key, &prev)) {
    prev = std::string(versioned_kv::kTomb);
  }
  return append_versioned_undo_with_prev_unlocked_(key, prev);
}

std::string StorageEngine::logical_to_stored_for_put_unlocked_(const std::string& key, const std::string& logical,
                                                               std::uint64_t batch_commit_seq) {
  if (import_batch_skip_undo_ && import_store_raw_logical_) {
    if (logical == versioned_kv::kTomb) return std::string(versioned_kv::kTomb);
    return logical;
  }
  std::string to_store = logical;
  if (!versioned_kv::key_versions_persist(key)) return to_store;
  if (logical == versioned_kv::kTomb) {
    return std::string(versioned_kv::kTomb);
  }
  if (logical.size() >= versioned_kv::kVerPrefix.size() &&
      logical.compare(0, versioned_kv::kVerPrefix.size(), versioned_kv::kVerPrefix) == 0) {
    return std::string(logical);
  }
  std::uint64_t seq = batch_commit_seq;
  if (seq == 0) {
    seq = commit_seq_hw_.fetch_add(1, std::memory_order_relaxed) + 1;
  }
  return versioned_kv::wrap_payload(logical, seq);
}

bool StorageEngine::commit_embed_batch_unlocked_(const std::vector<std::string>& dels,
                                                   const std::vector<std::pair<std::string, std::string>>& puts,
                                                   bool fsync_wal, std::string* error_out) {
  (void)error_out;
  infra::SpanGuard trace_commit(trace::kCommitEmbedBatch, static_cast<std::uint64_t>(puts.size()));
  constexpr std::string_view kHdr = "STDBBW1\n";
  std::uint64_t batch_commit_seq = 0;
  const bool raw_import = import_batch_skip_undo_ && import_store_raw_logical_;
  if (!puts.empty() && !raw_import) {
    batch_commit_seq = commit_seq_hw_.fetch_add(1, std::memory_order_relaxed) + 1;
  }
  std::vector<std::pair<std::string, std::string>> stored_puts;
  const std::vector<std::pair<std::string, std::string>>* wal_puts = &puts;
  if (!raw_import) {
    stored_puts.reserve(puts.size());
    for (const auto& pr : puts) {
      stored_puts.emplace_back(pr.first, logical_to_stored_for_put_unlocked_(pr.first, pr.second, batch_commit_seq));
    }
    wal_puts = &stored_puts;
  }
  std::string body;
  body.reserve(static_cast<std::size_t>(
      estimate_commit_embed_batch_wal_frame_bytes(dels, *wal_puts)));
  body.append(kHdr.data(), kHdr.size());
  sed::append_u32_le(&body, static_cast<std::uint32_t>(dels.size()));
  for (const auto& dk : dels) {
    if (dk.size() > static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)())) return false;
    sed::append_u32_le(&body, static_cast<std::uint32_t>(dk.size()));
    body.append(dk);
  }
  sed::append_u32_le(&body, static_cast<std::uint32_t>(wal_puts->size()));
  for (const auto& pr : *wal_puts) {
    if (pr.first.size() > static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)()) ||
        pr.second.size() > static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)())) {
      return false;
    }
    sed::append_u32_le(&body, static_cast<std::uint32_t>(pr.first.size()));
    body.append(pr.first);
    sed::append_u32_le(&body, static_cast<std::uint32_t>(pr.second.size()));
    body.append(pr.second);
  }
  if (!wal_.append_record(body.data(), body.size(), fsync_wal)) return false;
  wal_coordinator_.observe_append_unlocked(fsync_wal);
  if (!wal_coordinator_.maybe_roll_after_append_unlocked()) return false;
  if (kAppendRedoMirrorWal) {
    const std::string redo_rec = std::string("PUTBATCH ") + body;
    if (!redo_.append(redo_rec.data(), redo_rec.size(), false)) return false;
  }
  std::unordered_map<std::string, std::string> undo_prev;
  if (batch_undo_lookup_ && !import_batch_skip_undo_) {
    std::unordered_set<std::string> keys;
    keys.reserve(dels.size() + stored_puts.size());
    for (const auto& dk : dels) keys.insert(dk);
    for (const auto& pr : stored_puts) keys.insert(pr.first);
    for (const auto& key : keys) {
      if (!versioned_kv::key_versions_persist(key)) continue;
      std::string prev;
      if (batch_undo_mem_only_) {
        if (!mem_layers_get_raw_unlocked_(key, &prev) || prev == versioned_kv::kTomb) {
          prev = std::string(versioned_kv::kTomb);
        }
      } else if (!lookup_versioned_raw_for_undo_unlocked_(key, &prev)) {
        prev = std::string(versioned_kv::kTomb);
      }
      undo_prev.emplace(key, std::move(prev));
    }
  }
  auto append_undo = [&](const std::string& key) -> bool {
    if (import_batch_skip_undo_) return true;
    if (!batch_undo_lookup_) return append_versioned_undo_if_needed_unlocked_(key);
    const auto it = undo_prev.find(key);
    if (it == undo_prev.end()) return append_versioned_undo_if_needed_unlocked_(key);
    return append_versioned_undo_with_prev_unlocked_(key, it->second);
  };
  for (const auto& dk : dels) {
    if (!append_undo(dk)) return false;
    mem_mgr_.active().put(dk, logical_to_stored_for_put_unlocked_(dk, std::string(versioned_kv::kTomb), 0));
  }
  const bool bulk_mem =
      (memtable_bulk_put_enabled_ || raw_import) && wal_puts->size() >= kMemtableBulkPutMinKeys;
  if (bulk_mem) {
    std::vector<std::pair<std::string, std::string>> put_items;
    put_items.reserve(wal_puts->size());
    for (const auto& pr : *wal_puts) {
      if (!append_undo(pr.first)) return false;
      if (!raw_import) observe_stored_commit_seq_(pr.second);
      put_items.emplace_back(pr.first, pr.second);
    }
    mem_apply_puts_unlocked(mem_mgr_.active(), put_items, true);
  } else {
    for (const auto& pr : *wal_puts) {
      if (!append_undo(pr.first)) return false;
      if (!raw_import) observe_stored_commit_seq_(pr.second);
      mem_mgr_.active().put(pr.first, pr.second);
    }
  }
  return true;
}

bool StorageEngine::commit_embed_batch_unlocked_split_(std::vector<std::string> dels,
                                                       std::vector<std::pair<std::string, std::string>> puts,
                                                       bool fsync_last, std::string* error_out) {
  const std::uint64_t max_bytes = embed_batch_max_frame_bytes_;
  if (max_bytes == 0 || puts.empty()) {
    return commit_embed_batch_unlocked_(dels, puts, fsync_last, error_out);
  }
  bool first = true;
  for (std::size_t start = 0; start < puts.size();) {
    std::vector<std::pair<std::string, std::string>> chunk;
    chunk.reserve(4096);
    const std::vector<std::string> empty_dels;
    while (start + chunk.size() < puts.size()) {
      chunk.push_back(puts[start + chunk.size()]);
      const std::vector<std::string>& use_dels = first ? dels : empty_dels;
      const std::uint64_t est = estimate_commit_embed_batch_wal_frame_bytes(use_dels, chunk);
      if (est > max_bytes && chunk.size() > 1) {
        chunk.pop_back();
        break;
      }
      if (est > max_bytes && chunk.size() == 1) break;
    }
    if (chunk.empty()) {
      if (error_out) *error_out = "embed batch split: empty chunk";
      return false;
    }
    const bool fsync_chunk = fsync_last && (start + chunk.size() >= puts.size());
    if (first) {
      if (!commit_embed_batch_unlocked_(dels, chunk, fsync_chunk, error_out)) return false;
      dels.clear();
      first = false;
    } else if (!commit_embed_batch_unlocked_(empty_dels, chunk, fsync_chunk, error_out)) {
      return false;
    }
    start += chunk.size();
  }
  return true;
}

bool StorageEngine::commit_embed_batch(const std::vector<std::string>& dels,
                                       const std::vector<std::pair<std::string, std::string>>& puts, bool fsync_wal,
                                       std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  if (dels.empty() && puts.empty()) return true;
  std::lock_guard<std::shared_mutex> lk(mu_);
  if (embed_batch_max_frame_bytes_ > 0) {
    const std::uint64_t est = estimate_commit_embed_batch_wal_frame_bytes(dels, puts);
    if (est > embed_batch_max_frame_bytes_) {
      return commit_embed_batch_unlocked_split_(std::vector<std::string>(dels), puts, fsync_wal, error_out);
    }
  }
  return commit_embed_batch_unlocked_(dels, puts, fsync_wal, error_out);
}

bool StorageEngine::put_impl_unlocked_(const std::string& key, const std::string& value, bool fsync_wal,
                                       std::uint64_t batch_commit_seq, bool record_versioned_undo) {
  if (record_versioned_undo) {
    if (!append_versioned_undo_if_needed_unlocked_(key)) return false;
  }
  std::string to_store = logical_to_stored_for_put_unlocked_(key, value, batch_commit_seq);
  if (versioned_kv::key_versions_persist(key) && to_store != versioned_kv::kTomb) {
    observe_stored_commit_seq_(to_store);
  }
  const std::string line = key + "=" + to_store + "\n";
  if (!wal_.append_record(line.data(), line.size(), fsync_wal)) return false;
  wal_coordinator_.observe_append_unlocked(fsync_wal);
  if (!wal_coordinator_.maybe_roll_after_append_unlocked()) return false;
  if (kAppendRedoMirrorWal) {
    const std::string redo_rec = std::string("PUT ") + line;
    if (!redo_.append(redo_rec.data(), redo_rec.size(), false)) return false;
  }
  mem_mgr_.active().put(key, std::move(to_store));
  return true;
}

bool StorageEngine::put(const std::string& key, const std::string& value, bool fsync_wal, std::uint64_t batch_commit_seq) {
  if (!opened_) return false;
  std::lock_guard<std::shared_mutex> lk(mu_);
  return put_impl_unlocked_(key, value, fsync_wal, batch_commit_seq, true);
}

bool StorageEngine::wal_sync(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::lock_guard<std::shared_mutex> lk(mu_);
  if (!wal_.sync()) {
    if (error_out) *error_out = "wal sync";
    return false;
  }
  wal_coordinator_.observe_fsync_unlocked();
  return true;
}

std::uint64_t StorageEngine::wal_log_bytes_on_disk() const {
  std::shared_lock<std::shared_mutex> lk(mu_);
  std::uint64_t t = sed::file_size_u64_or_zero(wal_.path());
  for (const auto& rel : wal_sealed_relative_) t += sed::file_size_u64_or_zero(dir_ / rel);
  return t;
}

std::uint64_t StorageEngine::undo_log_bytes_on_disk() const {
  std::shared_lock<std::shared_mutex> lk(mu_);
  std::uint64_t t = sed::file_size_u64_or_zero(dir_ / "undo.log");
  for (const auto& rel : undo_sealed_relative_) t += sed::file_size_u64_or_zero(dir_ / rel);
  return t;
}

bool StorageEngine::undo_try_truncate_when_stack_empty(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::lock_guard<std::shared_mutex> lk(mu_);
  if (!undo_stack_.empty()) {
    if (error_out) *error_out = "undo_stack not empty";
    return false;
  }
  std::error_code ec;
  for (const auto& rel : undo_sealed_relative_) {
    std::filesystem::remove(dir_ / rel, ec);
  }
  undo_sealed_relative_.clear();
  undo_catalog_v2_ = false;
  undo_next_roll_seq_ = 1;
  undo_segment_count_observed_ = 1u;
  std::filesystem::remove(dir_ / "undo.segments", ec);
  return undo_.truncate_to_empty(error_out);
}

bool StorageEngine::undo_try_truncate_recyclable_prefix(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::lock_guard<std::shared_mutex> lk(mu_);
  return checkpoint_undo_coordinator_.undo_truncate_recyclable_prefix_unlocked(error_out);
}

bool StorageEngine::read_checkpoint_state(CheckpointState* out) {
  if (!out) return false;
  std::shared_lock<std::shared_mutex> lk(mu_);
  std::string err;
  return ckpt_.read_latest(dir_, out, &err);
}

bool StorageEngine::wal_try_trim_prefix_through_checkpoint(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::lock_guard<std::shared_mutex> lk(mu_);
  return wal_coordinator_.try_trim_prefix_through_checkpoint_unlocked(error_out);
}

bool StorageEngine::rollback_one_undo_frame(std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::lock_guard<std::shared_mutex> lk(mu_);
  if (undo_stack_.empty()) return false;
  return checkpoint_undo_coordinator_.rollback_one_undo_frame_unlocked(error_out);
}

std::size_t StorageEngine::undo_stack_depth() const {
  if (!opened_) return 0;
  std::shared_lock<std::shared_mutex> lk(mu_);
  return undo_stack_.size();
}

bool StorageEngine::rollback_undo_frames_until_depth(std::size_t target_depth, std::string* error_out) {
  if (!opened_) {
    if (error_out) *error_out = "not open";
    return false;
  }
  std::lock_guard<std::shared_mutex> lk(mu_);
  while (undo_stack_.size() > target_depth) {
    if (!checkpoint_undo_coordinator_.rollback_one_undo_frame_unlocked(error_out)) return false;
  }
  return true;
}

bool StorageEngine::remove(const std::string& key, bool fsync_wal) {
  return put(key, std::string(versioned_kv::kTomb), fsync_wal);
}

std::uint64_t StorageEngine::estimate_put_wal_frame_bytes(const std::string& key, const std::string& logical_value) {
  constexpr std::uint64_t kStoredSlack = 192;
  const std::uint64_t line =
      static_cast<std::uint64_t>(key.size()) + 1 + static_cast<std::uint64_t>(logical_value.size()) + kStoredSlack + 1;
  return 4 + line;
}

std::uint64_t StorageEngine::estimate_commit_embed_batch_wal_frame_bytes(
    const std::vector<std::string>& dels, const std::vector<std::pair<std::string, std::string>>& puts) {
  constexpr std::string_view kHdr = "STDBBW1\n";
  constexpr std::size_t kMaxU32 = static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)());
  std::uint64_t body = static_cast<std::uint64_t>(kHdr.size()) + 4;
  for (const auto& dk : dels) {
    if (dk.size() > kMaxU32) return 256ull * 1024 * 1024;
    body += 4 + static_cast<std::uint64_t>(dk.size());
  }
  body += 4;
  constexpr std::uint64_t kPerPutSlack = 192;
  for (const auto& pr : puts) {
    if (pr.first.size() > kMaxU32 || pr.second.size() > kMaxU32) return 256ull * 1024 * 1024;
    body += 4 + static_cast<std::uint64_t>(pr.first.size()) + 4 + static_cast<std::uint64_t>(pr.second.size()) +
            kPerPutSlack;
  }
  return 4 + body;
}

}  // namespace structdb::storage
