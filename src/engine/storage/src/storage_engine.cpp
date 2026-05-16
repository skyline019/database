#include "structdb/storage/storage_engine.hpp"

#include "structdb/infra/file_handle.hpp"

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <fstream>
#include <shared_mutex>
#include <string_view>

namespace structdb::storage {

std::uint64_t StorageEngine::versioned_read_seq_latest() { return versioned_kv::read_seq_latest(); }

bool StorageEngine::load_commit_seq_hw_(std::string* error_out) {
  const auto p = dir_ / "COMMIT_SEQ";
  if (!std::filesystem::exists(p)) {
    commit_seq_hw_.store(0, std::memory_order_relaxed);
    return true;
  }
  std::ifstream in(p);
  if (!in) {
    if (error_out) *error_out = "COMMIT_SEQ read";
    return false;
  }
  std::string line;
  if (!std::getline(in, line)) {
    if (error_out) *error_out = "COMMIT_SEQ empty";
    return false;
  }
  if (!line.empty() && line.back() == '\r') line.pop_back();
  std::uint64_t v = 0;
  try {
    v = static_cast<std::uint64_t>(std::stoull(line));
  } catch (...) {
    if (error_out) *error_out = "COMMIT_SEQ parse";
    return false;
  }
  commit_seq_hw_.store(v, std::memory_order_relaxed);
  return true;
}

bool StorageEngine::persist_commit_seq_hw_(std::string* error_out) {
  const auto tmp = dir_ / "COMMIT_SEQ.tmp";
  const auto dst = dir_ / "COMMIT_SEQ";
  std::ofstream out(tmp, std::ios::trunc | std::ios::binary);
  if (!out) {
    if (error_out) *error_out = "COMMIT_SEQ write";
    return false;
  }
  out << commit_seq_hw_.load(std::memory_order_relaxed) << '\n';
  out.close();
  std::error_code ec;
  std::filesystem::rename(tmp, dst, ec);
  if (ec) {
    if (error_out) *error_out = "COMMIT_SEQ rename";
    return false;
  }
  return true;
}

void StorageEngine::observe_stored_commit_seq_(std::string_view stored) {
  std::string body;
  std::uint64_t seq = 0;
  if (!versioned_kv::unwrap_visible(stored, versioned_kv::read_seq_latest(), &body, &seq)) return;
  if (seq == 0) return;
  for (;;) {
    std::uint64_t cur = commit_seq_hw_.load(std::memory_order_relaxed);
    if (seq <= cur) return;
    if (commit_seq_hw_.compare_exchange_weak(cur, seq, std::memory_order_relaxed)) return;
  }
}

std::uint64_t StorageEngine::reserve_commit_seq() {
  std::lock_guard<std::shared_mutex> lk(mu_);
  return commit_seq_hw_.fetch_add(1, std::memory_order_relaxed) + 1;
}

}  // namespace structdb::storage
