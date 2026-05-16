#include "structdb/storage/undo_log.hpp"

#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <vector>

namespace structdb::storage {

namespace {

void push_u32_le(std::vector<std::uint8_t>& out, std::uint32_t v) {
  for (int i = 0; i < 4; ++i) out.push_back(static_cast<std::uint8_t>((v >> (8 * i)) & 0xff));
}

}  // namespace

bool UndoLog::open(const std::filesystem::path& dir) {
  std::filesystem::create_directories(dir);
  path_ = dir / "undo.log";
  return w_.open(path_, true);
}

void UndoLog::close() { w_.close(); }

bool UndoLog::sync() { return w_.is_open() && w_.sync(); }

bool UndoLog::append(const void* data, std::size_t len, bool fsync) {
  if (!w_.is_open()) return false;
  std::uint32_t le = static_cast<std::uint32_t>(len);
  if (!w_.write_all(&le, sizeof(le))) return false;
  if (!w_.write_all(data, len)) return false;
  if (fsync && !w_.sync()) return false;
  return true;
}

bool UndoLog::append_versioned_prev_snapshot(std::string_view key, std::string_view prev_raw_stored, bool fsync) {
  if (!w_.is_open()) return false;
  if (key.size() > static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)()) ||
      prev_raw_stored.size() > static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)())) {
    return false;
  }
  std::vector<std::uint8_t> body;
  constexpr char kMagic[8] = {'S', 'T', 'R', 'D', 'B', 'U', 'V', '1'};
  body.reserve(8 + 4 + key.size() + 4 + prev_raw_stored.size());
  body.insert(body.end(), kMagic, kMagic + 8);
  push_u32_le(body, static_cast<std::uint32_t>(key.size()));
  body.insert(body.end(), key.begin(), key.end());
  push_u32_le(body, static_cast<std::uint32_t>(prev_raw_stored.size()));
  body.insert(body.end(), prev_raw_stored.begin(), prev_raw_stored.end());
  std::uint32_t le = static_cast<std::uint32_t>(body.size());
  if (!w_.write_all(&le, sizeof(le))) return false;
  if (!w_.write_all(body.data(), body.size())) return false;
  if (fsync && !w_.sync()) return false;
  return true;
}

bool UndoLog::truncate_to_empty(std::string* error_out) {
  w_.close();
  std::error_code ec;
  if (std::filesystem::exists(path_)) {
    std::filesystem::resize_file(path_, 0, ec);
    if (ec) {
      if (error_out) *error_out = "undo.log resize";
      if (!w_.open(path_, true) && error_out) *error_out = "undo.log reopen after resize failure";
      return false;
    }
  }
  if (!w_.open(path_, true)) {
    if (error_out) *error_out = "undo.log reopen";
    return false;
  }
  return true;
}

bool UndoLog::truncate_prefix_at_path(const std::filesystem::path& path, std::uint64_t prefix_bytes,
                                      std::string* error_out) {
  if (prefix_bytes == 0) return true;
  std::error_code ec;
  if (!std::filesystem::exists(path)) {
    return true;
  }
  const auto sz_u = std::filesystem::file_size(path, ec);
  if (ec) {
    if (error_out) *error_out = "undo.log file_size";
    return false;
  }
  const std::uint64_t sz = static_cast<std::uint64_t>(sz_u);
  if (prefix_bytes > sz) {
    if (error_out) *error_out = "undo.log truncate_prefix past EOF";
    return false;
  }
  if (prefix_bytes == sz) {
    std::filesystem::resize_file(path, 0, ec);
    if (ec) {
      if (error_out) *error_out = "undo.log resize";
      return false;
    }
    return true;
  }
  infra::FileReader r(path);
  if (!r.is_open()) {
    if (error_out) *error_out = "undo.log truncate_prefix read open";
    return false;
  }
  std::vector<std::uint8_t> buf;
  if (!r.read_all(buf)) {
    if (error_out) *error_out = "undo.log truncate_prefix read";
    return false;
  }
  const std::filesystem::path tmp = path.string() + ".tmp";
  {
    infra::FileWriter w(tmp, false);
    if (!w.is_open()) {
      if (error_out) *error_out = "undo.log truncate_prefix tmp open";
      return false;
    }
    const std::uint8_t* tail = buf.data() + static_cast<std::size_t>(prefix_bytes);
    const std::size_t tail_len = buf.size() - static_cast<std::size_t>(prefix_bytes);
    if (tail_len > 0 && !w.write_all(tail, tail_len)) {
      std::filesystem::remove(tmp, ec);
      if (error_out) *error_out = "undo.log truncate_prefix tmp write";
      return false;
    }
    if (!w.sync()) {
      std::filesystem::remove(tmp, ec);
      if (error_out) *error_out = "undo.log truncate_prefix tmp sync";
      return false;
    }
  }
  std::filesystem::remove(path, ec);
  std::filesystem::rename(tmp, path, ec);
  if (ec) {
    if (error_out) *error_out = "undo.log truncate_prefix rename";
    return false;
  }
  return true;
}

bool UndoLog::truncate_prefix(std::uint64_t prefix_bytes, std::string* error_out) {
  w_.close();
  if (!truncate_prefix_at_path(path_, prefix_bytes, error_out)) {
    (void)w_.open(path_, true);
    return false;
  }
  if (!w_.open(path_, true)) {
    if (error_out) *error_out = "undo.log reopen";
    return false;
  }
  return true;
}

}  // namespace structdb::storage
