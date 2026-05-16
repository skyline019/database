#include "structdb/storage/checkpoint.hpp"

#include <array>
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include <crc32c/crc32c.h>

#include "structdb/infra/file_handle.hpp"

namespace structdb::storage {

namespace {

constexpr std::uint32_t kCheckpointFormatV1 = 1u;
constexpr std::uint32_t kCheckpointFormatV2 = 2u;
constexpr std::size_t kSlotBytesV1 = 60;
constexpr std::size_t kSlotBytesV2 = 68;
constexpr char kMagic[4] = {'S', 'T', 'C', 'K'};

void put_u32_le(std::uint8_t* p, std::uint32_t v) {
  p[0] = static_cast<std::uint8_t>(v & 0xffu);
  p[1] = static_cast<std::uint8_t>((v >> 8) & 0xffu);
  p[2] = static_cast<std::uint8_t>((v >> 16) & 0xffu);
  p[3] = static_cast<std::uint8_t>((v >> 24) & 0xffu);
}

void put_u64_le(std::uint8_t* p, std::uint64_t v) {
  for (int i = 0; i < 8; ++i) p[i] = static_cast<std::uint8_t>((v >> (8 * i)) & 0xffu);
}

std::uint32_t read_u32_le(const std::uint8_t* p) {
  return static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8) |
         (static_cast<std::uint32_t>(p[2]) << 16) | (static_cast<std::uint32_t>(p[3]) << 24);
}

std::uint64_t read_u64_le(const std::uint8_t* p) {
  std::uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<std::uint64_t>(p[i]) << (8 * i);
  return v;
}

bool decode_slot_v1(const std::vector<std::uint8_t>& buf, CheckpointState* out) {
  if (buf.size() < kSlotBytesV1) return false;
  const std::uint8_t* d = buf.data();
  if (std::memcmp(d, kMagic, 4) != 0) return false;
  if (read_u32_le(d + 4) != kCheckpointFormatV1) return false;
  const std::uint32_t crc_stored = read_u32_le(d + 56);
  const std::uint32_t crc_calc = crc32c::Crc32c(d, 56);
  if (crc_calc != crc_stored) return false;
  if (!out) return true;
  out->checkpoint_seq = read_u64_le(d + 8);
  /* written_unix_ns at 16 — not stored in CheckpointState */
  out->wal_offset = read_u64_le(d + 24);
  out->redo_offset = read_u64_le(d + 32);
  out->manifest_version = read_u64_le(d + 40);
  out->mdb_catalog_epoch = read_u64_le(d + 48);
  out->undo_log_safe_prefix_bytes = 0;
  return true;
}

bool decode_slot_v2(const std::vector<std::uint8_t>& buf, CheckpointState* out) {
  if (buf.size() < kSlotBytesV2) return false;
  const std::uint8_t* d = buf.data();
  if (std::memcmp(d, kMagic, 4) != 0) return false;
  if (read_u32_le(d + 4) != kCheckpointFormatV2) return false;
  const std::uint32_t crc_stored = read_u32_le(d + 64);
  const std::uint32_t crc_calc = crc32c::Crc32c(d, 64);
  if (crc_calc != crc_stored) return false;
  if (!out) return true;
  out->checkpoint_seq = read_u64_le(d + 8);
  out->wal_offset = read_u64_le(d + 24);
  out->redo_offset = read_u64_le(d + 32);
  out->manifest_version = read_u64_le(d + 40);
  out->mdb_catalog_epoch = read_u64_le(d + 48);
  out->undo_log_safe_prefix_bytes = read_u64_le(d + 56);
  return true;
}

bool decode_slot_any(const std::vector<std::uint8_t>& buf, CheckpointState* out) {
  if (buf.size() < 8) return false;
  const std::uint8_t* d = buf.data();
  if (std::memcmp(d, kMagic, 4) != 0) return false;
  const std::uint32_t ver = read_u32_le(d + 4);
  if (ver == kCheckpointFormatV2) return decode_slot_v2(buf, out);
  if (ver == kCheckpointFormatV1) return decode_slot_v1(buf, out);
  return false;
}

void encode_slot_v2(const CheckpointState& st, std::uint64_t written_unix_ns, std::array<std::uint8_t, kSlotBytesV2>* out) {
  std::uint8_t* d = out->data();
  std::memcpy(d, kMagic, 4);
  put_u32_le(d + 4, kCheckpointFormatV2);
  put_u64_le(d + 8, st.checkpoint_seq);
  put_u64_le(d + 16, written_unix_ns);
  put_u64_le(d + 24, st.wal_offset);
  put_u64_le(d + 32, st.redo_offset);
  put_u64_le(d + 40, st.manifest_version);
  put_u64_le(d + 48, st.mdb_catalog_epoch);
  put_u64_le(d + 56, st.undo_log_safe_prefix_bytes);
  const std::uint32_t crc = crc32c::Crc32c(d, 64);
  put_u32_le(d + 64, crc);
}

bool read_file_all(const std::filesystem::path& path, std::vector<std::uint8_t>* buf) {
  infra::FileReader r(path);
  if (!r.is_open()) return false;
  return r.read_all(*buf);
}

bool read_legacy_text(const std::filesystem::path& dir, CheckpointState* out) {
  infra::FileReader r(dir / "checkpoint");
  if (!r.is_open()) return false;
  std::vector<std::uint8_t> buf;
  if (!r.read_all(buf)) return false;
  std::string text(reinterpret_cast<const char*>(buf.data()), buf.size());
  std::istringstream in(text);
  CheckpointState st{};
  in >> st.wal_offset >> st.redo_offset >> st.manifest_version;
  if (!in) return false;
  if (!(in >> st.mdb_catalog_epoch)) {
    st.mdb_catalog_epoch = 0;
  }
  st.checkpoint_seq = 0;
  st.undo_log_safe_prefix_bytes = 0;
  if (out) *out = st;
  return true;
}

char read_active_letter(const std::filesystem::path& dir) {
  infra::FileReader r(dir / "checkpoint.active");
  if (!r.is_open()) return 0;
  std::vector<std::uint8_t> buf;
  if (!r.read_all(buf) || buf.empty()) return 0;
  char c = static_cast<char>(buf[0]);
  if (c != 'a' && c != 'b') return 0;
  return c;
}

bool try_slot(const std::filesystem::path& path, CheckpointState* st) {
  if (!std::filesystem::exists(path)) return false;
  std::vector<std::uint8_t> buf;
  if (!read_file_all(path, &buf)) return false;
  return decode_slot_any(buf, st);
}

bool any_checkpoint_files_exist(const std::filesystem::path& dir) {
  return std::filesystem::exists(dir / "checkpoint") || std::filesystem::exists(dir / "checkpoint.a") ||
         std::filesystem::exists(dir / "checkpoint.b") || std::filesystem::exists(dir / "checkpoint.active");
}

}  // namespace

bool CheckpointWriter::write(const std::filesystem::path& dir, const CheckpointState& st) {
  std::filesystem::create_directories(dir);
  std::ostringstream out;
  out << st.wal_offset << " " << st.redo_offset << " " << st.manifest_version << " " << st.mdb_catalog_epoch << "\n";
  const auto s = out.str();
  infra::FileWriter w(dir / "checkpoint", false);
  if (!w.is_open()) return false;
  return w.write_all(s.data(), s.size()) && w.sync();
}

bool CheckpointWriter::read(const std::filesystem::path& dir, CheckpointState* out) {
  infra::FileReader r(dir / "checkpoint");
  if (!r.is_open()) return false;
  std::vector<std::uint8_t> buf;
  if (!r.read_all(buf)) return false;
  std::string text(reinterpret_cast<const char*>(buf.data()), buf.size());
  std::istringstream in(text);
  CheckpointState st{};
  in >> st.wal_offset >> st.redo_offset >> st.manifest_version;
  if (!in) return false;
  if (!(in >> st.mdb_catalog_epoch)) {
    st.mdb_catalog_epoch = 0;
  }
  st.checkpoint_seq = 0;
  st.undo_log_safe_prefix_bytes = 0;
  if (out) *out = st;
  return true;
}

bool CheckpointWriter::read_latest(const std::filesystem::path& dir, CheckpointState* out, std::string* error_out) {
  if (!out) {
    if (error_out) *error_out = "null out";
    return false;
  }
  if (!any_checkpoint_files_exist(dir)) {
    *out = CheckpointState{};
    return true;
  }

  CheckpointState sa{};
  CheckpointState sb{};
  const bool valid_a = try_slot(dir / "checkpoint.a", &sa);
  const bool valid_b = try_slot(dir / "checkpoint.b", &sb);
  const char active = read_active_letter(dir);

  if (active == 'a' && valid_a) {
    *out = sa;
    return true;
  }
  if (active == 'b' && valid_b) {
    *out = sb;
    return true;
  }
  if (valid_a && valid_b) {
    *out = sa.checkpoint_seq >= sb.checkpoint_seq ? sa : sb;
    return true;
  }
  if (valid_a) {
    *out = sa;
    return true;
  }
  if (valid_b) {
    *out = sb;
    return true;
  }

  if (read_legacy_text(dir, out)) {
    return true;
  }

  if (error_out) *error_out = "checkpoint: no valid slot or legacy record";
  return false;
}

bool CheckpointWriter::write_rotating(const std::filesystem::path& dir, const CheckpointState& st, std::string* error_out) {
  CheckpointState cur{};
  if (!read_latest(dir, &cur, error_out)) {
    return false;
  }
  const char active = read_active_letter(dir);
  const char inactive = (active == 'a') ? 'b' : 'a';
  const std::filesystem::path inactive_path = (inactive == 'a') ? (dir / "checkpoint.a") : (dir / "checkpoint.b");

  CheckpointState to_write = st;
  to_write.checkpoint_seq = cur.checkpoint_seq + 1;
  if (to_write.checkpoint_seq == 0) {
    if (error_out) *error_out = "checkpoint: seq overflow";
    return false;
  }

  const auto now_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count());

  std::array<std::uint8_t, kSlotBytesV2> slot{};
  encode_slot_v2(to_write, now_ns, &slot);

  {
    infra::FileWriter w(inactive_path, false);
    if (!w.is_open()) {
      if (error_out) *error_out = "checkpoint: slot open";
      return false;
    }
    if (!w.write_all(slot.data(), slot.size()) || !w.sync()) {
      if (error_out) *error_out = "checkpoint: slot write";
      return false;
    }
  }

  {
    const std::string active_line(1, inactive);
    infra::FileWriter w(dir / "checkpoint.active", false);
    if (!w.is_open()) {
      if (error_out) *error_out = "checkpoint: active open";
      return false;
    }
    if (!w.write_all(active_line.data(), active_line.size()) || !w.write_all("\n", 1) || !w.sync()) {
      if (error_out) *error_out = "checkpoint: active write";
      return false;
    }
  }

  if (!write(dir, to_write)) {
    if (error_out) *error_out = "checkpoint: legacy write";
    return false;
  }
  return true;
}

}  // namespace structdb::storage
