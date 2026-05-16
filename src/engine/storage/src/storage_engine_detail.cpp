#include "storage_engine_detail.hpp"

#include "structdb/infra/file_handle.hpp"

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <utility>
#include <string>
#include <string_view>
#include <vector>

namespace structdb::storage::storage_engine_detail {

void append_u32_le(std::string* s, std::uint32_t v) {
  for (int i = 0; i < 4; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

bool read_u32_le(const std::uint8_t*& p, const std::uint8_t* end, std::uint32_t* out) {
  if (static_cast<std::size_t>(end - p) < 4) return false;
  std::uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<std::uint32_t>(*p++) << (8 * i);
  *out = v;
  return true;
}

namespace {

constexpr unsigned char kSstMagicV2[8] = {'S', 'T', 'D', 'B', 'S', 'S', 'T', '2'};
constexpr unsigned char kSstMagicV3[8] = {'S', 'T', 'D', 'B', 'S', 'S', 'T', '3'};
constexpr std::size_t kSstBloomBytes = 64;

bool read_u64_le(const std::uint8_t*& p, const std::uint8_t* end, std::uint64_t* out) {
  if (static_cast<std::size_t>(end - p) < 8) return false;
  std::uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<std::uint64_t>(*p++) << (8 * i);
  *out = v;
  return true;
}

std::uint64_t sst_fnv1a64(std::string_view s) {
  std::uint64_t h = 14695981039346656037ull;
  for (unsigned char c : s) {
    h ^= static_cast<std::uint64_t>(c);
    h *= 1099511628211ull;
  }
  return h;
}

std::uint64_t sst_mix64_key(std::string_view s) {
  std::uint64_t h = 0xcbf29ce484222325ull;
  for (std::size_t i = 0; i < s.size(); ++i) {
    h ^= static_cast<std::uint64_t>(static_cast<unsigned char>(s[i]));
    h *= 0x100000001B3ull;
    h = (h << 17) | (h >> 47);
  }
  return h | 1ull;
}

void sst_bloom_add_key(std::array<std::uint8_t, kSstBloomBytes>* out, std::string_view key) {
  const std::uint64_t h1 = sst_fnv1a64(key);
  const std::uint64_t h2 = sst_mix64_key(key);
  for (int i = 0; i < 8; ++i) {
    const std::uint64_t bit = (h1 + static_cast<std::uint64_t>(i) * h2) % (kSstBloomBytes * 8);
    (*out)[static_cast<std::size_t>(bit / 8)] |= static_cast<std::uint8_t>(1u << (bit % 8));
  }
}

bool sst_bloom_might_contain(const std::array<std::uint8_t, kSstBloomBytes>& bits, std::string_view key) {
  bool any = false;
  for (std::size_t i = 0; i < kSstBloomBytes; ++i) {
    if (bits[i]) {
      any = true;
      break;
    }
  }
  if (!any) return true;
  const std::uint64_t h1 = sst_fnv1a64(key);
  const std::uint64_t h2 = sst_mix64_key(key);
  for (int i = 0; i < 8; ++i) {
    const std::uint64_t bit = (h1 + static_cast<std::uint64_t>(i) * h2) % (kSstBloomBytes * 8);
    if ((bits[static_cast<std::size_t>(bit / 8)] & static_cast<std::uint8_t>(1u << (bit % 8))) == 0) return false;
  }
  return true;
}

/// SST v2/v3: magic (8) + body_len (u64 LE) + body + footer: minklen + min + maxklen + max [+ v3: bloom_len u32 + 64B bloom].
/// Legacy: entire file is the kv stream (no magic).
struct SstBodySlice {
  const std::uint8_t* begin{nullptr};
  const std::uint8_t* end{nullptr};
  std::string_view min_key;
  std::string_view max_key;
  bool v2_meta_valid{false};
  bool bloom_valid{false};
  std::array<std::uint8_t, kSstBloomBytes> bloom{};
};

bool parse_sst_v2_footer_after_body(const std::uint8_t* body_start, std::uint64_t body_len, const std::uint8_t* file_end,
                                    SstBodySlice* out) {
  const std::uint8_t* foot = body_start + static_cast<std::size_t>(body_len);
  if (foot > file_end) return false;
  const std::uint8_t* q = foot;
  std::uint32_t mink = 0;
  std::uint32_t maxk = 0;
  if (!read_u32_le(q, file_end, &mink)) return false;
  if (static_cast<std::size_t>(file_end - q) < mink) return false;
  const std::string_view mk(reinterpret_cast<const char*>(q), mink);
  q += mink;
  if (!read_u32_le(q, file_end, &maxk)) return false;
  if (static_cast<std::size_t>(file_end - q) < maxk) return false;
  const std::string_view mx(reinterpret_cast<const char*>(q), maxk);
  q += maxk;
  out->begin = body_start;
  out->end = foot;
  out->min_key = mk;
  out->max_key = mx;
  out->v2_meta_valid = true;
  out->bloom_valid = false;
  out->bloom.fill(0);
  if (q == file_end) return true;
  std::uint32_t blen = 0;
  if (!read_u32_le(q, file_end, &blen)) return false;
  if (blen != static_cast<std::uint32_t>(kSstBloomBytes)) return false;
  if (static_cast<std::size_t>(file_end - q) < kSstBloomBytes) return false;
  std::memcpy(out->bloom.data(), q, kSstBloomBytes);
  q += kSstBloomBytes;
  if (q != file_end) return false;
  out->bloom_valid = true;
  return true;
}

/// Returns false if buffer looks like v2/v3 but is truncated/corrupt (callers treat as read failure).
bool slice_sst_body_or_false_on_corrupt(const std::vector<std::uint8_t>& buf, SstBodySlice* out) {
  out->begin = buf.data();
  out->end = buf.data() + buf.size();
  out->min_key = {};
  out->max_key = {};
  out->v2_meta_valid = false;
  out->bloom_valid = false;
  out->bloom.fill(0);
  if (buf.size() >= 24) {
    const bool is_v3 = std::memcmp(buf.data(), kSstMagicV3, sizeof(kSstMagicV3)) == 0;
    const bool is_v2 = !is_v3 && std::memcmp(buf.data(), kSstMagicV2, sizeof(kSstMagicV2)) == 0;
    if (is_v2 || is_v3) {
      const std::uint8_t* p = buf.data() + 8;
      const std::uint8_t* file_end = buf.data() + buf.size();
      std::uint64_t body_len = 0;
      if (!read_u64_le(p, file_end, &body_len)) return false;
      if (body_len > buf.size() - 16) return false;
      const std::uint8_t* body_start = buf.data() + 16;
      if (is_v3) {
        if (!parse_sst_v2_footer_after_body(body_start, body_len, file_end, out)) return false;
        if (!out->bloom_valid) return false;
        return true;
      }
      const std::uint8_t* foot = body_start + static_cast<std::size_t>(body_len);
      if (foot > file_end) return false;
      const std::uint8_t* q = foot;
      std::uint32_t mink = 0;
      std::uint32_t maxk = 0;
      if (!read_u32_le(q, file_end, &mink)) return false;
      if (static_cast<std::size_t>(file_end - q) < mink) return false;
      const std::string_view mk(reinterpret_cast<const char*>(q), mink);
      q += mink;
      if (!read_u32_le(q, file_end, &maxk)) return false;
      if (static_cast<std::size_t>(file_end - q) < maxk) return false;
      const std::string_view mx(reinterpret_cast<const char*>(q), maxk);
      q += maxk;
      if (q != file_end) return false;
      out->begin = body_start;
      out->end = foot;
      out->min_key = mk;
      out->max_key = mx;
      out->v2_meta_valid = true;
      return true;
    }
  }
  return true;
}

bool scan_body_for_key(const std::uint8_t* p, const std::uint8_t* end, const std::string& key, std::string* value_out) {
  while (p < end) {
    std::uint32_t klen = 0;
    std::uint32_t vlen = 0;
    if (!read_u32_le(p, end, &klen)) return false;
    if (static_cast<std::size_t>(end - p) < klen) return false;
    const std::string k(reinterpret_cast<const char*>(p), klen);
    p += klen;
    if (!read_u32_le(p, end, &vlen)) return false;
    if (static_cast<std::size_t>(end - p) < vlen) return false;
    if (k == key) {
      if (value_out) value_out->assign(reinterpret_cast<const char*>(p), vlen);
      return true;
    }
    p += vlen;
  }
  return false;
}

bool scan_body_for_prefix(const std::uint8_t* p, const std::uint8_t* end, std::string_view prefix,
                          const std::function<bool(std::string_view, std::string_view)>& visitor) {
  while (p < end) {
    std::uint32_t klen = 0;
    std::uint32_t vlen = 0;
    if (!read_u32_le(p, end, &klen)) return false;
    if (static_cast<std::size_t>(end - p) < klen) return false;
    const std::string_view k(reinterpret_cast<const char*>(p), klen);
    p += klen;
    if (!read_u32_le(p, end, &vlen)) return false;
    if (static_cast<std::size_t>(end - p) < vlen) return false;
    const std::string_view v(reinterpret_cast<const char*>(p), vlen);
    p += vlen;
    if (k.size() >= prefix.size() && k.compare(0, prefix.size(), prefix) == 0) {
      if (!visitor(k, v)) return false;
    }
  }
  return true;
}

bool scan_body_load_all(const std::uint8_t* p, const std::uint8_t* end, std::vector<std::pair<std::string, std::string>>* out,
                        std::string* error_out) {
  while (p < end) {
    std::uint32_t klen = 0;
    std::uint32_t vlen = 0;
    if (!read_u32_le(p, end, &klen)) {
      if (error_out) *error_out = "sst parse klen";
      return false;
    }
    if (static_cast<std::size_t>(end - p) < klen) {
      if (error_out) *error_out = "sst parse key";
      return false;
    }
    std::string k(reinterpret_cast<const char*>(p), klen);
    p += klen;
    if (!read_u32_le(p, end, &vlen)) {
      if (error_out) *error_out = "sst parse vlen";
      return false;
    }
    if (static_cast<std::size_t>(end - p) < vlen) {
      if (error_out) *error_out = "sst parse value";
      return false;
    }
    std::string v(reinterpret_cast<const char*>(p), vlen);
    p += vlen;
    out->emplace_back(std::move(k), std::move(v));
  }
  return true;
}

}  // namespace

bool sst_get_key(const std::filesystem::path& path, const std::string& key, std::string* value_out) {
  infra::FileReader r(path);
  if (!r.is_open()) return false;
  std::vector<std::uint8_t> buf;
  if (!r.read_all(buf)) return false;
  SstBodySlice sl{};
  if (!slice_sst_body_or_false_on_corrupt(buf, &sl)) return false;
  if (sl.v2_meta_valid && sl.bloom_valid && !sst_bloom_might_contain(sl.bloom, key)) return false;
  if (sl.v2_meta_valid) {
    if (!sl.max_key.empty() && key > sl.max_key) return false;
    if (!sl.min_key.empty() && key < sl.min_key) return false;
  }
  return scan_body_for_key(sl.begin, sl.end, key, value_out);
}

bool sst_visit_prefix(const std::filesystem::path& path, std::string_view prefix,
                      const std::function<bool(std::string_view, std::string_view)>& visitor) {
  infra::FileReader r(path);
  if (!r.is_open()) return false;
  std::vector<std::uint8_t> buf;
  if (!r.read_all(buf)) return false;
  SstBodySlice sl{};
  if (!slice_sst_body_or_false_on_corrupt(buf, &sl)) return false;
  if (sl.v2_meta_valid && !sl.max_key.empty()) {
    if (sl.max_key < prefix) return true;
  }
  return scan_body_for_prefix(sl.begin, sl.end, prefix, visitor);
}

bool sst_load_all_entries(const std::filesystem::path& path, std::vector<std::pair<std::string, std::string>>* out,
                          std::string* error_out, bool sequential_scan_hint, std::size_t read_chunk_bytes,
                          const std::function<void(std::size_t)>& on_read_progress) {
  out->clear();
  infra::FileReader r;
  if (!r.open(path, sequential_scan_hint)) {
    if (error_out) *error_out = "sst read open";
    return false;
  }
  std::vector<std::uint8_t> buf;
  if (read_chunk_bytes > 0) {
    if (!r.read_all_chunked(buf, read_chunk_bytes, on_read_progress)) {
      if (error_out) *error_out = "sst read";
      return false;
    }
  } else {
    if (!r.read_all(buf)) {
      if (error_out) *error_out = "sst read";
      return false;
    }
  }
  SstBodySlice sl{};
  if (!slice_sst_body_or_false_on_corrupt(buf, &sl)) {
    if (error_out) *error_out = "sst header/footer";
    return false;
  }
  return scan_body_load_all(sl.begin, sl.end, out, error_out);
}

bool write_sst_sorted_entries_from_for_each(
    const std::filesystem::path& path,
    const std::function<bool(const std::function<bool(const std::string&, const std::string&)>& visitor)>& for_each_sorted,
    std::string* error_out, std::size_t write_chunk_bytes, const std::function<void(std::size_t)>& on_write_progress) {
  std::string body;
  std::string_view min_k;
  std::string_view max_k;
  bool any = false;
  std::array<std::uint8_t, kSstBloomBytes> bloom{};
  bloom.fill(0);
  if (!for_each_sorted([&](const std::string& k, const std::string& v) {
        if (!any) {
          min_k = k;
          any = true;
        }
        max_k = k;
        sst_bloom_add_key(&bloom, k);
        const std::uint32_t klen = static_cast<std::uint32_t>(k.size());
        const std::uint32_t vlen = static_cast<std::uint32_t>(v.size());
        append_u32_le(&body, klen);
        body.append(k);
        append_u32_le(&body, vlen);
        body.append(v);
        return true;
      })) {
    if (error_out) *error_out = "sst write visitor aborted";
    return false;
  }
  infra::FileWriter w(path, false);
  if (!w.is_open()) {
    if (error_out) *error_out = "sst write open";
    return false;
  }
  auto write_blob = [&](const void* data, std::size_t len) -> bool {
    if (len == 0) return true;
    if (write_chunk_bytes > 0) return w.write_all_chunked(data, len, write_chunk_bytes, on_write_progress);
    return w.write_all(data, len);
  };
  if (!write_blob(kSstMagicV3, sizeof(kSstMagicV3))) {
    if (error_out) *error_out = "sst write magic";
    return false;
  }
  const std::uint64_t blen = static_cast<std::uint64_t>(body.size());
  unsigned char blen_le[8]{};
  for (int i = 0; i < 8; ++i) blen_le[i] = static_cast<unsigned char>((blen >> (8 * i)) & 0xff);
  if (!write_blob(blen_le, sizeof(blen_le)) || !write_blob(body.data(), body.size())) {
    if (error_out) *error_out = "sst write body";
    return false;
  }
  const std::uint32_t mink = static_cast<std::uint32_t>(min_k.size());
  const std::uint32_t maxk = static_cast<std::uint32_t>(max_k.size());
  if (!write_blob(&mink, sizeof(mink)) || !write_blob(min_k.data(), min_k.size()) || !write_blob(&maxk, sizeof(maxk)) ||
      !write_blob(max_k.data(), max_k.size())) {
    if (error_out) *error_out = "sst write footer";
    return false;
  }
  const std::uint32_t bloom_len_field = static_cast<std::uint32_t>(kSstBloomBytes);
  if (!write_blob(&bloom_len_field, sizeof(bloom_len_field)) || !write_blob(bloom.data(), bloom.size())) {
    if (error_out) *error_out = "sst write bloom";
    return false;
  }
  if (!w.sync()) {
    if (error_out) *error_out = "sst sync";
    return false;
  }
  return true;
}

bool write_sst_sorted_entries(const std::filesystem::path& path, const std::map<std::string, std::string>& sorted,
                              std::string* error_out, std::size_t write_chunk_bytes,
                              const std::function<void(std::size_t)>& on_write_progress) {
  return write_sst_sorted_entries_from_for_each(
      path,
      [&sorted](const std::function<bool(const std::string&, const std::string&)>& visit) -> bool {
        for (const auto& kv : sorted) {
          if (!visit(kv.first, kv.second)) return false;
        }
        return true;
      },
      error_out,
      write_chunk_bytes,
      on_write_progress);
}

std::uint64_t file_size_u64_or_zero(const std::filesystem::path& p) {
  std::error_code ec;
  if (!std::filesystem::exists(p)) return 0;
  const auto s = std::filesystem::file_size(p, ec);
  if (ec) return 0;
  return static_cast<std::uint64_t>(s);
}

bool persist_wal_segments_metadata_v2(const std::filesystem::path& data_dir, std::uint64_t next_roll_seq,
                                        const std::vector<std::string>& sealed_relative, std::string* error_out) {
  const auto tmp = data_dir / "wal.segments.tmp";
  const auto dst = data_dir / "wal.segments";
  {
    std::ofstream out(tmp, std::ios::trunc | std::ios::binary);
    if (!out) {
      if (error_out) *error_out = "wal.segments v2 tmp open";
      return false;
    }
    out << "2\n" << next_roll_seq << "\n" << sealed_relative.size() << "\n";
    for (const auto& s : sealed_relative) out << s << "\n";
    out.flush();
    if (!out) {
      if (error_out) *error_out = "wal.segments v2 write";
      return false;
    }
  }
  std::error_code ec;
  std::filesystem::rename(tmp, dst, ec);
  if (ec) {
    if (error_out) *error_out = "wal.segments v2 rename";
    return false;
  }
  return true;
}

bool persist_undo_segments_metadata_v2(const std::filesystem::path& data_dir, std::uint64_t next_roll_seq,
                                       const std::vector<std::string>& sealed_relative, std::string* error_out) {
  const auto tmp = data_dir / "undo.segments.tmp";
  const auto dst = data_dir / "undo.segments";
  {
    std::ofstream out(tmp, std::ios::trunc | std::ios::binary);
    if (!out) {
      if (error_out) *error_out = "undo.segments v2 tmp open";
      return false;
    }
    out << "2\n" << next_roll_seq << "\n" << sealed_relative.size() << "\n";
    for (const auto& s : sealed_relative) out << s << "\n";
    out.flush();
    if (!out) {
      if (error_out) *error_out = "undo.segments v2 write";
      return false;
    }
  }
  std::error_code ec;
  std::filesystem::rename(tmp, dst, ec);
  if (ec) {
    if (error_out) *error_out = "undo.segments v2 rename";
    return false;
  }
  return true;
}

bool wal_segment_rel_path_safe(const std::string& rel) {
  if (rel.empty()) return false;
  if (rel.front() == '/' || rel.front() == '\\') return false;
  if (rel.find("..") != std::string::npos) return false;
  return true;
}

bool undo_segment_rel_path_safe(const std::string& rel) {
  if (!wal_segment_rel_path_safe(rel)) return false;
  return rel.size() >= 6 && rel.compare(0, 5, "undo/") == 0;
}

bool persist_wal_segments_metadata_v1(const std::filesystem::path& data_dir, std::uint32_t segment_count,
                                        std::string* error_out) {
  const auto tmp = data_dir / "wal.segments.tmp";
  const auto dst = data_dir / "wal.segments";
  {
    std::ofstream out(tmp, std::ios::trunc | std::ios::binary);
    if (!out) {
      if (error_out) *error_out = "wal.segments tmp open";
      return false;
    }
    out << "1\n" << segment_count << "\n";
    out.flush();
    if (!out) {
      if (error_out) *error_out = "wal.segments write";
      return false;
    }
  }
  std::error_code ec;
  std::filesystem::rename(tmp, dst, ec);
  if (ec) {
    if (error_out) *error_out = "wal.segments rename";
    return false;
  }
  return true;
}

std::uint32_t read_wal_segments_metadata_or_default(const std::filesystem::path& data_dir) {
  const auto p = data_dir / "wal.segments";
  if (!std::filesystem::exists(p)) return 1;
  std::ifstream in(p);
  if (!in) return 1;
  std::string vline, cline;
  if (!std::getline(in, vline) || !std::getline(in, cline)) return 1;
  if (!vline.empty() && vline.back() == '\r') vline.pop_back();
  if (!cline.empty() && cline.back() == '\r') cline.pop_back();
  try {
    const unsigned long ver = std::stoul(vline);
    if (ver != 1u) return 1;
    const auto c = static_cast<std::uint32_t>(std::stoul(cline));
    return c == 0 ? 1u : c;
  } catch (...) {
    return 1;
  }
}

void manifest_sst_paths_lookup_order(const Manifest& m, const std::filesystem::path& dir,
                                     std::vector<std::filesystem::path>* out) {
  out->clear();
  const auto& ent = m.sst_entries();
  std::size_t i = 0;
  while (i < ent.size()) {
    const auto lvl = ent[i].level;
    std::size_t j = i;
    while (j < ent.size() && ent[j].level == lvl) ++j;
    for (std::size_t k = j; k > i;) {
      --k;
      out->push_back(dir / ent[k].relative_path);
    }
    i = j;
  }
}

}  // namespace structdb::storage::storage_engine_detail
