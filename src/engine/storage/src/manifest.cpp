#include "structdb/storage/manifest.hpp"

#include <sstream>
#include <string>

namespace structdb::storage {

namespace {

constexpr const char* kManifestFormat2 = "FORMAT2";

void strip_cr(std::string* s) {
  if (!s->empty() && s->back() == '\r') s->pop_back();
}

}  // namespace

void Manifest::push_l0_sst(std::string relative_path) {
  std::size_t insert_at = sst_entries_.size();
  for (std::size_t i = 0; i < sst_entries_.size(); ++i) {
    if (sst_entries_[i].level != 0) {
      insert_at = i;
      break;
    }
  }
  sst_entries_.insert(sst_entries_.begin() + static_cast<std::ptrdiff_t>(insert_at),
                      ManifestSst{0, std::move(relative_path)});
}

std::vector<std::string> Manifest::sst_files() const {
  std::vector<std::string> out;
  out.reserve(sst_entries_.size());
  for (const auto& e : sst_entries_) out.push_back(e.relative_path);
  return out;
}

std::size_t Manifest::l0_prefix_length() const {
  std::size_t n = 0;
  for (const auto& e : sst_entries_) {
    if (e.level != 0) break;
    ++n;
  }
  return n;
}

bool Manifest::load(const std::filesystem::path& path) {
  infra::FileReader r(path);
  if (!r.is_open()) return false;
  std::vector<std::uint8_t> buf;
  if (!r.read_all(buf)) return false;
  std::string text(reinterpret_cast<const char*>(buf.data()), buf.size());
  std::istringstream in(text);
  std::string line;
  if (!std::getline(in, line)) return false;
  strip_cr(&line);
  version_ = static_cast<std::uint64_t>(std::stoull(line));
  if (!std::getline(in, line)) return false;
  strip_cr(&line);
  sst_entries_.clear();
  if (line == kManifestFormat2) {
    if (!std::getline(in, line)) return false;
    strip_cr(&line);
    const auto n = static_cast<std::size_t>(std::stoull(line));
    sst_entries_.reserve(n);
    int prev_level = -1;
    for (std::size_t i = 0; i < n; ++i) {
      if (!std::getline(in, line)) return false;
      strip_cr(&line);
      const auto sp = line.find(' ');
      if (sp == std::string::npos || sp == 0) return false;
      int lvl = 0;
      try {
        lvl = std::stoi(line.substr(0, sp));
      } catch (...) {
        return false;
      }
      if (lvl < 0 || lvl > 255) return false;
      if (lvl < prev_level) return false;
      prev_level = lvl;
      std::size_t path_start = sp + 1;
      while (path_start < line.size() && line[path_start] == ' ') ++path_start;
      if (path_start >= line.size()) return false;
      sst_entries_.push_back(ManifestSst{static_cast<std::uint8_t>(lvl), line.substr(path_start)});
    }
    return true;
  }
  const auto n = static_cast<std::size_t>(std::stoull(line));
  sst_entries_.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    if (!std::getline(in, line)) return false;
    strip_cr(&line);
    sst_entries_.push_back(ManifestSst{0, std::move(line)});
  }
  return true;
}

bool Manifest::save(const std::filesystem::path& path) const {
  std::ostringstream out;
  out << version_ << "\n";
  out << kManifestFormat2 << "\n";
  out << sst_entries_.size() << "\n";
  for (const auto& e : sst_entries_) {
    out << static_cast<int>(e.level) << ' ' << e.relative_path << "\n";
  }
  const auto str = out.str();
  infra::FileWriter w(path, false);
  if (!w.is_open()) return false;
  if (!w.write_all(str.data(), str.size())) return false;
  return w.sync();
}

}  // namespace structdb::storage
