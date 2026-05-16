#include "structdb/storage/redo_log.hpp"

namespace structdb::storage {

bool RedoLog::open(const std::filesystem::path& dir) {
  std::filesystem::create_directories(dir);
  return w_.open(dir / "redo.log", true);
}

void RedoLog::close() { w_.close(); }

bool RedoLog::append(const void* data, std::size_t len, bool fsync) {
  if (!w_.is_open()) return false;
  std::uint32_t le = static_cast<std::uint32_t>(len);
  if (!w_.write_all(&le, sizeof(le))) return false;
  if (!w_.write_all(data, len)) return false;
  if (fsync && !w_.sync()) return false;
  return true;
}

}  // namespace structdb::storage
