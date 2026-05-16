#pragma once

#include <filesystem>
#include <string>

#include "structdb/infra/file_handle.hpp"

namespace structdb::storage {

class RedoLog {
 public:
  bool open(const std::filesystem::path& dir);
  void close();
  bool append(const void* data, std::size_t len, bool fsync);

 private:
  infra::FileWriter w_;
};

}  // namespace structdb::storage
