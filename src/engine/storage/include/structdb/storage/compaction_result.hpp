#pragma once

#include <string>
#include <utility>

namespace structdb::storage {

/// 合并物化 / manifest 提交路径的统一返回体（逐步取代裸 `bool` + `error_out` 分散约定）。
struct CompactionResult {
  bool ok{false};
  std::string error;

  static CompactionResult success() {
    CompactionResult r;
    r.ok = true;
    return r;
  }
  static CompactionResult failure(std::string msg) {
    CompactionResult r;
    r.ok = false;
    r.error = std::move(msg);
    return r;
  }
  void copy_error_to(std::string* error_out) const {
    if (error_out && !error.empty()) *error_out = error;
  }
};

}  // namespace structdb::storage
