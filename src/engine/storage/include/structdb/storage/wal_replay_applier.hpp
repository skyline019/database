#pragma once

#include <string>
#include <string_view>

namespace structdb::storage {

class StorageEngine;

/// 在调用方已持有 `mu_` 的前提下，将 WAL 重放帧解码并写入 `mem_mgr_.active()`：文本行 `key=value\\n` 或二进制批次 `STDBBW1\\n`。
class WalReplayApplier {
 public:
  explicit WalReplayApplier(StorageEngine& engine) noexcept : engine_(engine) {}

  bool apply_line_unlocked(std::string_view line, std::string* error_out);
  bool apply_batch_unlocked(std::string_view record, std::string* error_out);

 private:
  StorageEngine& engine_;
};

}  // namespace structdb::storage
