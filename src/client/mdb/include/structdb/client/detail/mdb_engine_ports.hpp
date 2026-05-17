#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace structdb::facade {
class Engine;
}
namespace structdb::client {
class EmbedClient;
struct CommandBatch;
}  // namespace structdb::client

namespace structdb::client::mdb {

/// Narrow façade over `Engine` + `EmbedClient` for MDB storage paths (PHASE26 coupling envelope).
struct MdbEnginePorts {
  structdb::facade::Engine* engine = nullptr;
  structdb::client::EmbedClient* client = nullptr;
  /// When true, `persist_table` omits secondary-index keys (bulk import path).
  bool skip_secondary_index_on_persist{false};

  static MdbEnginePorts from(structdb::facade::Engine& e, structdb::client::EmbedClient& c) {
    return {&e, &c, false};
  }

  bool kv_get(std::string_view key, std::string* val, std::uint64_t read_max_seq) const;
  void kv_visit_prefix(std::string_view prefix,
                       const std::function<bool(std::string_view, std::string_view)>& visitor,
                       std::uint64_t read_max_seq) const;
  bool submit(const structdb::client::CommandBatch& batch, bool fsync_journal, std::string* err) const;
};

}  // namespace structdb::client::mdb
