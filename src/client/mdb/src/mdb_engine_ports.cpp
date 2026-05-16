#include "structdb/client/detail/mdb_engine_ports.hpp"

#include "structdb/client/embed_client.hpp"
#include "structdb/facade/engine.hpp"

namespace structdb::client::mdb {

bool MdbEnginePorts::kv_get(std::string_view key, std::string* val, std::uint64_t read_max_seq) const {
  return engine->kv_get(std::string(key), val, read_max_seq);
}

void MdbEnginePorts::kv_visit_prefix(std::string_view prefix,
                                     const std::function<bool(std::string_view, std::string_view)>& visitor,
                                     std::uint64_t read_max_seq) const {
  engine->kv_visit_prefix(prefix, visitor, read_max_seq);
}

bool MdbEnginePorts::submit(const structdb::client::CommandBatch& batch, bool fsync_journal, std::string* err) const {
  return client->submit(batch, fsync_journal, err);
}

}  // namespace structdb::client::mdb
