#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/mdb_persistence.hpp"
#include "structdb/facade/engine.hpp"

namespace structdb::client::mdb {

bool mdb_session_bulk_import_active(const structdb::facade::Engine& engine,
                                    const std::optional<bool>& session_override, const MdbRunOptions& opt) {
  if (opt.bulk_import_mode) return true;
  if (session_override.has_value()) return *session_override;
  return engine.config().snapshot().mdb_bulk_import_mode;
}

bool mdb_session_persist_coalesce_active(const structdb::facade::Engine& engine, const MdbRunOptions& opt) {
  if (opt.persist_coalesce) return true;
  return engine.config().snapshot().mdb_persist_coalesce;
}

bool mdb_script_amortize_bulk_dml_active(const structdb::facade::Engine& engine, const MdbRunOptions& opt) {
  if (!opt.amortize_bulk_dml_in_script) return false;
  return engine.config().snapshot().mdb_script_amortize_bulk_dml;
}

bool mdb_flush_coalesced_persist(const MdbEnginePorts& ports, LogicalTable& current, const std::string& idem,
                                 bool fsync, std::string* err) {
  if (current.mdb_persist_dirty_rows.empty() && !current.mdb_persist_schema_dirty) return true;
  MdbEnginePorts p = ports;
  return persist_table(p, current, idem, fsync, err);
}

void mdb_ports_set_bulk_import_persist(MdbEnginePorts* ports, bool bulk_active) {
  if (ports) ports->skip_secondary_index_on_persist = bulk_active;
}

std::string mdb_resolve_persist_idem(const std::string& base_idem, const LogicalTable& t,
                                     const std::optional<std::string>* import_segment_token) {
  if (import_segment_token && import_segment_token->has_value() && !import_segment_token->value().empty() &&
      !t.name.empty()) {
    return "idem:import:" + t.name + ":seg:" + import_segment_token->value();
  }
  return base_idem;
}

}  // namespace structdb::client::mdb
