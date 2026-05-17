#include "structdb/client/mdb_persistence.hpp"

#include "structdb/client/detail/mdb_named_index.hpp"
#include "structdb/client/mdb_logical_table.hpp"
#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/embed_client.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/storage/mdb_keyspace.hpp"
#include "structdb/storage/storage_engine.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <sstream>
#include <vector>

namespace structdb::client::mdb {

namespace {
constexpr std::size_t kMaxIncrementalDirtyRows = 8192;

std::string encode_row_tab_blob(const LogicalTable& t, const std::string& id) {
  const auto rit = t.rows.find(id);
  if (rit == t.rows.end()) return {};
  const auto& cells = rit->second;
  std::size_t cap = rit->first.size() + 1;
  for (const auto& cell : cells) cap += cell.size() + 1;
  std::string row;
  row.reserve(cap);
  row.append(rit->first);
  for (const auto& cell : cells) {
    row.push_back('\t');
    row.append(cell);
  }
  return row;
}

std::string encode_row_blob(const LogicalTable& t, const std::string& id,
                            structdb::facade::MdbWireEncoding enc) {
  return wire_encode_snapshot_blob(encode_row_tab_blob(t, id), enc);
}

void append_secidx_for_row(const LogicalTable& t, const std::string& id, const std::vector<std::string>& cells,
                           bool is_delete, structdb::client::CommandBatch* b) {
  namespace mk = structdb::storage::mdb_keyspace;
  if (is_delete) {
    for (std::size_t c = 0; c < t.schema.size() && c < cells.size(); ++c) {
      if (!is_string_type(t.schema[c].second)) continue;
      std::string ik =
          std::string(mk::kSecIdx) + t.name + '$' + t.schema[c].first + '$' + hex_encode(cells[c]) + '$' + id;
      b->dels.push_back(std::move(ik));
    }
    return;
  }
  const auto rit = t.rows.find(id);
  if (rit == t.rows.end()) return;
  for (std::size_t c = 0; c < t.schema.size() && c < rit->second.size(); ++c) {
    if (!is_string_type(t.schema[c].second)) continue;
    std::string ik = std::string(mk::kSecIdx) + t.name + '$' + t.schema[c].first + '$' +
                     hex_encode(rit->second[c]) + '$' + id;
    b->puts.push_back({std::move(ik), "1"});
  }
}

bool append_incremental_dirty(const MdbEnginePorts& /*ports*/, LogicalTable& t, const PersistBuildOptions& opt,
                              const std::unordered_set<std::string>& dirty_subset, CommandBatch* b,
                              std::string* err) {
  namespace mk = structdb::storage::mdb_keyspace;
  for (const std::string& rid : dirty_subset) {
    const auto pit = t.mdb_persist_prev_cells.find(rid);
    const auto rit = t.rows.find(rid);
    const bool deleted = rit == t.rows.end();
    if (deleted) {
      if (pit == t.mdb_persist_prev_cells.end()) {
        if (err) *err = "persist: incremental delete missing prev row";
        return false;
      }
      b->dels.push_back(mk::row_key(t.name, rid));
      if (!opt.skip_secondary_index) append_secidx_for_row(t, rid, pit->second, true, b);
    } else {
      if (pit != t.mdb_persist_prev_cells.end() && !opt.skip_secondary_index) {
        append_secidx_for_row(t, rid, pit->second, true, b);
      }
      b->puts.push_back({mk::row_key(t.name, rit->first), encode_row_blob(t, rit->first, opt.wire_encoding)});
      if (!opt.skip_secondary_index) append_secidx_for_row(t, rid, {}, false, b);
    }
  }
  const std::string idx_blob = logical_row_index_newline_blob(t);
  b->puts.push_back({std::string(mk::row_index_key(t.name)),
                     wire_encode_snapshot_blob(idx_blob, opt.wire_encoding)});
  b->puts.push_back({std::string(mk::catalog_key(t.name)), "1"});
  return true;
}

void apply_storage_persist_hints(const MdbEnginePorts& ports, const PersistBuildOptions& opt, bool large_snapshot) {
  if (!ports.engine) return;
  auto* st = ports.engine->storage();
  if (!st) return;
  const auto& cfg = ports.engine->config().snapshot();
  const bool bulk_import = opt.skip_secondary_index || cfg.mdb_bulk_import_mode;
  const bool skip_undo =
      cfg.storage_import_batch_skip_undo && (bulk_import || large_snapshot);
  const bool raw_logical =
      bulk_import && (cfg.storage_import_store_raw_logical || cfg.mdb_bulk_import_mode);
  st->set_batch_undo_lookup(cfg.storage_batch_undo_lookup);
  st->set_import_batch_skip_undo(skip_undo);
  st->set_batch_undo_mem_only(!skip_undo &&
                              (cfg.storage_batch_undo_mem_only || large_snapshot || bulk_import));
  st->set_import_store_raw_logical(raw_logical);
  st->set_memtable_bulk_put_enabled(cfg.memtable_bulk_put_enabled);
  st->set_embed_batch_max_frame_bytes(cfg.storage_embed_batch_max_frame_bytes);
}

std::vector<std::string> ordered_row_ids_for_persist(const LogicalTable& t) {
  if (!t.row_ids_ordered.empty() && t.row_ids_ordered.size() == t.rows.size()) {
    return t.row_ids_ordered;
  }
  std::vector<std::string> ids;
  ids.reserve(t.rows.size());
  for (const auto& kv : t.rows) ids.push_back(kv.first);
  if (logical_row_ids_all_int_literals(ids)) {
    logical_row_ids_sort_numeric(ids, false);
  } else {
    std::sort(ids.begin(), ids.end());
  }
  return ids;
}

void append_full_snapshot_leading_dels(const MdbEnginePorts& ports, const LogicalTable& t, const PersistBuildOptions& opt,
                                       CommandBatch* b) {
  namespace mk = structdb::storage::mdb_keyspace;
  if (t.schema.empty()) return;
  b->dels.push_back(snapshot_key_for(t.name));
  if (opt.skip_secondary_index) return;
  const std::string idx_pfx = std::string(mk::kSecIdx) + t.name + '$';
  ports.engine->kv_visit_prefix(idx_pfx, [&](std::string_view k, std::string_view v) {
    (void)v;
    b->dels.push_back(std::string(k));
    return true;
  });
}

void append_row_put(const LogicalTable& t, const std::string& rid, const PersistBuildOptions& opt, CommandBatch* b) {
  namespace mk = structdb::storage::mdb_keyspace;
  if (opt.plain_row_values) {
    b->puts.push_back({mk::row_key(t.name, rid), encode_row_tab_blob(t, rid)});
  } else {
    b->puts.push_back({mk::row_key(t.name, rid), encode_row_blob(t, rid, opt.wire_encoding)});
  }
}

void append_table_metadata_puts(const LogicalTable& t, const PersistBuildOptions& opt, CommandBatch* b) {
  namespace mk = structdb::storage::mdb_keyspace;
  if (t.schema.empty()) {
    const std::string raw = serialize_table(t);
    b->puts.push_back({snapshot_key_for(t.name), wire_encode_snapshot_blob(raw, opt.wire_encoding)});
    b->puts.push_back({std::string(mk::catalog_key(t.name)), "1"});
    return;
  }
  b->puts.push_back({std::string(mk::catalog_key(t.name)), "1"});
  std::ostringstream schema_blob;
  schema_blob << "v1\n";
  for (std::size_t i = 0; i < t.schema.size(); ++i) {
    if (i) schema_blob << ',';
    schema_blob << t.schema[i].first << ':' << t.schema[i].second;
  }
  schema_blob << "\n";
  if (!t.pk_column.empty()) schema_blob << "PKCOL\t" << t.pk_column << "\n";
  b->puts.push_back({std::string(mk::schema_key(t.name)),
                     wire_encode_snapshot_blob(schema_blob.str(), opt.wire_encoding)});
  const std::string idx_blob = logical_row_index_newline_blob(t);
  b->puts.push_back({std::string(mk::row_index_key(t.name)),
                     wire_encode_snapshot_blob(idx_blob, opt.wire_encoding)});
  if (!opt.skip_secondary_index) {
    for (const auto& kv : t.rows) {
      for (std::size_t c = 0; c < t.schema.size(); ++c) {
        if (!is_string_type(t.schema[c].second)) continue;
        std::string ik = std::string(mk::kSecIdx) + t.name + '$' + t.schema[c].first + '$' +
                         hex_encode(kv.second[c]) + '$' + kv.first;
        b->puts.push_back({std::move(ik), "1"});
      }
    }
  }
}

bool estimate_puts_exceeds_frame_limit(const structdb::facade::EngineConfigSnapshot& cfg, std::size_t put_count) {
  if (cfg.mdb_persist_chunk_max_frame_bytes == 0) return false;
  std::vector<std::string> empty_dels;
  std::vector<std::pair<std::string, std::string>> dummy_puts;
  dummy_puts.reserve((std::min)(put_count, static_cast<std::size_t>(4)));
  for (std::size_t i = 0; i < dummy_puts.capacity(); ++i) {
    dummy_puts.emplace_back("mdb$v2$row$t$0000001", std::string(64, 'x'));
  }
  const std::uint64_t est =
      structdb::storage::StorageEngine::estimate_commit_embed_batch_wal_frame_bytes(empty_dels, dummy_puts);
  if (dummy_puts.empty()) return false;
  const std::uint64_t per_put = est / dummy_puts.size();
  const std::uint64_t total_est = per_put * static_cast<std::uint64_t>(put_count);
  return total_est > cfg.mdb_persist_chunk_max_frame_bytes;
}

bool should_chunk_full_snapshot_persist(const structdb::facade::EngineConfigSnapshot& cfg, std::size_t row_count,
                                        std::size_t estimated_puts) {
  if (row_count == 0) return false;
  if (cfg.mdb_persist_chunk_max_puts > 0 && row_count > cfg.mdb_persist_chunk_max_puts) return true;
  if (cfg.mdb_persist_chunk_max_frame_bytes > 0 && estimate_puts_exceeds_frame_limit(cfg, estimated_puts)) {
    return true;
  }
  return false;
}

std::size_t estimate_full_snapshot_put_count(const LogicalTable& t, const PersistBuildOptions& opt) {
  std::size_t n = t.rows.size();
  if (t.schema.empty()) {
    n += 2;
    return n;
  }
  n += 3;
  if (!opt.skip_secondary_index) {
    for (const auto& row : t.rows) {
      for (std::size_t c = 0; c < t.schema.size() && c < row.second.size(); ++c) {
        if (is_string_type(t.schema[c].second)) ++n;
      }
    }
  }
  return n;
}

void reset_storage_persist_hints(const MdbEnginePorts& ports) {
  if (!ports.engine) return;
  auto* st = ports.engine->storage();
  if (!st) return;
  const auto& cfg = ports.engine->config().snapshot();
  st->set_batch_undo_lookup(cfg.storage_batch_undo_lookup);
  st->set_import_batch_skip_undo(false);
  st->set_batch_undo_mem_only(false);
  st->set_import_store_raw_logical(false);
  st->set_memtable_bulk_put_enabled(false);
  st->set_embed_batch_max_frame_bytes(cfg.storage_embed_batch_max_frame_bytes);
}

bool persist_table_chunked(const MdbEnginePorts& ports, const LogicalTable& t, const PersistBuildOptions& opt,
                           std::string_view idem_base, bool fsync_last, std::string* err) {
  const auto& cfg = ports.engine->config().snapshot();
  const std::vector<std::string> row_ids = ordered_row_ids_for_persist(t);
  const std::size_t chunk_max =
      cfg.mdb_persist_chunk_max_puts > 0 ? static_cast<std::size_t>(cfg.mdb_persist_chunk_max_puts) : row_ids.size();
  const std::size_t num_chunks = row_ids.empty() ? 1u : (row_ids.size() + chunk_max - 1) / chunk_max;
  for (std::size_t c = 0; c < num_chunks; ++c) {
    CommandBatch batch;
    batch.idempotency_token = std::string(idem_base) + ":chunk:" + std::to_string(c);
    if (c == 0) append_full_snapshot_leading_dels(ports, t, opt, &batch);
    const std::size_t begin = c * chunk_max;
    const std::size_t end = (std::min)(begin + chunk_max, row_ids.size());
    batch.puts.reserve(batch.puts.size() + (end - begin) + (c + 1 == num_chunks ? 8u : 0u));
    for (std::size_t i = begin; i < end; ++i) append_row_put(t, row_ids[i], opt, &batch);
    if (c + 1 == num_chunks) append_table_metadata_puts(t, opt, &batch);
    const bool fsync_chunk = fsync_last && (c + 1 == num_chunks);
    if (!submit_persist_command_batch(ports, std::move(batch), fsync_chunk, err, !opt.plain_row_values)) return false;
  }
  return true;
}

}  // namespace

bool load_table_from_storage(const MdbEnginePorts& ports, std::string_view table_name, LogicalTable* t,
                             std::uint64_t read_max_seq, std::string* err) {
  t->name = std::string(table_name);
  t->schema.clear();
  t->rows.clear();
  t->row_ids_ordered.clear();
  namespace mdb_keyspace = structdb::storage::mdb_keyspace;
  std::string schv;
  if (ports.engine->kv_get(std::string(mdb_keyspace::schema_key(table_name)), &schv, read_max_seq) && !schv.empty()) {
    std::string schraw;
    if (!wire_decode_snapshot_blob(schv, &schraw, err)) return false;
    if (!deserialize_table(schraw, t, err)) return false;
    std::string idxv;
    if (!ports.engine->kv_get(std::string(mdb_keyspace::row_index_key(table_name)), &idxv, read_max_seq) ||
        idxv.empty()) {
      logical_rebuild_str_index(t);
      logical_row_index_rebuild_from_rows(t);
      return true;
    }
    std::string idxraw;
    if (!wire_decode_snapshot_blob(idxv, &idxraw, err)) return false;
    t->rows.clear();
    t->row_ids_ordered.clear();
    std::istringstream ids_in(idxraw);
    std::string id;
    while (std::getline(ids_in, id)) {
      trim_inplace(id);
      if (id.empty()) continue;
      std::string rowv;
      if (!ports.engine->kv_get(mdb_keyspace::row_key(table_name, id), &rowv, read_max_seq)) {
        if (err) *err = "missing row key";
        return false;
      }
      std::string rowraw;
      if (!wire_decode_snapshot_blob(rowv, &rowraw, err)) return false;
      std::vector<std::string> cells;
      std::size_t pos = 0;
      while (pos <= rowraw.size()) {
        const auto tab = rowraw.find('\t', pos);
        const std::size_t end = (tab == std::string::npos) ? rowraw.size() : tab;
        cells.emplace_back(rowraw.substr(pos, end - pos));
        if (tab == std::string::npos) break;
        pos = tab + 1;
      }
      if (cells.empty()) continue;
      if (cells.size() != 1 + t->schema.size()) {
        if (err) *err = "row width";
        return false;
      }
      const std::string rid = cells[0];
      std::vector<std::string> vals(cells.begin() + 1, cells.end());
      t->rows[rid] = std::move(vals);
    }
    logical_rebuild_str_index(t);
    logical_row_index_rebuild_from_rows(t);
    load_named_index_defs_from_storage(ports, table_name, t, read_max_seq);
    logical_persist_clear_dirty(t);
    return true;
  }
  const std::string key = snapshot_key_for(table_name);
  std::string blob;
  if (!ports.engine->kv_get(key, &blob, read_max_seq) || blob.empty()) {
    if (err) *err = "no table snapshot";
    return false;
  }
  std::string raw;
  if (!wire_decode_snapshot_blob(blob, &raw, err)) return false;
  const bool ok = deserialize_table(raw, t, err);
  if (ok) {
    logical_rebuild_str_index(t);
    logical_row_index_rebuild_from_rows(t);
    load_named_index_defs_from_storage(ports, table_name, t, read_max_seq);
  }
  if (ok) logical_persist_clear_dirty(t);
  return ok;
}

bool build_persist_command_batch(const MdbEnginePorts& ports, LogicalTable& t, std::string idem_token,
                                 const PersistBuildOptions& opt, structdb::client::CommandBatch* out,
                                 std::string* err) {
  if (!out) {
    if (err) *err = "null batch";
    return false;
  }
  *out = structdb::client::CommandBatch{};
  out->idempotency_token = std::move(idem_token);
  namespace mdb_keyspace = structdb::storage::mdb_keyspace;
  const std::uint64_t read_max = (std::numeric_limits<std::uint64_t>::max)();
  const bool inc_cfg = ports.engine && ports.engine->config().snapshot().mdb_incremental_persist;
  const bool exists = table_exists_in_storage(ports, t.name, read_max);
  const bool try_inc =
      inc_cfg && exists && !t.schema.empty() && !t.mdb_persist_schema_dirty && !t.mdb_persist_dirty_rows.empty();

  if (try_inc && t.mdb_persist_dirty_rows.size() <= kMaxIncrementalDirtyRows) {
    return append_incremental_dirty(ports, t, opt, t.mdb_persist_dirty_rows, out, err);
  }

  const std::string snap_key = snapshot_key_for(t.name);
  const std::string raw = serialize_table(t);
  const std::string snap_val = wire_encode_snapshot_blob(raw, opt.wire_encoding);
  if (!t.schema.empty()) {
    out->dels.push_back(snap_key);
    if (!opt.skip_secondary_index) {
      const std::string idx_pfx = std::string(mdb_keyspace::kSecIdx) + t.name + '$';
      ports.engine->kv_visit_prefix(idx_pfx, [&](std::string_view k, std::string_view v) {
        (void)v;
        out->dels.push_back(std::string(k));
        return true;
      });
    }
  }
  if (t.schema.empty()) {
    out->puts.push_back({snap_key, snap_val});
  }
  out->puts.push_back({std::string(mdb_keyspace::catalog_key(t.name)), "1"});
  if (!t.schema.empty()) {
    std::ostringstream schema_blob;
    schema_blob << "v1\n";
    for (std::size_t i = 0; i < t.schema.size(); ++i) {
      if (i) schema_blob << ',';
      schema_blob << t.schema[i].first << ':' << t.schema[i].second;
    }
    schema_blob << "\n";
    if (!t.pk_column.empty()) schema_blob << "PKCOL\t" << t.pk_column << "\n";
    out->puts.push_back({std::string(mdb_keyspace::schema_key(t.name)),
                         wire_encode_snapshot_blob(schema_blob.str(), opt.wire_encoding)});
    const std::string idx_blob = logical_row_index_newline_blob(t);
    out->puts.push_back({std::string(mdb_keyspace::row_index_key(t.name)),
                         wire_encode_snapshot_blob(idx_blob, opt.wire_encoding)});
    const std::vector<std::string> row_ids = ordered_row_ids_for_persist(t);
    if (opt.plain_row_values) {
      out->puts.reserve(out->puts.size() + row_ids.size());
      for (const std::string& rid : row_ids) {
        out->puts.push_back({mdb_keyspace::row_key(t.name, rid), encode_row_tab_blob(t, rid)});
      }
    } else {
      out->puts.reserve(out->puts.size() + row_ids.size());
      for (const std::string& rid : row_ids) {
        out->puts.push_back({mdb_keyspace::row_key(t.name, rid), encode_row_blob(t, rid, opt.wire_encoding)});
      }
    }
    if (!opt.skip_secondary_index) {
      for (const auto& kv : t.rows) {
        for (std::size_t c = 0; c < t.schema.size(); ++c) {
          if (!is_string_type(t.schema[c].second)) continue;
          std::string ik = std::string(mdb_keyspace::kSecIdx) + t.name + '$' + t.schema[c].first + '$' +
                           hex_encode(kv.second[c]) + '$' + kv.first;
          out->puts.push_back({std::move(ik), "1"});
        }
      }
    }
  }
  return true;
}

bool submit_persist_command_batch(const MdbEnginePorts& ports, structdb::client::CommandBatch batch, bool fsync,
                                  std::string* err, bool write_journal) {
  if (!ports.client) {
    if (err) *err = "null embed client";
    return false;
  }
  if (batch.dels.empty() && batch.puts.empty()) return true;
  const auto& cfg = ports.engine->config().snapshot();
  if (!write_journal) {
    return ports.client->submit_ex(batch, fsync, false, err);
  }
  bool do_journal = !cfg.embed_journal_skip_until_commit;
  // `mdbwire2:` / plain tab row payloads cannot use legacy text journal (WAL is authoritative).
  if (do_journal && cfg.mdb_wire_encoding == structdb::facade::MdbWireEncoding::Wire2) do_journal = false;
  return ports.client->submit_ex(batch, fsync, do_journal, err);
}

bool persist_table(const MdbEnginePorts& ports, LogicalTable& t, std::string idem_token, bool fsync,
                   std::string* err) {
  if (!ports.engine || !ports.client) {
    if (err) *err = "persist: null engine or client";
    return false;
  }
  PersistBuildOptions opt;
  const auto& cfg_snap = ports.engine->config().snapshot();
  opt.wire_encoding = cfg_snap.mdb_wire_encoding;
  opt.skip_secondary_index = ports.skip_secondary_index_on_persist || cfg_snap.mdb_bulk_import_mode;

  const std::uint64_t read_max = (std::numeric_limits<std::uint64_t>::max)();
  const bool inc_cfg = cfg_snap.mdb_incremental_persist;
  const bool exists = table_exists_in_storage(ports, t.name, read_max);
  const bool large_snapshot =
      inc_cfg && exists && !t.schema.empty() && !t.mdb_persist_schema_dirty &&
      !t.mdb_persist_dirty_rows.empty() && t.mdb_persist_dirty_rows.size() > kMaxIncrementalDirtyRows;
  const bool large_table = large_snapshot || t.rows.size() > kMaxIncrementalDirtyRows;
  opt.plain_row_values = large_table && cfg_snap.mdb_bulk_persist_plain_rows;
  apply_storage_persist_hints(ports, opt, large_table);

  const auto& cfg = ports.engine->config().snapshot();
  const bool try_inc =
      inc_cfg && exists && !t.schema.empty() && !t.mdb_persist_schema_dirty && !t.mdb_persist_dirty_rows.empty();
  const bool full_snapshot = !try_inc || t.mdb_persist_dirty_rows.size() > kMaxIncrementalDirtyRows;
  const std::size_t est_puts = estimate_full_snapshot_put_count(t, opt);
  if (t.row_ids_ordered.size() != t.rows.size()) {
    logical_row_index_rebuild_from_rows(&t);
  }
  bool ok = false;
  if (full_snapshot && !t.schema.empty() &&
      should_chunk_full_snapshot_persist(cfg, t.rows.size(), est_puts)) {
    ok = persist_table_chunked(ports, t, opt, idem_token, fsync, err);
  } else {
    structdb::client::CommandBatch b;
    ok = build_persist_command_batch(ports, t, std::move(idem_token), opt, &b, err) &&
         submit_persist_command_batch(ports, std::move(b), fsync, err, !opt.plain_row_values);
  }
  reset_storage_persist_hints(ports);
  if (!ok) return false;
  logical_persist_clear_dirty(&t);
  return true;
}

bool rebuild_secondary_index_storage(const MdbEnginePorts& ports, LogicalTable& t, std::string idem_token, bool fsync,
                                     std::string* err) {
  if (t.schema.empty() || t.name.empty()) return true;
  namespace mk = structdb::storage::mdb_keyspace;
  const std::uint64_t read_max = (std::numeric_limits<std::uint64_t>::max)();
  structdb::client::CommandBatch b;
  b.idempotency_token = std::move(idem_token);
  const std::string idx_pfx = std::string(mk::kSecIdx) + t.name + '$';
  ports.engine->kv_visit_prefix(idx_pfx, [&](std::string_view k, std::string_view v) {
    (void)v;
    b.dels.push_back(std::string(k));
    return true;
  }, read_max);
  gather_named_index_keys(ports, t.name, read_max, &b);
  const std::vector<std::string> row_ids = ordered_row_ids_for_persist(t);
  for (const std::string& rid : row_ids) {
    const auto rit = t.rows.find(rid);
    if (rit == t.rows.end()) continue;
    for (std::size_t c = 0; c < t.schema.size(); ++c) {
      if (!is_string_type(t.schema[c].second)) continue;
      std::string ik = std::string(mk::kSecIdx) + t.name + '$' + t.schema[c].first + '$' +
                       hex_encode(rit->second[c]) + '$' + rid;
      b.puts.push_back({std::move(ik), "1"});
    }
  }
  for (const auto& nidx : t.named_indexes) {
    if (!rebuild_named_index_postings(ports, t, nidx.first, &b)) {
      if (err) *err = "rebuild named index failed";
      return false;
    }
  }
  for (const auto& nidx : t.named_indexes) {
    b.puts.push_back({mk::named_index_def_key(t.name, nidx.first), nidx.second.column});
  }
  return submit_persist_command_batch(ports, std::move(b), fsync, err);
}

bool table_exists_in_storage(const MdbEnginePorts& ports, std::string_view table_name, std::uint64_t read_max_seq) {
  namespace mdb_keyspace = structdb::storage::mdb_keyspace;
  std::string v;
  if (ports.engine->kv_get(std::string(mdb_keyspace::catalog_key(table_name)), &v, read_max_seq) && !v.empty())
    return true;
  return ports.engine->kv_get(snapshot_key_for(table_name), &v, read_max_seq) && !v.empty();
}

void gather_drop_table_keys(const MdbEnginePorts& ports, std::string_view table_name, std::uint64_t read_max_seq,
                            structdb::client::CommandBatch* b) {
  if (!b) return;
  namespace mdb_keyspace = structdb::storage::mdb_keyspace;
  gather_named_index_keys(ports, table_name, read_max_seq, b);
  b->dels.push_back(std::string(mdb_keyspace::catalog_key(table_name)));
  b->dels.push_back(snapshot_key_for(table_name));
  b->dels.push_back(std::string(mdb_keyspace::schema_key(table_name)));
  b->dels.push_back(std::string(mdb_keyspace::row_index_key(table_name)));
  const std::string idx_pfx = std::string(mdb_keyspace::kSecIdx) + std::string(table_name) + '$';
  ports.engine->kv_visit_prefix(idx_pfx,
                              [&](std::string_view k, std::string_view v) {
                                (void)v;
                                b->dels.push_back(std::string(k));
                                return true;
                              },
                              read_max_seq);
  const std::string row_pfx = std::string(mdb_keyspace::kRow) + std::string(table_name) + '$';
  ports.engine->kv_visit_prefix(row_pfx,
                              [&](std::string_view k, std::string_view v) {
                                (void)v;
                                b->dels.push_back(std::string(k));
                                return true;
                              },
                              read_max_seq);
}

bool rename_table_storage(const MdbEnginePorts& ports, std::string_view old_name, std::string_view new_name,
                          const std::string& idem_persist, const std::string& /*idem_drop_old*/, bool fsync,
                          std::string* err) {
  if (old_name == new_name) return true;
  const std::uint64_t read_max = (std::numeric_limits<std::uint64_t>::max)();
  if (table_exists_in_storage(ports, new_name, read_max)) {
    if (err) *err = "RENAME: target table already exists";
    return false;
  }
  LogicalTable loaded;
  if (!load_table_from_storage(ports, old_name, &loaded, read_max, err)) return false;
  loaded.name = std::string(new_name);
  PersistBuildOptions opt;
  const auto& cfg_snap = ports.engine->config().snapshot();
  opt.wire_encoding = cfg_snap.mdb_wire_encoding;
  opt.skip_secondary_index = ports.skip_secondary_index_on_persist || cfg_snap.mdb_bulk_import_mode;
  structdb::client::CommandBatch b;
  if (!build_persist_command_batch(ports, loaded, idem_persist, opt, &b, err)) return false;
  gather_drop_table_keys(ports, old_name, read_max, &b);
  return submit_persist_command_batch(ports, std::move(b), fsync, err, !opt.plain_row_values);
}

}  // namespace structdb::client::mdb
