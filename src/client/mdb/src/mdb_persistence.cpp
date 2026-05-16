#include "structdb/client/mdb_persistence.hpp"

#include "structdb/client/mdb_logical_table.hpp"
#include "structdb/client/detail/mdb_ops_detail.hpp"
#include "structdb/client/detail/mdb_runner_internal.hpp"
#include "structdb/client/embed_client.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/storage/mdb_keyspace.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <sstream>
#include <vector>

namespace structdb::client::mdb {

bool load_table_from_storage(const MdbEnginePorts& ports, std::string_view table_name, LogicalTable* t,
                             std::uint64_t read_max_seq, std::string* err) {
  t->name = std::string(table_name);
  t->schema.clear();
  t->rows.clear();
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
      return true;
    }
    std::string idxraw;
    if (!wire_decode_snapshot_blob(idxv, &idxraw, err)) return false;
    t->rows.clear();
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
  if (ok) logical_rebuild_str_index(t);
  if (ok) logical_persist_clear_dirty(t);
  return ok;
}

bool persist_table(const MdbEnginePorts& ports, LogicalTable& t, std::string idem_token, bool fsync,
                   std::string* err) {
  namespace mdb_keyspace = structdb::storage::mdb_keyspace;
  const std::uint64_t read_max = (std::numeric_limits<std::uint64_t>::max)();
  const bool inc_cfg = ports.engine && ports.engine->config().snapshot().mdb_incremental_persist;
  const bool exists = table_exists_in_storage(ports, t.name, read_max);
  const bool try_inc =
      inc_cfg && exists && !t.schema.empty() && !t.mdb_persist_schema_dirty && !t.mdb_persist_dirty_rows.empty();
  if (try_inc && t.mdb_persist_dirty_rows.size() <= 8192u) {
    structdb::client::CommandBatch b;
    b.idempotency_token = std::move(idem_token);
    for (const std::string& rid : t.mdb_persist_dirty_rows) {
      const auto pit = t.mdb_persist_prev_cells.find(rid);
      const auto rit = t.rows.find(rid);
      const bool deleted = rit == t.rows.end();
      if (deleted) {
        if (pit == t.mdb_persist_prev_cells.end()) {
          if (err) *err = "persist: incremental delete missing prev row";
          return false;
        }
        b.dels.push_back(mdb_keyspace::row_key(t.name, rid));
        for (std::size_t c = 0; c < t.schema.size() && c < pit->second.size(); ++c) {
          if (!is_string_type(t.schema[c].second)) continue;
          std::string ik = std::string(mdb_keyspace::kSecIdx) + t.name + '$' + t.schema[c].first + '$' +
                           hex_encode(pit->second[c]) + '$' + rid;
          b.dels.push_back(std::move(ik));
        }
      } else {
        if (pit != t.mdb_persist_prev_cells.end()) {
          for (std::size_t c = 0; c < t.schema.size() && c < pit->second.size(); ++c) {
            if (!is_string_type(t.schema[c].second)) continue;
            std::string ik = std::string(mdb_keyspace::kSecIdx) + t.name + '$' + t.schema[c].first + '$' +
                             hex_encode(pit->second[c]) + '$' + rid;
            b.dels.push_back(std::move(ik));
          }
        }
        std::ostringstream row;
        row << rit->first;
        for (const auto& cell : rit->second) row << '\t' << cell;
        b.puts.push_back({mdb_keyspace::row_key(t.name, rit->first), wire_encode_snapshot_blob(row.str())});
        for (std::size_t c = 0; c < t.schema.size() && c < rit->second.size(); ++c) {
          if (!is_string_type(t.schema[c].second)) continue;
          std::string ik = std::string(mdb_keyspace::kSecIdx) + t.name + '$' + t.schema[c].first + '$' +
                           hex_encode(rit->second[c]) + '$' + rit->first;
          b.puts.push_back({std::move(ik), "1"});
        }
      }
    }
    std::ostringstream idlist;
    bool first = true;
    for (const auto& kv : t.rows) {
      if (!first) idlist << '\n';
      first = false;
      idlist << kv.first;
    }
    b.puts.push_back({std::string(mdb_keyspace::row_index_key(t.name)), wire_encode_snapshot_blob(idlist.str())});
    b.puts.push_back({std::string(mdb_keyspace::catalog_key(t.name)), "1"});
    if (!ports.client->submit(b, fsync, err)) return false;
    logical_persist_clear_dirty(&t);
    return true;
  }

  const std::string snap_key = snapshot_key_for(t.name);
  const std::string raw = serialize_table(t);
  const std::string snap_val = wire_encode_snapshot_blob(raw);
  structdb::client::CommandBatch b;
  b.idempotency_token = std::move(idem_token);
  if (!t.schema.empty()) {
    b.dels.push_back(snap_key);
    const std::string idx_pfx = std::string(mdb_keyspace::kSecIdx) + t.name + '$';
    ports.engine->kv_visit_prefix(idx_pfx, [&](std::string_view k, std::string_view v) {
      (void)v;
      b.dels.push_back(std::string(k));
      return true;
    });
  }
  if (t.schema.empty()) {
    b.puts.push_back({snap_key, snap_val});
  }
  b.puts.push_back({std::string(mdb_keyspace::catalog_key(t.name)), "1"});
  if (!t.schema.empty()) {
    std::ostringstream schema_blob;
    schema_blob << "v1\n";
    for (std::size_t i = 0; i < t.schema.size(); ++i) {
      if (i) schema_blob << ',';
      schema_blob << t.schema[i].first << ':' << t.schema[i].second;
    }
    schema_blob << "\n";
    if (!t.pk_column.empty()) {
      schema_blob << "PKCOL\t" << t.pk_column << "\n";
    }
    b.puts.push_back({std::string(mdb_keyspace::schema_key(t.name)),
                      wire_encode_snapshot_blob(schema_blob.str())});
    std::ostringstream idlist;
    bool first = true;
    for (const auto& kv : t.rows) {
      if (!first) idlist << '\n';
      first = false;
      idlist << kv.first;
    }
    b.puts.push_back({std::string(mdb_keyspace::row_index_key(t.name)),
                      wire_encode_snapshot_blob(idlist.str())});
    for (const auto& kv : t.rows) {
      std::ostringstream row;
      row << kv.first;
      for (const auto& cell : kv.second) row << '\t' << cell;
      b.puts.push_back({mdb_keyspace::row_key(t.name, kv.first), wire_encode_snapshot_blob(row.str())});
    }
    for (const auto& kv : t.rows) {
      for (std::size_t c = 0; c < t.schema.size(); ++c) {
        if (!is_string_type(t.schema[c].second)) continue;
        std::string ik = std::string(structdb::storage::mdb_keyspace::kSecIdx) + t.name + '$' + t.schema[c].first +
                         '$' + hex_encode(kv.second[c]) + '$' + kv.first;
        b.puts.push_back({std::move(ik), "1"});
      }
    }
  }
  if (!ports.client->submit(b, fsync, err)) return false;
  logical_persist_clear_dirty(&t);
  return true;
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
                          const std::string& idem_persist, const std::string& idem_drop_old, bool fsync,
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
  if (!persist_table(ports, loaded, idem_persist, fsync, err)) return false;
  structdb::client::CommandBatch b;
  b.idempotency_token = idem_drop_old;
  gather_drop_table_keys(ports, old_name, read_max, &b);
  return ports.client->submit(b, fsync, err);
}

}  // namespace structdb::client::mdb
