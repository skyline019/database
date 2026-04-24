#include "covering_index_sidecar.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <cstdio>
#include <sstream>
#include <unordered_map>

#include <newdb/schema_io.h>

#include "sidecar_wal_lsn.h"
#include "visibility_checkpoint_sidecar.h"

namespace fs = std::filesystem;

namespace {

struct AggBucket {
    std::size_t count{0};
    long double sum{0.0L};
};

struct SidecarOffsetIndex {
    std::string key_attr;
    std::string include_attr;
    std::uint64_t data_sig{0};
    std::uint64_t attr_sig{0};
    std::uint64_t wal_lsn{0};
    std::unordered_map<std::string, std::uint64_t> key_offsets;
};

struct CoveringAggSidecarState {
    std::string key_attr;
    std::string include_attr;
    std::uint64_t data_sig{0};
    std::uint64_t attr_sig{0};
    std::uint64_t wal_lsn{0};
    std::size_t rows_hint{0};
    std::unordered_map<std::string, AggBucket> buckets;
};

struct ProjRow {
    int id{0};
    std::string value;
};

std::string sidecar_path_for(const std::string& data_file,
                             const std::string& key_attr,
                             const std::string& include_attr) {
    return data_file + ".cov." + key_attr + "." + include_attr;
}

std::string proj_sidecar_path_for(const std::string& data_file,
                                  const std::string& key_attr,
                                  const std::string& proj_attr) {
    return data_file + ".covp." + key_attr + "." + proj_attr;
}

std::string sidecar_index_path_for(const std::string& sidecar_path) {
    return sidecar_path + ".idx";
}

std::uint64_t file_sig(const std::string& p) {
    std::error_code ec;
    if (!fs::exists(p, ec)) {
        return 0;
    }
    const auto sz = fs::file_size(p, ec);
    if (ec) {
        return 0;
    }
    const auto tm = fs::last_write_time(p, ec);
    if (ec) {
        return static_cast<std::uint64_t>(sz);
    }
    const auto ticks = static_cast<std::uint64_t>(tm.time_since_epoch().count());
    return static_cast<std::uint64_t>(sz) ^ ticks;
}

bool row_at_slot_read_cov(const newdb::HeapTable& tbl, const std::size_t slot, newdb::Row& row) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(slot, row);
    }
    if (slot >= tbl.rows.size()) {
        return false;
    }
    row = tbl.rows[slot];
    return true;
}

void write_proj_sidecar(const std::string& path,
                        const std::string& key_attr,
                        const std::string& proj_attr,
                        const std::uint64_t data_sig,
                        const std::uint64_t attr_sig,
                        const std::uint64_t wal_lsn,
                        const std::unordered_map<std::string, std::vector<ProjRow>>& rows,
                        const std::size_t limit) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    out << "v=1;type=proj;key=" << key_attr << ";proj=" << proj_attr << ";limit=" << limit
        << ";data_sig=" << data_sig << ";attr_sig=" << attr_sig << ";wal_lsn=" << wal_lsn << "\n";
    for (const auto& kv : rows) {
        std::size_t wrote = 0;
        for (const auto& r : kv.second) {
            if (wrote++ >= limit) break;
            out << kv.first.size() << ":" << kv.first << "\t" << r.id << "\t" << r.value.size() << ":" << r.value << "\n";
        }
    }
}

bool read_proj_sidecar(const std::string& path,
                       const std::string& key_attr,
                       const std::string& proj_attr,
                       const std::uint64_t data_sig,
                       const std::uint64_t attr_sig,
                       const std::uint64_t wal_lsn,
                       const std::string& key_value,
                       const std::size_t limit,
                       std::vector<CoveringProjRow>& out) {
    std::ifstream in(path, std::ios::in);
    if (!in) return false;
    std::string hdr;
    if (!std::getline(in, hdr)) return false;
    if (hdr.find("type=proj") == std::string::npos ||
        hdr.find("key=" + key_attr) == std::string::npos ||
        hdr.find("proj=" + proj_attr) == std::string::npos ||
        hdr.find("data_sig=" + std::to_string(data_sig)) == std::string::npos ||
        hdr.find("attr_sig=" + std::to_string(attr_sig)) == std::string::npos ||
        hdr.find("wal_lsn=" + std::to_string(wal_lsn)) == std::string::npos) {
        return false;
    }
    out.clear();
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        const auto c = line.find(':');
        const auto t1 = line.find('\t');
        const auto t2 = (t1 == std::string::npos) ? std::string::npos : line.find('\t', t1 + 1);
        if (c == std::string::npos || t1 == std::string::npos || t2 == std::string::npos) return false;
        std::size_t klen = 0;
        try {
            klen = static_cast<std::size_t>(std::stoull(line.substr(0, c)));
        } catch (...) {
            return false;
        }
        const std::size_t kb = c + 1;
        if (kb + klen != t1) return false;
        const std::string key = line.substr(kb, klen);
        if (key != key_value) continue;
        int id = 0;
        try {
            id = std::stoi(line.substr(t1 + 1, t2 - (t1 + 1)));
        } catch (...) {
            return false;
        }
        const std::string rest = line.substr(t2 + 1);
        const auto c2 = rest.find(':');
        if (c2 == std::string::npos) return false;
        std::size_t vlen = 0;
        try {
            vlen = static_cast<std::size_t>(std::stoull(rest.substr(0, c2)));
        } catch (...) {
            return false;
        }
        const std::string val = rest.substr(c2 + 1);
        if (val.size() != vlen) return false;
        out.push_back(CoveringProjRow{.id = id, .value = val});
        if (out.size() >= limit) break;
    }
    return true;
}

void write_sidecar(const std::string& path,
                   const std::string& key_attr,
                   const std::string& include_attr,
                   const std::uint64_t data_sig,
                   const std::uint64_t attr_sig,
                   const std::uint64_t wal_lsn,
                   const std::size_t rows_hint,
                   const std::unordered_map<std::string, AggBucket>& buckets) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    std::unordered_map<std::string, std::uint64_t> key_offsets;
    key_offsets.reserve(buckets.size());
    out << "v=1;key=" << key_attr << ";include=" << include_attr << ";data_sig=" << data_sig
        << ";attr_sig=" << attr_sig << ";wal_lsn=" << wal_lsn << ";rows=" << rows_hint << "\n";
    for (const auto& kv : buckets) {
        const std::streampos pos = out.tellp();
        if (pos >= 0) {
            key_offsets[kv.first] = static_cast<std::uint64_t>(pos);
        }
        out << kv.first.size() << ":" << kv.first << "\t" << kv.second.count << "\t" << kv.second.sum << "\n";
    }
    std::ofstream idx(sidecar_index_path_for(path), std::ios::out | std::ios::trunc);
    if (!idx) {
        return;
    }
    idx << "v=1;type=agg_idx;key=" << key_attr << ";include=" << include_attr << ";data_sig=" << data_sig
        << ";attr_sig=" << attr_sig << ";wal_lsn=" << wal_lsn << "\n";
    for (const auto& kv : key_offsets) {
        idx << kv.first.size() << ":" << kv.first << "\t" << kv.second << "\n";
    }
}

std::optional<std::string> parse_header_value(const std::string& hdr, const std::string& key) {
    const std::string token = key + "=";
    const auto pos = hdr.find(token);
    if (pos == std::string::npos) {
        return std::nullopt;
    }
    auto end = hdr.find(';', pos);
    if (end == std::string::npos) {
        end = hdr.size();
    }
    return hdr.substr(pos + token.size(), end - (pos + token.size()));
}

bool read_sidecar_state(const std::string& path, CoveringAggSidecarState& state) {
    std::ifstream in(path, std::ios::in);
    if (!in) {
        return false;
    }
    std::string hdr;
    if (!std::getline(in, hdr)) {
        return false;
    }
    const auto key_attr_v = parse_header_value(hdr, "key");
    const auto include_attr_v = parse_header_value(hdr, "include");
    const auto data_sig_v = parse_header_value(hdr, "data_sig");
    const auto attr_sig_v = parse_header_value(hdr, "attr_sig");
    const auto wal_lsn_v = parse_header_value(hdr, "wal_lsn");
    if (!key_attr_v.has_value() || !include_attr_v.has_value() || !data_sig_v.has_value() ||
        !attr_sig_v.has_value() || !wal_lsn_v.has_value()) {
        return false;
    }
    try {
        state.key_attr = *key_attr_v;
        state.include_attr = *include_attr_v;
        state.data_sig = static_cast<std::uint64_t>(std::stoull(*data_sig_v));
        state.attr_sig = static_cast<std::uint64_t>(std::stoull(*attr_sig_v));
        state.wal_lsn = static_cast<std::uint64_t>(std::stoull(*wal_lsn_v));
        const auto rows_v = parse_header_value(hdr, "rows");
        state.rows_hint = rows_v.has_value() ? static_cast<std::size_t>(std::stoull(*rows_v)) : 0;
    } catch (...) {
        return false;
    }
    state.buckets.clear();
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const auto c = line.find(':');
        const auto t1 = line.find('\t');
        const auto t2 = (t1 == std::string::npos) ? std::string::npos : line.find('\t', t1 + 1);
        if (c == std::string::npos || t1 == std::string::npos || t2 == std::string::npos) {
            return false;
        }
        std::size_t klen = 0;
        try {
            klen = static_cast<std::size_t>(std::stoull(line.substr(0, c)));
        } catch (...) {
            return false;
        }
        const std::size_t kb = c + 1;
        if (kb + klen != t1) {
            return false;
        }
        const std::string key = line.substr(kb, klen);
        try {
            AggBucket bucket;
            bucket.count = static_cast<std::size_t>(std::stoull(line.substr(t1 + 1, t2 - (t1 + 1))));
            bucket.sum = std::stold(line.substr(t2 + 1));
            state.buckets[key] = bucket;
        } catch (...) {
            return false;
        }
    }
    return true;
}

bool read_sidecar_fast(const std::string& path,
                       const std::string& key_attr,
                       const std::string& include_attr,
                       const std::uint64_t data_sig,
                       const std::uint64_t attr_sig,
                       const std::uint64_t wal_lsn,
                       const std::string& key_value,
                       CoveringAggLookup& out) {
    static std::unordered_map<std::string, SidecarOffsetIndex> index_cache;
    const auto cache_it = index_cache.find(path);
    if (cache_it != index_cache.end() &&
        cache_it->second.key_attr == key_attr &&
        cache_it->second.include_attr == include_attr &&
        cache_it->second.data_sig == data_sig &&
        cache_it->second.attr_sig == attr_sig &&
        cache_it->second.wal_lsn == wal_lsn) {
        const auto it = cache_it->second.key_offsets.find(key_value);
        if (it == cache_it->second.key_offsets.end()) {
            out.used = true;
            out.count = 0;
            out.sum = 0.0L;
            return true;
        }
        std::ifstream in(path, std::ios::in);
        if (!in) {
            return false;
        }
        in.seekg(static_cast<std::streamoff>(it->second), std::ios::beg);
        std::string line;
        if (!std::getline(in, line)) {
            return false;
        }
        const auto c = line.find(':');
        const auto t1 = line.find('\t');
        const auto t2 = (t1 == std::string::npos) ? std::string::npos : line.find('\t', t1 + 1);
        if (c == std::string::npos || t1 == std::string::npos || t2 == std::string::npos) {
            return false;
        }
        try {
            out.used = true;
            out.count = static_cast<std::size_t>(std::stoull(line.substr(t1 + 1, t2 - (t1 + 1))));
            out.sum = std::stold(line.substr(t2 + 1));
            return true;
        } catch (...) {
            return false;
        }
    }

    SidecarOffsetIndex idx_state;
    idx_state.key_attr = key_attr;
    idx_state.include_attr = include_attr;
    idx_state.data_sig = data_sig;
    idx_state.attr_sig = attr_sig;
    idx_state.wal_lsn = wal_lsn;
    {
        std::ifstream idx_in(sidecar_index_path_for(path), std::ios::in);
        if (idx_in) {
            std::string idx_hdr;
            if (std::getline(idx_in, idx_hdr) &&
                idx_hdr.find("type=agg_idx") != std::string::npos &&
                idx_hdr.find("key=" + key_attr) != std::string::npos &&
                idx_hdr.find("include=" + include_attr) != std::string::npos &&
                idx_hdr.find("data_sig=" + std::to_string(data_sig)) != std::string::npos &&
                idx_hdr.find("attr_sig=" + std::to_string(attr_sig)) != std::string::npos &&
                idx_hdr.find("wal_lsn=" + std::to_string(wal_lsn)) != std::string::npos) {
                std::string line;
                while (std::getline(idx_in, line)) {
                    if (line.empty()) continue;
                    const auto c = line.find(':');
                    const auto t = line.find('\t');
                    if (c == std::string::npos || t == std::string::npos) {
                        idx_state.key_offsets.clear();
                        break;
                    }
                    std::size_t klen = 0;
                    try {
                        klen = static_cast<std::size_t>(std::stoull(line.substr(0, c)));
                    } catch (...) {
                        idx_state.key_offsets.clear();
                        break;
                    }
                    const std::size_t kb = c + 1;
                    if (kb + klen != t) {
                        idx_state.key_offsets.clear();
                        break;
                    }
                    std::string key = line.substr(kb, klen);
                    try {
                        const std::uint64_t off = static_cast<std::uint64_t>(std::stoull(line.substr(t + 1)));
                        idx_state.key_offsets[std::move(key)] = off;
                    } catch (...) {
                        idx_state.key_offsets.clear();
                        break;
                    }
                }
            }
        }
    }
    if (!idx_state.key_offsets.empty()) {
        index_cache[path] = std::move(idx_state);
        return read_sidecar_fast(path, key_attr, include_attr, data_sig, attr_sig, wal_lsn, key_value, out);
    }

    std::ifstream in(path, std::ios::in);
    if (!in) {
        return false;
    }
    std::string hdr;
    if (!std::getline(in, hdr)) {
        return false;
    }
    if (hdr.find("key=" + key_attr) == std::string::npos || hdr.find("include=" + include_attr) == std::string::npos ||
        hdr.find("data_sig=" + std::to_string(data_sig)) == std::string::npos ||
        hdr.find("attr_sig=" + std::to_string(attr_sig)) == std::string::npos ||
        hdr.find("wal_lsn=" + std::to_string(wal_lsn)) == std::string::npos) {
        return false;
    }
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        const auto c = line.find(':');
        const auto t1 = line.find('\t');
        const auto t2 = (t1 == std::string::npos) ? std::string::npos : line.find('\t', t1 + 1);
        if (c == std::string::npos || t1 == std::string::npos || t2 == std::string::npos) {
            return false;
        }
        std::size_t klen = 0;
        try {
            klen = static_cast<std::size_t>(std::stoull(line.substr(0, c)));
        } catch (...) {
            return false;
        }
        const std::size_t kb = c + 1;
        if (kb + klen != t1) {
            return false;
        }
        const std::string key = line.substr(kb, klen);
        if (key != key_value) {
            continue;
        }
        try {
            out.used = true;
            out.count = static_cast<std::size_t>(std::stoull(line.substr(t1 + 1, t2 - (t1 + 1))));
            out.sum = std::stold(line.substr(t2 + 1));
        } catch (...) {
            return false;
        }
        return true;
    }
    out.used = true;
    out.count = 0;
    out.sum = 0.0L;
    return true;
}

} // namespace

CoveringAggLookup lookup_or_build_covering_agg_sidecar(const std::string& data_file,
                                                       const std::string& key_attr,
                                                       const std::string& include_attr,
                                                       const std::string& key_value,
                                                       const newdb::TableSchema& schema,
                                                       const newdb::HeapTable& table) {
    CoveringAggLookup out;
    if (data_file.empty() || key_attr.empty() || include_attr.empty()) {
        return out;
    }
    const std::string sidecar = sidecar_path_for(data_file, key_attr, include_attr);
    const std::uint64_t data_sig = file_sig(data_file);
    const std::uint64_t attr_sig = file_sig(newdb::schema_sidecar_path_for_data_file(data_file));
    const std::uint64_t wal_lsn = read_wal_lsn_for_workspace(workspace_dir_for_data_file(data_file));
    if (read_sidecar_fast(sidecar, key_attr, include_attr, data_sig, attr_sig, wal_lsn, key_value, out)) {
        return out;
    }
    const std::vector<std::size_t> visible_slots = load_or_build_visibility_checkpoint_sidecar(data_file, schema, table);
    std::unordered_map<std::string, AggBucket> buckets;
    CoveringAggSidecarState sidecar_state;
    if (read_sidecar_state(sidecar, sidecar_state) &&
        sidecar_state.key_attr == key_attr &&
        sidecar_state.include_attr == include_attr &&
        sidecar_state.data_sig == data_sig &&
        sidecar_state.attr_sig == attr_sig) {
        buckets = std::move(sidecar_state.buckets);
        if (sidecar_state.wal_lsn != wal_lsn && visible_slots.size() >= sidecar_state.rows_hint) {
            newdb::Row row;
            for (std::size_t i = sidecar_state.rows_hint; i < visible_slots.size(); ++i) {
                const std::size_t slot = visible_slots[i];
                if (!row_at_slot_read_cov(table, slot, row)) {
                    continue;
                }
                std::string key;
                if (key_attr == "id") key = std::to_string(row.id);
                else {
                    const auto it = row.attrs.find(key_attr);
                    if (it == row.attrs.end()) continue;
                    key = it->second;
                }
                AggBucket& b = buckets[key];
                ++b.count;
                if (include_attr != "__count__") {
                    std::string val;
                    if (include_attr == "id") val = std::to_string(row.id);
                    else {
                        const auto it = row.attrs.find(include_attr);
                        if (it == row.attrs.end()) continue;
                        val = it->second;
                    }
                    try {
                        b.sum += std::stold(val);
                    } catch (...) {
                        continue;
                    }
                }
            }
            write_sidecar(sidecar, key_attr, include_attr, data_sig, attr_sig, wal_lsn, visible_slots.size(), buckets);
        } else if (sidecar_state.wal_lsn == wal_lsn) {
            const auto it = buckets.find(key_value);
            out.used = true;
            if (it == buckets.end()) {
                out.count = 0;
                out.sum = 0.0L;
            } else {
                out.count = it->second.count;
                out.sum = it->second.sum;
            }
            return out;
        } else {
            buckets.clear();
        }
    }

    if (buckets.empty()) {
        buckets.clear();
    }

    newdb::Row row;
    if (buckets.empty()) {
        for (const std::size_t slot : visible_slots) {
            if (!row_at_slot_read_cov(table, slot, row)) {
                continue;
            }
            std::string key;
            if (key_attr == "id") {
                key = std::to_string(row.id);
            } else {
                const auto it = row.attrs.find(key_attr);
                if (it == row.attrs.end()) {
                    continue;
                }
                key = it->second;
            }
            AggBucket& b = buckets[key];
            ++b.count;
            if (include_attr == "__count__") {
                continue;
            }
            std::string val;
            if (include_attr == "id") {
                val = std::to_string(row.id);
            } else {
                const auto it = row.attrs.find(include_attr);
                if (it == row.attrs.end()) {
                    continue;
                }
                val = it->second;
            }
            try {
                b.sum += std::stold(val);
            } catch (...) {
                continue;
            }
        }
    }
    write_sidecar(sidecar, key_attr, include_attr, data_sig, attr_sig, wal_lsn, visible_slots.size(), buckets);
    const auto it = buckets.find(key_value);
    out.used = true;
    if (it == buckets.end()) {
        out.count = 0;
        out.sum = 0.0L;
        return out;
    }
    out.count = it->second.count;
    out.sum = it->second.sum;
    return out;
}

std::vector<CoveringProjRow> lookup_or_build_covering_proj_sidecar(const std::string& data_file,
                                                                   const std::string& key_attr,
                                                                   const std::string& proj_attr,
                                                                   const std::string& key_value,
                                                                   const std::size_t limit,
                                                                   const newdb::TableSchema& schema,
                                                                   const newdb::HeapTable& table) {
    std::vector<CoveringProjRow> out;
    if (data_file.empty() || key_attr.empty() || proj_attr.empty() || limit == 0) {
        return out;
    }
    const std::string sidecar = proj_sidecar_path_for(data_file, key_attr, proj_attr);
    const std::uint64_t data_sig = file_sig(data_file);
    const std::uint64_t attr_sig = file_sig(newdb::schema_sidecar_path_for_data_file(data_file));
    const std::uint64_t wal_lsn = read_wal_lsn_for_workspace(workspace_dir_for_data_file(data_file));
    if (read_proj_sidecar(sidecar, key_attr, proj_attr, data_sig, attr_sig, wal_lsn, key_value, limit, out)) {
        return out;
    }

    std::unordered_map<std::string, std::vector<ProjRow>> rows;
    const std::vector<std::size_t> visible_slots = load_or_build_visibility_checkpoint_sidecar(data_file, schema, table);
    newdb::Row row;
    for (const std::size_t slot : visible_slots) {
        if (!row_at_slot_read_cov(table, slot, row)) continue;
        std::string key;
        if (key_attr == "id") {
            key = std::to_string(row.id);
        } else {
            const auto it = row.attrs.find(key_attr);
            if (it == row.attrs.end()) continue;
            key = it->second;
        }
        std::string val;
        if (proj_attr == "id") {
            val = std::to_string(row.id);
        } else {
            const auto it = row.attrs.find(proj_attr);
            if (it == row.attrs.end()) val = "";
            else val = it->second;
        }
        auto& vec = rows[key];
        if (vec.size() < limit) {
            vec.push_back(ProjRow{.id = row.id, .value = std::move(val)});
        }
    }
    write_proj_sidecar(sidecar, key_attr, proj_attr, data_sig, attr_sig, wal_lsn, rows, limit);

    // Reload from in-memory map for requested key.
    const auto it = rows.find(key_value);
    if (it == rows.end()) return out;
    out.reserve(std::min(limit, it->second.size()));
    for (const auto& r : it->second) {
        out.push_back(CoveringProjRow{.id = r.id, .value = r.value});
        if (out.size() >= limit) break;
    }
    return out;
}

void invalidate_covering_sidecars_for_data_file(const std::string& data_file) {
    if (data_file.empty()) {
        return;
    }
    std::error_code ec;
    newdb::TableSchema schema;
    (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema);
    std::size_t removed = 0;
    for (const auto& key_attr : schema.attrs) {
        for (const auto& inc_attr : schema.attrs) {
            removed += fs::remove(data_file + ".cov." + key_attr.name + "." + inc_attr.name, ec) ? 1u : 0u;
            ec.clear();
            removed += fs::remove(data_file + ".covp." + key_attr.name + "." + inc_attr.name, ec) ? 1u : 0u;
            ec.clear();
        }
    }
    std::printf("[SIDECAR_INVALIDATE] covering data=%s removed=%zu\n", data_file.c_str(), removed);
}

void invalidate_covering_sidecars_for_attrs(const std::string& data_file,
                                            const std::set<std::string>& attrs) {
    if (data_file.empty() || attrs.empty()) {
        return;
    }
    // Phase-1 conservative strategy for covering sidecar.
    invalidate_covering_sidecars_for_data_file(data_file);
}
