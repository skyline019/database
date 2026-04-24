#include "page_index_sidecar.h"
#include "bptree_index.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <newdb/schema_io.h>
#include "sidecar_wal_lsn.h"
#include "visibility_checkpoint_sidecar.h"

namespace fs = std::filesystem;

namespace {

bool row_at_slot_read_sidecar(const newdb::HeapTable& tbl, const std::size_t i, newdb::Row& r) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(i, r);
    }
    if (i >= tbl.rows.size()) {
        return false;
    }
    r = tbl.rows[i];
    return true;
}

std::string sidecar_path_for(const PageSidecarRequest& req) {
    return req.data_file + ".idx." + req.order_key + (req.descending ? ".desc" : ".asc");
}

std::uint64_t file_sig(const std::string& p) {
    std::error_code ec;
    if (!fs::exists(p, ec)) return 0;
    const auto sz = fs::file_size(p, ec);
    if (ec) return 0;
    const auto tm = fs::last_write_time(p, ec);
    if (ec) return static_cast<std::uint64_t>(sz);
    const auto ticks = static_cast<std::uint64_t>(tm.time_since_epoch().count());
    return static_cast<std::uint64_t>(sz) ^ ticks;
}

bool parse_header(const std::string& line, std::string& key, bool& desc, std::uint64_t& data_sig, std::uint64_t& attr_sig, std::uint64_t& wal_lsn) {
    // key=<k>;desc=<0|1>;data_sig=<n>;attr_sig=<n>[;wal_lsn=<n>]
    const auto k0 = line.find("key=");
    const auto d0 = line.find(";desc=");
    const auto s0 = line.find(";data_sig=");
    const auto a0 = line.find(";attr_sig=");
    if (k0 != 0 || d0 == std::string::npos || s0 == std::string::npos || a0 == std::string::npos) {
        return false;
    }
    key = line.substr(4, d0 - 4);
    const std::string d = line.substr(d0 + 6, s0 - (d0 + 6));
    const std::string::size_type wpos = line.find(";wal_lsn=");
    desc = (d == "1");
    wal_lsn = 0;
    if (wpos == std::string::npos) {
        const std::string ds = line.substr(s0 + 10, a0 - (s0 + 10));
        const std::string as = line.substr(a0 + 10);
        try {
            data_sig = static_cast<std::uint64_t>(std::stoull(ds));
            attr_sig = static_cast<std::uint64_t>(std::stoull(as));
        } catch (...) {
            return false;
        }
    } else {
        const std::string ds = line.substr(s0 + 10, a0 - (s0 + 10));
        const std::string as = line.substr(a0 + 10, wpos - (a0 + 10));
        const std::string wl = line.substr(wpos + 9);
        try {
            data_sig = static_cast<std::uint64_t>(std::stoull(ds));
            attr_sig = static_cast<std::uint64_t>(std::stoull(as));
            wal_lsn = static_cast<std::uint64_t>(std::stoull(wl));
        } catch (...) {
            return false;
        }
    }
    return true;
}

bool try_read_sidecar(const std::string& path,
                      const PageSidecarRequest& req,
                      const std::uint64_t data_sig,
                      const std::uint64_t attr_sig,
                      const std::uint64_t wal_lsn,
                      std::vector<std::size_t>& out) {
    std::ifstream in(path, std::ios::in);
    if (!in) return false;
    std::string hdr;
    if (!std::getline(in, hdr)) return false;
    std::string k;
    bool d = false;
    std::uint64_t ds = 0;
    std::uint64_t as = 0;
    std::uint64_t wln = 0;
    if (!parse_header(hdr, k, d, ds, as, wln)) return false;
    if (k != req.order_key || d != req.descending || ds != data_sig || as != attr_sig || wln != wal_lsn) {
        return false;
    }
    out.clear();
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        try {
            out.push_back(static_cast<std::size_t>(std::stoull(line)));
        } catch (...) {
            return false;
        }
    }
    return true;
}

void write_sidecar(const std::string& path,
                   const PageSidecarRequest& req,
                   const std::uint64_t data_sig,
                   const std::uint64_t attr_sig,
                   const std::uint64_t wal_lsn,
                   const std::vector<std::size_t>& idx) {
    std::ofstream out(path, std::ios::out | std::ios::trunc);
    if (!out) return;
    out << "key=" << req.order_key
        << ";desc=" << (req.descending ? "1" : "0")
        << ";data_sig=" << data_sig
        << ";attr_sig=" << attr_sig
        << ";wal_lsn=" << wal_lsn << "\n";
    for (const auto v : idx) {
        out << v << "\n";
    }
}

} // namespace

std::vector<std::size_t> load_or_build_page_index_sidecar(const PageSidecarRequest& req,
                                                          const newdb::TableSchema& schema,
                                                          const newdb::HeapTable& table) {
    const std::string attr_file = newdb::schema_sidecar_path_for_data_file(req.data_file);
    const std::uint64_t data_sig = file_sig(req.data_file);
    const std::uint64_t attr_sig = file_sig(attr_file);
    const std::uint64_t wal_lsn = read_wal_lsn_for_workspace(workspace_dir_for_data_file(req.data_file));
    const std::string sidecar = sidecar_path_for(req);

    std::vector<std::size_t> indices;
    if (try_read_sidecar(sidecar, req, data_sig, attr_sig, wal_lsn, indices)) {
        return indices;
    }

    indices = load_or_build_visibility_checkpoint_sidecar(req.data_file, schema, table);

    BPlusTreeIndex bpt(req.descending);
    for (const auto slot : indices) {
        newdb::Row row;
        if (!row_at_slot_read_sidecar(table, slot, row)) {
            continue;
        }
        std::string key;
        if (req.order_key == "id") {
            key = std::to_string(row.id);
        } else {
            const auto it = row.attrs.find(req.order_key);
            key = (it == row.attrs.end()) ? std::string() : it->second;
        }
        // Normalize using schema comparator-compatible typed string domain:
        // reuse compare_attr indirectly by maintaining stable value text per typed field.
        bpt.insert(std::move(key), slot);
    }
    indices = bpt.flatten_slots();
    if (req.order_key != "id") {
        // For typed attrs (int/float/date/etc.), refine B+Tree leaf order with schema comparator.
        std::stable_sort(indices.begin(), indices.end(), [&](const std::size_t ia, const std::size_t ib) {
            newdb::Row a;
            newdb::Row b;
            if (!row_at_slot_read_sidecar(table, ia, a) || !row_at_slot_read_sidecar(table, ib, b)) {
                return ia < ib;
            }
            std::string va;
            std::string vb;
            const auto ita = a.attrs.find(req.order_key);
            const auto itb = b.attrs.find(req.order_key);
            if (ita != a.attrs.end()) va = ita->second;
            if (itb != b.attrs.end()) vb = itb->second;
            const int cmp = schema.compare_attr(req.order_key, va, vb);
            return req.descending ? (cmp > 0) : (cmp < 0);
        });
    }

    write_sidecar(sidecar, req, data_sig, attr_sig, wal_lsn, indices);
    return indices;
}

void invalidate_page_index_sidecars_for_data_file(const std::string& data_file) {
    if (data_file.empty()) {
        return;
    }
    std::error_code ec;
    newdb::TableSchema schema;
    (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema);
    std::size_t removed = 0;
    for (const auto& attr : schema.attrs) {
        removed += fs::remove(data_file + ".idx." + attr.name + ".asc", ec) ? 1u : 0u;
        ec.clear();
        removed += fs::remove(data_file + ".idx." + attr.name + ".desc", ec) ? 1u : 0u;
        ec.clear();
    }
    std::printf("[SIDECAR_INVALIDATE] page data=%s removed=%zu\n", data_file.c_str(), removed);
}

void invalidate_page_index_sidecars_for_order_attrs(const std::string& data_file,
                                                    const std::set<std::string>& order_key_names) {
    if (data_file.empty() || order_key_names.empty()) {
        return;
    }
    for (const auto& key : order_key_names) {
        if (key.empty()) {
            continue;
        }
        for (const char* suf : {".asc", ".desc"}) {
            const std::string path = data_file + ".idx." + key + suf;
            std::error_code ec;
            fs::remove(path, ec);
        }
    }
}
