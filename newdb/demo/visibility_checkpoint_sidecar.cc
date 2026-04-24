#include "visibility_checkpoint_sidecar.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <newdb/schema_io.h>

#include "sidecar_wal_lsn.h"

namespace fs = std::filesystem;

namespace {

bool row_at_slot_read_vis(const newdb::HeapTable& tbl, const std::size_t i, newdb::Row& r) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(i, r);
    }
    if (i >= tbl.rows.size()) {
        return false;
    }
    r = tbl.rows[i];
    return true;
}

bool visibility_checkpoint_enabled() {
    const char* env = std::getenv("NEWDB_VISCHK");
    if (env == nullptr) {
        return true;
    }
    std::string v(env);
    for (char& c : v) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return !(v == "0" || v == "off" || v == "false" || v == "no");
}

std::string sidecar_path_for_data_file(const std::string& data_file) {
    return data_file + ".vischk";
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

bool try_read_checkpoint(const std::string& sidecar,
                         const std::uint64_t data_sig,
                         const std::uint64_t attr_sig,
                         const std::uint64_t wal_lsn,
                         std::vector<std::size_t>& slots) {
    std::ifstream in(sidecar, std::ios::in);
    if (!in) {
        return false;
    }
    std::string hdr;
    if (!std::getline(in, hdr)) {
        return false;
    }
    // v=1;data_sig=<n>;attr_sig=<n>;wal_lsn=<n>
    const auto ds = hdr.find(";data_sig=");
    const auto as = hdr.find(";attr_sig=");
    const auto wl = hdr.find(";wal_lsn=");
    if (hdr.rfind("v=1", 0) != 0 || ds == std::string::npos || as == std::string::npos || wl == std::string::npos) {
        return false;
    }
    std::uint64_t ds_v = 0;
    std::uint64_t as_v = 0;
    std::uint64_t wl_v = 0;
    try {
        ds_v = static_cast<std::uint64_t>(std::stoull(hdr.substr(ds + 10, as - (ds + 10))));
        as_v = static_cast<std::uint64_t>(std::stoull(hdr.substr(as + 10, wl - (as + 10))));
        wl_v = static_cast<std::uint64_t>(std::stoull(hdr.substr(wl + 9)));
    } catch (...) {
        return false;
    }
    if (ds_v != data_sig || as_v != attr_sig || wl_v != wal_lsn) {
        return false;
    }
    slots.clear();
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        try {
            slots.push_back(static_cast<std::size_t>(std::stoull(line)));
        } catch (...) {
            return false;
        }
    }
    return true;
}

void write_checkpoint(const std::string& sidecar,
                      const std::uint64_t data_sig,
                      const std::uint64_t attr_sig,
                      const std::uint64_t wal_lsn,
                      const std::vector<std::size_t>& slots) {
    std::ofstream out(sidecar, std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    out << "v=1;data_sig=" << data_sig << ";attr_sig=" << attr_sig << ";wal_lsn=" << wal_lsn << "\n";
    for (const std::size_t slot : slots) {
        out << slot << "\n";
    }
}

std::vector<std::size_t> build_visible_slots(const newdb::HeapTable& table) {
    std::vector<std::size_t> slots;
    const std::size_t n = table.logical_row_count();
    slots.reserve(n);
    newdb::Row row;
    for (std::size_t i = 0; i < n; ++i) {
        if (!row_at_slot_read_vis(table, i, row)) {
            continue;
        }
        if (!table.is_row_visible(i, row)) {
            continue;
        }
        slots.push_back(i);
    }
    return slots;
}

} // namespace

std::vector<std::size_t> load_or_build_visibility_checkpoint_sidecar(const std::string& data_file,
                                                                     const newdb::TableSchema& schema,
                                                                     const newdb::HeapTable& table) {
    (void)schema;
    const std::vector<std::size_t> fallback = build_visible_slots(table);
    if (data_file.empty() || !visibility_checkpoint_enabled()) {
        return fallback;
    }
    const std::string attr_file = newdb::schema_sidecar_path_for_data_file(data_file);
    const std::uint64_t data_sig = file_sig(data_file);
    const std::uint64_t attr_sig = file_sig(attr_file);
    const std::uint64_t wal_lsn = read_wal_lsn_for_workspace(workspace_dir_for_data_file(data_file));
    const std::string sidecar = sidecar_path_for_data_file(data_file);
    std::vector<std::size_t> slots;
    if (try_read_checkpoint(sidecar, data_sig, attr_sig, wal_lsn, slots)) {
        return slots;
    }
    write_checkpoint(sidecar, data_sig, attr_sig, wal_lsn, fallback);
    return fallback;
}

void invalidate_visibility_checkpoint_sidecars_for_data_file(const std::string& data_file) {
    if (data_file.empty()) {
        return;
    }
    std::error_code ec;
    const bool removed = fs::remove(sidecar_path_for_data_file(data_file), ec);
    std::printf("[SIDECAR_INVALIDATE] vischk data=%s removed=%d\n", data_file.c_str(), removed ? 1 : 0);
}

void invalidate_visibility_checkpoint_sidecars_for_attrs(const std::string& data_file,
                                                         const std::set<std::string>& attr_names) {
    if (data_file.empty() || attr_names.empty()) {
        return;
    }
    // Conservative phase-1 invalidation for attribute-level writes.
    invalidate_visibility_checkpoint_sidecars_for_data_file(data_file);
}
