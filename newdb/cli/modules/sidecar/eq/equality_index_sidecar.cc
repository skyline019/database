#include "cli/modules/sidecar/eq/equality_index_sidecar.h"
#include <algorithm>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <sstream>
#include <unordered_map>
#include <system_error>

#include <newdb/memory_budget.h>
#include <newdb/memory_registry.h>
#include <newdb/schema_io.h>
#include "cli/modules/sidecar/common/index_catalog.h"
#include "cli/modules/sidecar/common/sidecar_wal_lsn.h"
#include "cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.h"
#include "cli/modules/sidecar/eq/eq_bloom.h"
#include "cli/modules/where/executor/where.h"

#include <list>
#include <mutex>

namespace fs = std::filesystem;

namespace {

std::atomic<std::uint64_t> g_eq_sidecar_invalidate_remove_fails{0};
std::atomic<std::uint64_t> g_eq_sidecar_memory_budget_skips{0};

struct EqSidecarCacheEntry {
    std::uint64_t data_sig{0};
    std::uint64_t attr_sig{0};
    std::uint64_t wal_lsn{0};
    std::string header_line;
    IndexCatalogParsedTail catalog_tail{};
    std::map<std::string, std::vector<std::size_t>> buckets;
};

std::unordered_map<std::string, EqSidecarCacheEntry> g_eq_sidecar_cache;

std::recursive_mutex g_eq_sidecar_mu;
std::list<std::string> g_eq_sidecar_lru;
std::unordered_map<std::string, std::list<std::string>::iterator> g_eq_sidecar_lru_pos;
std::unordered_map<std::string, std::uint64_t> g_eq_sidecar_bytes;
std::atomic<bool> g_eq_sidecar_evictor_registered{false};

std::uint64_t estimate_eq_entry_bytes(const EqSidecarCacheEntry& e) {
    std::uint64_t sz = static_cast<std::uint64_t>(e.header_line.size()) + 64u;
    for (const auto& kv : e.buckets) {
        sz += static_cast<std::uint64_t>(kv.first.size()) + 64u +
              static_cast<std::uint64_t>(kv.second.size()) * sizeof(std::size_t);
    }
    return sz;
}

void eq_sidecar_lru_remove_locked(const std::string& path) {
    const auto pit = g_eq_sidecar_lru_pos.find(path);
    if (pit != g_eq_sidecar_lru_pos.end()) {
        g_eq_sidecar_lru.erase(pit->second);
        g_eq_sidecar_lru_pos.erase(pit);
    }
}

void eq_sidecar_lru_touch_front_locked(const std::string& path) {
    eq_sidecar_lru_remove_locked(path);
    g_eq_sidecar_lru.push_front(path);
    g_eq_sidecar_lru_pos[path] = g_eq_sidecar_lru.begin();
}

std::uint64_t eq_sidecar_release_path_locked(const std::string& path) {
    const auto bit = g_eq_sidecar_bytes.find(path);
    if (bit == g_eq_sidecar_bytes.end()) {
        return 0;
    }
    const std::uint64_t bytes = bit->second;
    g_eq_sidecar_bytes.erase(bit);
    eq_sidecar_lru_remove_locked(path);
    return bytes;
}

void eq_sidecar_cache_erase(const std::string& path) {
    std::uint64_t bytes_to_release = 0;
    {
        std::lock_guard<std::recursive_mutex> lk(g_eq_sidecar_mu);
        const auto cit = g_eq_sidecar_cache.find(path);
        if (cit != g_eq_sidecar_cache.end()) {
            g_eq_sidecar_cache.erase(cit);
        }
        bytes_to_release = eq_sidecar_release_path_locked(path);
    }
    if (bytes_to_release > 0) {
        newdb::memory_registry_release(newdb::MemoryKind::EqSidecar, bytes_to_release);
    }
}

std::uint64_t eq_sidecar_evict_tail_locked(std::uint64_t target_free,
                                           std::vector<std::string>& evicted_paths) {
    std::uint64_t freed = 0;
    while (freed < target_free && !g_eq_sidecar_lru.empty()) {
        const std::string victim = g_eq_sidecar_lru.back();
        const auto bit = g_eq_sidecar_bytes.find(victim);
        const std::uint64_t bytes = (bit != g_eq_sidecar_bytes.end()) ? bit->second : 0;
        g_eq_sidecar_lru.pop_back();
        g_eq_sidecar_lru_pos.erase(victim);
        if (bit != g_eq_sidecar_bytes.end()) {
            g_eq_sidecar_bytes.erase(bit);
        }
        const auto cit = g_eq_sidecar_cache.find(victim);
        if (cit != g_eq_sidecar_cache.end()) {
            g_eq_sidecar_cache.erase(cit);
        }
        evicted_paths.push_back(victim);
        freed += bytes;
    }
    return freed;
}

std::uint64_t eq_sidecar_evictor_callback(std::uint64_t target_free) {
    std::vector<std::string> evicted_paths;
    std::uint64_t freed = 0;
    {
        std::lock_guard<std::recursive_mutex> lk(g_eq_sidecar_mu);
        freed = eq_sidecar_evict_tail_locked(target_free, evicted_paths);
    }
    if (freed > 0) {
        newdb::memory_registry_release(newdb::MemoryKind::EqSidecar, freed);
        newdb::memory_registry_record_eviction(newdb::MemoryKind::EqSidecar, freed);
    }
    return freed;
}

void ensure_eq_sidecar_evictor_registered() {
    bool expected = false;
    if (g_eq_sidecar_evictor_registered.compare_exchange_strong(expected, true)) {
        newdb::memory_registry_register_evictor(newdb::MemoryKind::EqSidecar,
                                                &eq_sidecar_evictor_callback);
    }
}

bool eq_sidecar_cache_install(const std::string& path, EqSidecarCacheEntry entry) {
    ensure_eq_sidecar_evictor_registered();
    const std::uint64_t est = estimate_eq_entry_bytes(entry);
    std::uint64_t prior_bytes = 0;
    {
        std::lock_guard<std::recursive_mutex> lk(g_eq_sidecar_mu);
        prior_bytes = eq_sidecar_release_path_locked(path);
        const auto cit = g_eq_sidecar_cache.find(path);
        if (cit != g_eq_sidecar_cache.end()) {
            g_eq_sidecar_cache.erase(cit);
        }
    }
    if (prior_bytes > 0) {
        newdb::memory_registry_release(newdb::MemoryKind::EqSidecar, prior_bytes);
    }
    if (!newdb::memory_registry_try_admit(newdb::MemoryKind::EqSidecar, est)) {
        g_eq_sidecar_memory_budget_skips.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    std::lock_guard<std::recursive_mutex> lk(g_eq_sidecar_mu);
    g_eq_sidecar_cache[path] = std::move(entry);
    g_eq_sidecar_bytes[path] = est;
    eq_sidecar_lru_touch_front_locked(path);
    return true;
}

bool row_at_slot_read_eq(const newdb::HeapTable& tbl, const std::size_t i, newdb::Row& r) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(i, r);
    }
    if (i >= tbl.rows.size()) {
        return false;
    }
    r = tbl.rows[i];
    return true;
}

std::string sidecar_path_for(const EqIndexRequest& req) {
    return req.data_file + ".eqidx." + req.attr_name;
}

std::string bloom_path_for(const EqIndexRequest& req) {
    return req.data_file + ".eqbloom." + req.attr_name;
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

bool parse_header(const std::string& line, std::string& attr, std::uint64_t& data_sig, std::uint64_t& attr_sig, std::uint64_t& wal_lsn) {
    // attr=<name>;data_sig=<n>;attr_sig=<n>[;wal_lsn=<digits>][;idx_kind=...;built_ms=...][;buckets=<n>]
    const auto a0 = line.find("attr=");
    const auto s0 = line.find(";data_sig=");
    const auto t0 = line.find(";attr_sig=");
    if (a0 != 0 || s0 == std::string::npos || t0 == std::string::npos) {
        return false;
    }
    attr = line.substr(5, s0 - 5);
    const std::string::size_type wpos = line.find(";wal_lsn=");
    const std::string::size_type bpos = line.find(";buckets=");
    if (wpos == std::string::npos) {
        const std::string ds = line.substr(s0 + 10, t0 - (s0 + 10));
        const std::string attr_sig_s =
            (bpos == std::string::npos) ? line.substr(t0 + 10) : line.substr(t0 + 10, bpos - (t0 + 10));
        try {
            data_sig = static_cast<std::uint64_t>(std::stoull(ds));
            attr_sig = static_cast<std::uint64_t>(std::stoull(attr_sig_s));
        } catch (...) {
            return false;
        }
        wal_lsn = 0;
    } else {
        const std::string ds = line.substr(s0 + 10, t0 - (s0 + 10));
        const std::string as = line.substr(t0 + 10, wpos - (t0 + 10));
        try {
            data_sig = static_cast<std::uint64_t>(std::stoull(ds));
            attr_sig = static_cast<std::uint64_t>(std::stoull(as));
        } catch (...) {
            return false;
        }
        wal_lsn = 0;
        const std::size_t i0 = wpos + 9;
        for (std::size_t i = i0; i < line.size(); ++i) {
            const unsigned char c = static_cast<unsigned char>(line[i]);
            if (c < '0' || c > '9') {
                break;
            }
            wal_lsn = wal_lsn * 10 + static_cast<std::uint64_t>(c - '0');
        }
    }
    return true;
}

std::size_t eq_bucket_count() {
    const char* env = std::getenv("NEWDB_EQ_BUCKETS");
    if (env == nullptr) {
        return 1;
    }
    std::size_t v = 1;
    if (std::from_chars(env, env + std::strlen(env), v).ec == std::errc{} && v > 0) {
        return v;
    }
    return 1;
}

std::size_t bucket_for_key(const std::string& key, const std::size_t bucket_count) {
    if (bucket_count <= 1) {
        return 0;
    }
    return std::hash<std::string>{}(key) % bucket_count;
}

bool parse_entry(const std::string& line, std::string& key, std::vector<std::size_t>& slots) {
    // <len>:<key>\t<slot>,<slot>,...
    const auto colon = line.find(':');
    const auto tab = line.find('\t');
    if (colon == std::string::npos || tab == std::string::npos || tab <= colon + 1) {
        return false;
    }
    std::size_t klen = 0;
    try {
        klen = static_cast<std::size_t>(std::stoull(line.substr(0, colon)));
    } catch (...) {
        return false;
    }
    const std::size_t key_begin = colon + 1;
    if (key_begin + klen != tab) {
        return false;
    }
    key = line.substr(key_begin, klen);

    slots.clear();
    std::string rest = line.substr(tab + 1);
    std::stringstream ss(rest);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (token.empty()) {
            continue;
        }
        try {
            slots.push_back(static_cast<std::size_t>(std::stoull(token)));
        } catch (...) {
            return false;
        }
    }
    return true;
}

std::string eq_expected_table_plain(const EqIndexRequest& req) {
    if (!req.table_name.empty()) {
        return req.table_name;
    }
    return index_catalog_infer_table_plain_from_data_file(req.data_file);
}

void write_sidecar(const std::string& path,
                   const EqIndexRequest& req,
                   const std::uint64_t data_sig,
                   const std::uint64_t attr_sig,
                   const std::uint64_t wal_lsn,
                   const std::map<std::string, std::vector<std::size_t>>& buckets) {
    const std::size_t bucket_count = eq_bucket_count();
    const std::string tmp_path = path + ".tmp";
    std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
    if (!out) {
        return;
    }
    const std::string tbl_p = eq_expected_table_plain(req);
    out << "attr=" << req.attr_name
        << ";data_sig=" << data_sig
        << ";attr_sig=" << attr_sig
        << ";wal_lsn=" << wal_lsn
        << index_catalog_sidecar_meta_suffix(IndexKind::Eq, attr_sig, wal_lsn, req.data_file, req.attr_name, nullptr,
                                             tbl_p, req.attr_name)
        << ";buckets=" << bucket_count << "\n";
    for (const auto& kv : buckets) {
        if (bucket_count > 1) {
            out << bucket_for_key(kv.first, bucket_count) << "\t";
        }
        out << kv.first.size() << ":" << kv.first << "\t";
        for (std::size_t i = 0; i < kv.second.size(); ++i) {
            if (i > 0) {
                out << ",";
            }
            out << kv.second[i];
        }
        out << "\n";
    }
    out.flush();
    out.close();
    std::error_code ec;
    fs::remove(path, ec);
    fs::rename(tmp_path, path, ec);
}

bool try_lookup_sidecar(const std::string& path,
                        const EqIndexRequest& req,
                        const std::uint64_t data_sig,
                        const std::uint64_t attr_sig,
                        const std::uint64_t wal_lsn,
                        const std::string& value,
                        std::vector<std::size_t>& out,
                        WhereQueryContext* where_obs) {
    {
        // Optional bloom prefilter. No false negatives; can skip parsing sidecar on definite miss.
        std::uint32_t bits = 1u << 20;
        std::uint8_t k = 7;
        if (const char* eb = std::getenv("NEWDB_EQ_BLOOM_BITS")) {
            try {
                const std::uint32_t v = static_cast<std::uint32_t>(std::stoul(eb));
                if (v >= 1024) bits = v;
            } catch (...) {
            }
        }
        if (const char* ek = std::getenv("NEWDB_EQ_BLOOM_K")) {
            try {
                const unsigned v = static_cast<unsigned>(std::stoul(ek));
                if (v >= 1 && v <= 20) k = static_cast<std::uint8_t>(v);
            } catch (...) {
            }
        }
        EqBloomHeader expected{.data_sig = data_sig, .attr_sig = attr_sig, .wal_lsn = wal_lsn, .bits = bits, .k = k};
        bool may = true;
        if (eq_bloom_may_contain(bloom_path_for(req), expected, value, may) && !may) {
            out.clear();
            return true;
        }
    }
    {
        std::lock_guard<std::recursive_mutex> slk(g_eq_sidecar_mu);
        const auto cache_it = g_eq_sidecar_cache.find(path);
        if (cache_it != g_eq_sidecar_cache.end() &&
            cache_it->second.wal_lsn == wal_lsn &&
            cache_it->second.data_sig == data_sig &&
            cache_it->second.attr_sig == attr_sig) {
            const IndexCatalogParsedTail& ct = cache_it->second.catalog_tail;
            {
                const char* enf = std::getenv("NEWDB_INDEX_CATALOG_ENFORCE");
                const bool enforce = (enf != nullptr && std::strcmp(enf, "1") == 0);
                if (enforce && ct.catalog_build_state == 1) {
                    eq_sidecar_cache_erase(path);
                    std::error_code ec;
                    fs::remove(path, ec);
                    fs::remove(bloom_path_for(req), ec);
                    return false;
                }
            }
            const std::string exp_tbl = eq_expected_table_plain(req);
            const bool id_ok =
                cache_it->second.header_line.empty()
                    ? index_catalog_tail_identity_matches(ct, req.data_file, req.attr_name)
                    : index_catalog_header_identity_ok(cache_it->second.header_line, ct, req.data_file, req.attr_name,
                                                       exp_tbl, req.attr_name, IndexKind::Eq);
            if (!id_ok) {
                const char* enf = std::getenv("NEWDB_INDEX_CATALOG_ENFORCE");
                const bool enforce = (enf != nullptr && std::strcmp(enf, "1") == 0);
                if (enforce) {
                    eq_sidecar_cache_erase(path);
                    std::error_code ec;
                    fs::remove(path, ec);
                    fs::remove(bloom_path_for(req), ec);
                    return false;
                }
                std::fprintf(stderr,
                             "[NEWDB_INDEX_CATALOG] eq cache hit identity mismatch (enforce off) attr=%s data=%s\n",
                             req.attr_name.c_str(), req.data_file.c_str());
            }
            IndexDescriptor d{};
            d.index_name = req.attr_name;
            d.kind = IndexKind::Eq;
            d.data_lsn = ct.idx_dl != 0 ? ct.idx_dl : cache_it->second.wal_lsn;
            d.schema_version = ct.idx_sv != 0 ? ct.idx_sv : cache_it->second.attr_sig;
            d.built_at_ms = ct.built_ms;
            d.valid = true;
            if (!index_descriptor_matches_runtime(d, attr_sig, wal_lsn)) {
                const char* enf = std::getenv("NEWDB_INDEX_CATALOG_ENFORCE");
                const bool enforce = (enf != nullptr && std::strcmp(enf, "1") == 0);
                if (enforce) {
                    eq_sidecar_cache_erase(path);
                    std::error_code ec;
                    fs::remove(path, ec);
                    fs::remove(bloom_path_for(req), ec);
                    return false;
                }
                std::fprintf(stderr,
                             "[NEWDB_INDEX_CATALOG] eq cache hit catalog mismatch (enforce off) attr=%s data=%s\n",
                             req.attr_name.c_str(), req.data_file.c_str());
            }
            const auto it = cache_it->second.buckets.find(value);
            if (it == cache_it->second.buckets.end()) {
                out.clear();
            } else {
                out = it->second;
            }
            eq_sidecar_lru_touch_front_locked(path);
            return true;
        }
    }

    std::uint64_t disk_bytes_for_budget = 0;
    {
        std::error_code fsec;
        const auto fsz = fs::file_size(path, fsec);
        if (!fsec) {
            disk_bytes_for_budget = static_cast<std::uint64_t>(fsz);
            if (where_obs != nullptr) {
                where_obs->where_eq_sidecar_disk_bytes_read_total.fetch_add(disk_bytes_for_budget,
                                                                            std::memory_order_relaxed);
                where_obs->where_eq_sidecar_disk_loads.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
    {
        const std::uint64_t cap = newdb::memory_budget_max_bytes_env();
        if (cap > 0 && disk_bytes_for_budget > 0) {
            const auto snap = newdb::memory_budget_snapshot();
            if (snap.used_bytes + disk_bytes_for_budget > cap) {
                g_eq_sidecar_memory_budget_skips.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
        }
    }

    std::ifstream in(path, std::ios::in);
    if (!in) {
        return false;
    }
    std::string hdr;
    if (!std::getline(in, hdr)) {
        return false;
    }
    std::string attr;
    std::uint64_t ds = 0;
    std::uint64_t as = 0;
    std::uint64_t wln = 0;
    if (!parse_header(hdr, attr, ds, as, wln)) {
        return false;
    }
    if (attr != req.attr_name || ds != data_sig || as != attr_sig || wln != wal_lsn) {
        return false;
    }
    IndexCatalogParsedTail hdr_tail{};
    index_catalog_parse_header_tail(hdr, hdr_tail);
    const std::string exp_tbl_disk = eq_expected_table_plain(req);
    {
        const char* enf = std::getenv("NEWDB_INDEX_CATALOG_ENFORCE");
        const bool enforce = (enf != nullptr && std::strcmp(enf, "1") == 0);
        if (enforce && hdr_tail.catalog_build_state == 1) {
            std::error_code ec;
            fs::remove(path, ec);
            fs::remove(bloom_path_for(req), ec);
            return false;
        }
    }
    if (!index_catalog_header_identity_ok(hdr, hdr_tail, req.data_file, req.attr_name, exp_tbl_disk, req.attr_name,
                                          IndexKind::Eq)) {
        const char* enf = std::getenv("NEWDB_INDEX_CATALOG_ENFORCE");
        const bool enforce = (enf != nullptr && std::strcmp(enf, "1") == 0);
        if (enforce) {
            std::error_code ec;
            fs::remove(path, ec);
            fs::remove(bloom_path_for(req), ec);
            return false;
        }
        std::fprintf(stderr,
                     "[NEWDB_INDEX_CATALOG] eq disk hit identity mismatch (enforce off) attr=%s data=%s\n",
                     req.attr_name.c_str(), req.data_file.c_str());
    }
    {
        IndexDescriptor d{};
        d.index_name = req.attr_name;
        d.kind = IndexKind::Eq;
        d.data_lsn = hdr_tail.idx_dl != 0 ? hdr_tail.idx_dl : wln;
        d.schema_version = hdr_tail.idx_sv != 0 ? hdr_tail.idx_sv : as;
        d.built_at_ms = hdr_tail.built_ms;
        d.valid = true;
        if (!index_descriptor_matches_runtime(d, attr_sig, wal_lsn)) {
            const char* enf = std::getenv("NEWDB_INDEX_CATALOG_ENFORCE");
            const bool enforce = (enf != nullptr && std::strcmp(enf, "1") == 0);
            if (enforce) {
                eq_sidecar_cache_erase(path);
                std::error_code ec;
                fs::remove(path, ec);
                fs::remove(bloom_path_for(req), ec);
                return false;
            }
            std::fprintf(stderr,
                         "[NEWDB_INDEX_CATALOG] eq disk hit catalog mismatch (enforce off) attr=%s data=%s (%s)\n",
                         req.attr_name.c_str(), req.data_file.c_str(),
                         explain_index_descriptor_mismatch(d, attr_sig, wal_lsn).c_str());
        }
    }
    std::size_t bucket_count = 1;
    if (const std::string::size_type bpos = hdr.find(";buckets="); bpos != std::string::npos) {
        try {
            bucket_count = static_cast<std::size_t>(std::stoull(hdr.substr(bpos + 9)));
        } catch (...) {
            bucket_count = 1;
        }
        if (bucket_count == 0) {
            bucket_count = 1;
        }
    }
    const std::size_t target_bucket = bucket_for_key(value, bucket_count);

    EqSidecarCacheEntry parsed;
    parsed.data_sig = data_sig;
    parsed.attr_sig = attr_sig;
    parsed.wal_lsn = wal_lsn;
    parsed.header_line = hdr;
    parsed.catalog_tail = hdr_tail;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::string parse_line = line;
        if (bucket_count > 1) {
            const auto tab = line.find('\t');
            if (tab == std::string::npos) {
                return false;
            }
            std::size_t b = 0;
            try {
                b = static_cast<std::size_t>(std::stoull(line.substr(0, tab)));
            } catch (...) {
                return false;
            }
            if (b != target_bucket) {
                continue;
            }
            parse_line = line.substr(tab + 1);
        }
        std::string key;
        std::vector<std::size_t> slots;
        if (!parse_entry(parse_line, key, slots)) {
            return false;
        }
        parsed.buckets.emplace(std::move(key), std::move(slots));
    }

    const auto out_it = parsed.buckets.find(value);
    if (out_it == parsed.buckets.end()) {
        out.clear();
    } else {
        out = out_it->second;
    }
    (void)eq_sidecar_cache_install(path, std::move(parsed));
    return true;
}

} // namespace

EqLookupResult lookup_or_build_eq_index_sidecar(const EqIndexRequest& req,
                                                const newdb::TableSchema& schema,
                                                const newdb::HeapTable& table,
                                                const std::string& value,
                                                WhereQueryContext* where_obs) {
    (void)schema;
    const std::string attr_file = newdb::schema_sidecar_path_for_data_file(req.data_file);
    const std::uint64_t data_sig = file_sig(req.data_file);
    const std::uint64_t attr_sig = file_sig(attr_file);
    const std::uint64_t wal_lsn = read_wal_lsn_for_workspace(workspace_dir_for_data_file(req.data_file));
    const std::string sidecar = sidecar_path_for(req);

    std::vector<std::size_t> matched;
    if (try_lookup_sidecar(sidecar, req, data_sig, attr_sig, wal_lsn, value, matched, where_obs)) {
        EqLookupResult out;
        out.used_index = true;
        out.slots = std::move(matched);
        return out;
    }

    std::map<std::string, std::vector<std::size_t>> buckets;
    const std::vector<std::size_t> visible_slots =
        load_or_build_visibility_checkpoint_sidecar(req.data_file, schema, table);
    newdb::Row row;
    for (const std::size_t i : visible_slots) {
        if (!row_at_slot_read_eq(table, i, row)) {
            continue;
        }
        std::string key;
        if (req.attr_name == "id") {
            key = std::to_string(row.id);
        } else {
            const auto it = row.attrs.find(req.attr_name);
            if (it == row.attrs.end()) {
                continue;
            }
            key = it->second;
        }
        buckets[key].push_back(i);
    }

    write_sidecar(sidecar, req, data_sig, attr_sig, wal_lsn, buckets);
    {
        std::uint32_t bits = 1u << 20;
        std::uint8_t k = 7;
        if (const char* eb = std::getenv("NEWDB_EQ_BLOOM_BITS")) {
            try {
                const std::uint32_t v = static_cast<std::uint32_t>(std::stoul(eb));
                if (v >= 1024) bits = v;
            } catch (...) {
            }
        }
        if (const char* ek = std::getenv("NEWDB_EQ_BLOOM_K")) {
            try {
                const unsigned v = static_cast<unsigned>(std::stoul(ek));
                if (v >= 1 && v <= 20) k = static_cast<std::uint8_t>(v);
            } catch (...) {
            }
        }
        std::vector<std::string> keys;
        keys.reserve(buckets.size());
        for (const auto& kv : buckets) {
            keys.push_back(kv.first);
        }
        eq_bloom_write(bloom_path_for(req),
                       EqBloomHeader{.data_sig = data_sig, .attr_sig = attr_sig, .wal_lsn = wal_lsn, .bits = bits, .k = k},
                       keys);
    }
    IndexCatalogParsedTail wt{};
    std::string lh_store;
    {
        std::ifstream hdr_in(sidecar, std::ios::in);
        if (hdr_in && std::getline(hdr_in, lh_store)) {
            index_catalog_parse_header_tail(lh_store, wt);
        }
    }
    const auto built_it = buckets.find(value);
    std::vector<std::size_t> built_slots =
        (built_it != buckets.end()) ? built_it->second : std::vector<std::size_t>{};
    (void)eq_sidecar_cache_install(
        sidecar, EqSidecarCacheEntry{data_sig, attr_sig, wal_lsn, std::move(lh_store), wt, std::move(buckets)});
    EqLookupResult out;
    out.used_index = true;
    out.slots = std::move(built_slots);
    return out;
}

std::uint64_t eq_sidecar_invalidate_remove_fail_count() {
    return g_eq_sidecar_invalidate_remove_fails.load(std::memory_order_relaxed);
}

std::uint64_t eq_sidecar_memory_budget_skip_count() {
    return g_eq_sidecar_memory_budget_skips.load(std::memory_order_relaxed);
}

void invalidate_eq_index_sidecars_for_data_file(const std::string& data_file) {
    if (data_file.empty()) {
        return;
    }
    std::vector<std::string> drop_paths;
    {
        std::lock_guard<std::recursive_mutex> slk(g_eq_sidecar_mu);
        const std::string prefix = data_file + ".eqidx.";
        for (const auto& kv : g_eq_sidecar_cache) {
            if (kv.first.rfind(prefix, 0) == 0) {
                drop_paths.push_back(kv.first);
            }
        }
    }
    for (const std::string& p : drop_paths) {
        eq_sidecar_cache_erase(p);
    }

    std::error_code ec;
    newdb::TableSchema schema;
    (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(data_file), schema);
    std::size_t removed = 0;
    for (const auto& attr : schema.attrs) {
        ec.clear();
        fs::remove(data_file + ".eqidx." + attr.name, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            g_eq_sidecar_invalidate_remove_fails.fetch_add(1, std::memory_order_relaxed);
        } else if (!ec) {
            ++removed;
        }
        ec.clear();
        fs::remove(data_file + ".eqbloom." + attr.name, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            g_eq_sidecar_invalidate_remove_fails.fetch_add(1, std::memory_order_relaxed);
        } else if (!ec) {
            ++removed;
        }
        ec.clear();
    }
    const char* v = std::getenv("NEWDB_SIDECAR_INVALIDATE_VERBOSE");
    const bool verbose = (v == nullptr) ? true : (std::string(v) != "0");
    if (verbose) {
        std::printf("[SIDECAR_INVALIDATE] eq data=%s removed=%zu\n", data_file.c_str(), removed);
    }
}

void invalidate_eq_index_sidecars_for_attrs(const std::string& data_file,
                                            const std::set<std::string>& attr_names) {
    if (data_file.empty() || attr_names.empty()) {
        return;
    }
    for (const auto& attr : attr_names) {
        if (attr.empty()) {
            continue;
        }
        const std::string sidecar = data_file + ".eqidx." + attr;
        const std::string bloom = data_file + ".eqbloom." + attr;
        eq_sidecar_cache_erase(sidecar);
        std::error_code ec;
        fs::remove(sidecar, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            g_eq_sidecar_invalidate_remove_fails.fetch_add(1, std::memory_order_relaxed);
        }
        ec.clear();
        fs::remove(bloom, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            g_eq_sidecar_invalidate_remove_fails.fetch_add(1, std::memory_order_relaxed);
        }
    }
}
