#pragma once

#include <newdb/session.h>
#include <newdb/schema_io.h>

#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/where/executor/where.h"

#include <filesystem>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <unordered_map>
#include <vector>
#include <string>

// Single place for interactive / --exec state (no globals).
struct ShellState {
    enum class BenchmarkProfile : std::uint8_t {
        NewdbDefault = 0,
        LeveldbLike = 1,
        InnodbLike = 2,
        HybridBalanced = 3,
    };
    struct RuntimePolicy {
        BenchmarkProfile profile{BenchmarkProfile::NewdbDefault};
        bool initialized{false};
    };
    struct LsmEntry {
        newdb::Row row;
        bool deleted{false};
        std::uint64_t seq{0};
    };
    struct LsmSegmentMeta {
        std::string path;
        std::uint32_t level{0};
        int min_key{0};
        int max_key{0};
        std::uint64_t entry_count{0};
        std::uint64_t max_seq{0};
    };

    newdb::Session session;
    // When set, `get_cached_table` reuses this guard for the rest of the current command line
    // (see `process_command_line` RAII). Keeps `Session::mut_` held so `HeapTable*` stays valid.
    std::optional<newdb::Session::HeapAccess> session_heap_guard;
    TxnCoordinator txn;
    std::string log_file_path{"demo_log.bin"};

    // When non-empty, table files (.bin) and schema sidecars are resolved under this directory.
    std::string data_dir;

    // Mirror log/printf to this FD (e.g. socket). -1 = disabled. Linux send() only.
    int mirror_output_fd{-1};

    // When true, session log file uses legacy XOR-framed records; default is plain UTF-8 lines.
    bool encrypt_log{false};

    // Extra diagnostics to stderr (paths, load errors).
    bool verbose{false};
    WhereQueryContext where_ctx;

    // Shell-local LSM-lite caches and sidecar invalidation tuning (see txn + lsm_lite_service).
    struct LsmShellCache {
        // Stage 1: process-local hot index for recently written rows (best-effort).
        std::unordered_map<int, newdb::Row> hot_index_recent;
        // Stage 2: mutable/immutable memtable and segment catalog.
        std::unordered_map<int, LsmEntry> lsm_memtable;
        std::unordered_map<int, LsmEntry> lsm_immutable;
        std::vector<LsmSegmentMeta> lsm_segments;
        std::uint64_t lsm_memtable_bytes{0};
        std::uint64_t lsm_seq{0};
        std::string lsm_table_name;
    } lsm;

    struct SidecarShellTuning {
        // Sidecar invalidation coalescing for write-heavy benchmarks (every_n=1 = per write).
        std::uint64_t sidecar_pending_writes{0};
        std::uint64_t sidecar_invalidate_every_n{0};
        // 0=uninitialized, 1=sync(default), 2=async background.
        std::uint8_t sidecar_invalidate_mode{0};
    } sidecar;

    RuntimePolicy runtime_policy{};
};

inline std::string resolve_table_file(const ShellState& st, const std::string& rel_or_abs) {
    if (rel_or_abs.empty()) {
        return {};
    }
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path p(rel_or_abs);
    if (p.is_absolute()) {
        const fs::path c = fs::weakly_canonical(p, ec);
        return ec ? p.string() : c.string();
    }
    if (!st.data_dir.empty()) {
        const fs::path base = fs::absolute(st.data_dir, ec);
        return (base / p).lexically_normal().string();
    }
    const fs::path c = fs::absolute(p, ec);
    return ec ? (fs::current_path(ec) / p).lexically_normal().string() : c.string();
}

inline std::string effective_data_path(const ShellState& st) {
    return resolve_table_file(st, st.session.data_path);
}

inline std::filesystem::path workspace_directory(const ShellState& st) {
    namespace fs = std::filesystem;
    std::error_code ec;
    if (!st.data_dir.empty()) {
        return fs::absolute(st.data_dir, ec);
    }
    return fs::current_path(ec);
}

inline newdb::HeapTable* get_cached_table(ShellState& st) {
    if (!st.session_heap_guard.has_value() || !st.session_heap_guard.value()) {
        st.session_heap_guard.emplace(st.session.lock_heap(st.log_file_path.c_str()));
    }
    newdb::Session::HeapAccess& acc = st.session_heap_guard.value();
    return acc ? &st.session.table : nullptr;
}

// Call before `Session::invalidate()` so the session mutex is not held by `HeapAccess` (deadlock).
inline void shell_invalidate_session_table(ShellState& st) {
    st.session_heap_guard.reset();
    st.session.invalidate();
}

// Expands mmap-backed heap into `HeapTable::rows` (required before mutating `rows` in memory).
// Avoid on large tables except where mutation or APIs explicitly require a full vector: it forces
// a full read and can dominate memory. Prefer lazy heap paths (PAGE, indexed WHERE) when possible;
// tune `NEWDB_LAZY_MATERIALIZE_WARN_ROWS` (env) to surface accidental full materialization earlier.
// When `stats_sink` is non-null, successful materialization updates `TxnRuntimeStats` lazy counters.
inline newdb::Status newdb_materialize_heap_if_lazy(newdb::HeapTable& t,
                                                    const newdb::TableSchema& sch,
                                                    ShellState* stats_sink = nullptr) {
    if (!t.is_heap_storage_backed()) {
        return newdb::Status::Ok();
    }
    const std::size_t logical_rows = t.logical_row_count();
    std::size_t warn_rows = 10000;
    if (const char* env = std::getenv("NEWDB_LAZY_MATERIALIZE_WARN_ROWS")) {
        try {
            const std::size_t v = static_cast<std::size_t>(std::stoull(env));
            if (v > 0) {
                warn_rows = v;
            }
        } catch (...) {
        }
    }
    if (logical_rows >= warn_rows) {
        std::fprintf(stderr,
                     "[LAZY_MATERIALIZE] forcing full materialization rows=%zu (warn_rows=%zu). "
                     "Prefer PAGE/indexed WHERE/streaming reads for large tables.\n",
                     logical_rows,
                     warn_rows);
    }
    const auto t0 = std::chrono::steady_clock::now();
    const newdb::Status st = t.materialize_all_rows(sch);
    if (st.ok && stats_sink != nullptr) {
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
        stats_sink->txn.noteLazyMaterialize(
            static_cast<std::uint64_t>(logical_rows),
            static_cast<std::uint64_t>(ms < 0 ? 0 : ms));
    }
    return st;
}

inline void reload_schema_from_data_path(ShellState& st, const std::string& data_path) {
    st.session_heap_guard.reset();
    st.session.data_path = resolve_table_file(st, data_path);
    (void)newdb::load_schema_text(newdb::schema_sidecar_path_for_data_file(st.session.data_path),
                                   st.session.schema);
    st.session.invalidate();
}

inline const newdb::AttrMeta* find_attr_meta(const newdb::TableSchema& sch, const std::string& name) {
    return sch.find_attr(name);
}

struct HeapReadViewGuard {
    ShellState& st;
    newdb::HeapTable& tbl;
    HeapReadViewGuard(ShellState& s, newdb::HeapTable& t) : st(s), tbl(t) { st.txn.syncHeapReadSnapshotForQuery(tbl); }
    ~HeapReadViewGuard() { tbl.clear_snapshot(); }
};
