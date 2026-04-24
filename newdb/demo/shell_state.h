#pragma once

#include <newdb/session.h>
#include <newdb/schema_io.h>

#include "txn_manager.h"
#include "where.h"

#include <filesystem>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>

// Single place for interactive / --exec state (no globals).
struct ShellState {
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
// a full read and can dominate memory. Prefer lazy heap paths (PAGE, indexed WHERE) when possible.
inline newdb::Status newdb_materialize_heap_if_lazy(newdb::HeapTable& t, const newdb::TableSchema& sch) {
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
    return t.materialize_all_rows(sch);
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
