#pragma once

#include <newdb/error.h>
#include <newdb/heap_table.h>
#include <newdb/mvcc.h>
#include <newdb/schema.h>

#include <mutex>
#include <optional>
#include <string>

namespace newdb {

// Explicit session context: no hidden globals. Owns paths + in-memory state.
//
// Threading: a single `std::mutex` protects `cache_valid`, schema reload side-effects, and the
// in-memory `table` snapshot. `mutable_heap()` only holds that mutex for the duration of the call
// (returned `HeapTable*` is unsafe across concurrent `reload` / `reset_memory`). For multi-threaded
// use, prefer `lock_heap()` which keeps the mutex until the returned `HeapAccess` object is
// destroyed; release any `HeapAccess` before calling `invalidate()` / `reload()` on the same
// `Session` from the same thread to avoid deadlock.
struct Session {
    class HeapAccess {
        friend struct Session; // lock_heap() packs the held mutex into `lock_`.

        std::optional<std::unique_lock<std::mutex>> lock_{};
        Session* session_{nullptr};
        bool ok_{false};

    public:
        HeapAccess() = default;
        HeapAccess(HeapAccess&& o) noexcept = default;
        HeapAccess& operator=(HeapAccess&& o) noexcept = default;
        HeapAccess(const HeapAccess&) = delete;
        HeapAccess& operator=(const HeapAccess&) = delete;
        ~HeapAccess();

        explicit operator bool() const noexcept { return ok_ && session_ != nullptr; }

        [[nodiscard]] HeapTable* table() noexcept { return (*this) ? &session_->table : nullptr; }
        [[nodiscard]] const HeapTable* table() const noexcept {
            return (*this) ? &session_->table : nullptr;
        }
        [[nodiscard]] Session& session() noexcept { return *session_; }
        [[nodiscard]] const Session& session() const noexcept { return *session_; }
    };

    std::string data_path;   // e.g. users.bin
    std::string table_name;  // logical table name (stem)
    TableSchema schema;
    HeapTable table;
    bool cache_valid{false};

    mutable std::mutex mut_;

    void reset_memory();

    void invalidate();

    // Load sidecar schema (plain .attr) then merge heap file into `table`.
    Status reload();

    Status ensure_loaded();
    void set_snapshot(const MVCCSnapshot& snapshot);
    void clear_snapshot();

    // Keeps `mut_` locked until the `HeapAccess` is destroyed (or moved-from). Safe to use the
    // returned `table()` pointer only while this object is alive and no other thread can run
    // `reload` / `reset_memory` / `invalidate` / `ensure_loaded` on the same `Session`.
    [[nodiscard]] HeapAccess lock_heap(const char* log_file);

    // Loads if needed, returns `&table` only while `mut_` is held — pointer invalid after return
    // if another thread mutates the session. Prefer `lock_heap` when sharing across threads.
    HeapTable* mutable_heap(const char* log_file);
};

} // namespace newdb
