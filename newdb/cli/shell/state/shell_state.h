#pragma once

#include "cli/shell/state/shell_state_benchmark.h"
#include "cli/shell/state/shell_state_paths.h"

#include <cstdint>
#include <filesystem>
#include <newdb/engine_session_opaque.h>
#include <newdb/error.h>

#include <memory>
#include <string>

namespace newdb {
struct Session;
struct HeapTable;
struct TableSchema;
} // namespace newdb

struct WhereCond;

class TxnCoordinator;
struct WhereQueryContext;
struct ShellTxnWhereRuntime;
struct ShellLsmSidecarRuntime;
struct LsmShellCache;
struct SidecarShellTuning;

/// Tag for [`ShellState`](shell_state.h) that borrows `newdb::Session` from [`newdb_engine_session_t`](../../../engine/include/newdb/engine_session_handle.h) (single owner: engine handle; full C API path).
struct ShellStateEngineBorrowedTag {
    explicit ShellStateEngineBorrowedTag() = default;
};

/// Single place for interactive / --exec state (no globals). Implementation details
/// live in `ShellState::Impl` ([`shell_state_impl.h`](shell_state_impl.h)) to keep this
/// header free of `<newdb/session.h>` and heavy runtime includes.
class ShellState {
    struct Impl;
    std::unique_ptr<Impl> impl_;

public:
    /// Owns an embedded [`Session`](../../engine/include/newdb/session.h); production paths should prefer
    /// [`ShellStateEngineBorrowedTag`](shell_state.h) + [`newdb_engine_session_t`](../../engine/include/newdb/engine_session_handle.h).
    ShellState();
    explicit ShellState(ShellStateEngineBorrowedTag, newdb_engine_session_t* engine_host);
    ~ShellState();
    ShellState(ShellState&&) noexcept;
    ShellState& operator=(ShellState&&) noexcept;
    ShellState(const ShellState&) = delete;
    ShellState& operator=(const ShellState&) = delete;

    [[nodiscard]] newdb::Session& session() noexcept;
    [[nodiscard]] const newdb::Session& session() const noexcept;

    void reset_session_heap_guard() noexcept;

    /// Next internal savepoint suffix (`__newdb_stmt_<n>`) for batch DML under an active txn.
    [[nodiscard]] std::uint64_t bump_txn_stmt_savepoint_seq() noexcept;

    TxnCoordinator& txn();
    [[nodiscard]] const TxnCoordinator& txn() const;
    WhereQueryContext& where_ctx();
    [[nodiscard]] const WhereQueryContext& where_ctx() const;

    LsmShellCache& lsm();
    [[nodiscard]] const LsmShellCache& lsm() const;
    SidecarShellTuning& sidecar();
    [[nodiscard]] const SidecarShellTuning& sidecar() const;

    [[nodiscard]] std::string& log_file_path() noexcept;
    [[nodiscard]] const std::string& log_file_path() const noexcept;
    [[nodiscard]] std::string& data_dir() noexcept;
    [[nodiscard]] const std::string& data_dir() const noexcept;

    [[nodiscard]] int& mirror_output_fd() noexcept;
    [[nodiscard]] int mirror_output_fd() const noexcept;

    [[nodiscard]] bool& encrypt_log() noexcept;
    [[nodiscard]] bool encrypt_log() const noexcept;

    [[nodiscard]] bool& verbose() noexcept;
    [[nodiscard]] bool verbose() const noexcept;

    [[nodiscard]] ShellRuntimePolicy& runtime_policy() noexcept;
    [[nodiscard]] const ShellRuntimePolicy& runtime_policy() const noexcept;

    /// Heap decode counters for runtime JSON (`const ShellState&` callers need not include `session.h`).
    [[nodiscard]] std::uint64_t heap_decode_slot_calls() const noexcept;
    [[nodiscard]] std::uint64_t heap_decode_slot_hits() const noexcept;
    [[nodiscard]] std::uint64_t heap_decode_slot_misses() const noexcept;

    /// Narrow session field access (implemented in `shell_state.cc`; avoids `#include <newdb/session.h>` in `shell_state_facade.cc`).
    [[nodiscard]] std::string& session_table_name() noexcept;
    [[nodiscard]] const std::string& session_table_name() const noexcept;
    [[nodiscard]] std::string& session_data_path() noexcept;
    [[nodiscard]] const std::string& session_data_path() const noexcept;
    [[nodiscard]] newdb::TableSchema& session_schema() noexcept;
    [[nodiscard]] const newdb::TableSchema& session_schema() const noexcept;
    [[nodiscard]] newdb::HeapTable& session_heap_table() noexcept;
    [[nodiscard]] const newdb::HeapTable& session_heap_table() const noexcept;

    [[nodiscard]] newdb::Status session_ensure_loaded();
    void session_invalidate();

    /// WHERE plan JSON for C API / `SHOW PLAN` (`WhereCond` is from the where module).
    [[nodiscard]] bool emit_where_plan_json(const char* log_path,
                                            const WhereCond* conds,
                                            std::size_t cond_count,
                                            std::string* out_json);

    friend newdb::HeapTable* get_cached_table(ShellState& st);
    friend void shell_invalidate_session_table(ShellState& st);
    friend void reload_schema_from_data_path(ShellState& st, const std::string& data_path);
};

ShellStatePathsView shell_paths_view(const ShellState& st) noexcept;
std::string resolve_table_file(const ShellState& st, const std::string& rel_or_abs);
std::string effective_data_path(const ShellState& st);
std::filesystem::path workspace_directory(const ShellState& st);
