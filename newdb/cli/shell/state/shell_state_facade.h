#pragma once

#include <newdb/error.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>

#include "cli/shell/state/shell_state_benchmark.h"
#include "cli/shell/state/shell_state_fwd.h"

class TxnCoordinator;
struct WhereCond;
struct WhereQueryContext;
struct LsmShellCache;
struct SidecarShellTuning;

namespace newdb {
struct Session;
struct TableSchema;
struct HeapTable;
} // namespace newdb

/// Non-owning view over `ShellState` for handlers that should depend on a narrow surface.
/// Implementation is in `shell_state_facade.cc` so this header avoids pulling `txn_manager.h` / `where.h`.
struct ShellStateFacade {
    ShellState& st;

    explicit ShellStateFacade(ShellState& shell) noexcept;

    [[nodiscard]] TxnCoordinator& txn() noexcept;
    [[nodiscard]] const TxnCoordinator& txn() const noexcept;
    [[nodiscard]] WhereQueryContext& where() noexcept;
    [[nodiscard]] const WhereQueryContext& where() const noexcept;
    [[nodiscard]] newdb::Session& session() noexcept;
    [[nodiscard]] const newdb::Session& session() const noexcept;

    [[nodiscard]] std::string& table_name() noexcept;
    [[nodiscard]] const std::string& table_name() const noexcept;
    [[nodiscard]] std::string& data_path() noexcept;
    [[nodiscard]] const std::string& data_path() const noexcept;
    [[nodiscard]] newdb::TableSchema& schema() noexcept;
    [[nodiscard]] const newdb::TableSchema& schema() const noexcept;
    [[nodiscard]] newdb::HeapTable& heap_table() noexcept;
    [[nodiscard]] const newdb::HeapTable& heap_table() const noexcept;

    /// Heap decode counters (avoids `#include <newdb/session.h>` in stats JSON builders).
    [[nodiscard]] std::uint64_t heap_decode_slot_calls() const noexcept;
    [[nodiscard]] std::uint64_t heap_decode_slot_hits() const noexcept;
    [[nodiscard]] std::uint64_t heap_decode_slot_misses() const noexcept;

    [[nodiscard]] LsmShellCache& lsm() noexcept;
    [[nodiscard]] SidecarShellTuning& sidecar_tuning() noexcept;
    [[nodiscard]] std::string& log_file_path() noexcept;
    [[nodiscard]] const std::string& log_file_path() const noexcept;
    [[nodiscard]] std::string& data_dir() noexcept;
    [[nodiscard]] const std::string& data_dir() const noexcept;
    [[nodiscard]] bool verbose() const noexcept;
    [[nodiscard]] int mirror_output_fd() const noexcept;

    [[nodiscard]] ShellRuntimePolicy& runtime_policy() noexcept;
    [[nodiscard]] const ShellRuntimePolicy& runtime_policy() const noexcept;

    /// Mirror FD + XOR log framing for the active shell (thin wrapper over `logging_bind_shell`).
    void bind_logging() const noexcept;

    /// Same as free `effective_data_path(const ShellState&)` implemented beside `ShellState` in `shell_state.cc`.
    [[nodiscard]] std::string effective_data_path() const;

    [[nodiscard]] std::filesystem::path workspace_directory() const;

    [[nodiscard]] std::string resolve_table_file(const std::string& rel_or_abs) const;

    void reset_session_heap_guard() noexcept;

    [[nodiscard]] bool encrypt_log() const noexcept;
    [[nodiscard]] bool& encrypt_log() noexcept;

    [[nodiscard]] newdb::Status ensure_loaded();
    void invalidate_session();

    /// Serialize WHERE plan candidates to JSON (`log_path` is passed to `Session::lock_heap`).
    /// `conds` may be null only when `cond_count` is 0.
    bool emit_where_plan_json(const char* log_path,
                                const WhereCond* conds,
                                std::size_t cond_count,
                                std::string* out_json);
};
