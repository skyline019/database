#include <waterfall/config.h>

#include "cli/shell/state/shell_state_facade.h"

#include "cli/modules/common/logging/logging.h"
#include "cli/shell/state/shell_state.h"
#include "cli/shell/state/shell_state_paths.h"

ShellStateFacade::ShellStateFacade(ShellState& shell) noexcept : st(shell) {}

TxnCoordinator& ShellStateFacade::txn() noexcept {
    return st.txn();
}

const TxnCoordinator& ShellStateFacade::txn() const noexcept {
    return st.txn();
}

WhereQueryContext& ShellStateFacade::where() noexcept {
    return st.where_ctx();
}

const WhereQueryContext& ShellStateFacade::where() const noexcept {
    return st.where_ctx();
}

newdb::Session& ShellStateFacade::session() noexcept {
    return st.session();
}

const newdb::Session& ShellStateFacade::session() const noexcept {
    return st.session();
}

std::string& ShellStateFacade::table_name() noexcept {
    return st.session_table_name();
}

const std::string& ShellStateFacade::table_name() const noexcept {
    return st.session_table_name();
}

std::string& ShellStateFacade::data_path() noexcept {
    return st.session_data_path();
}

const std::string& ShellStateFacade::data_path() const noexcept {
    return st.session_data_path();
}

newdb::TableSchema& ShellStateFacade::schema() noexcept {
    return st.session_schema();
}

const newdb::TableSchema& ShellStateFacade::schema() const noexcept {
    return st.session_schema();
}

newdb::HeapTable& ShellStateFacade::heap_table() noexcept {
    return st.session_heap_table();
}

const newdb::HeapTable& ShellStateFacade::heap_table() const noexcept {
    return st.session_heap_table();
}

std::uint64_t ShellStateFacade::heap_decode_slot_calls() const noexcept {
    return st.heap_decode_slot_calls();
}

std::uint64_t ShellStateFacade::heap_decode_slot_hits() const noexcept {
    return st.heap_decode_slot_hits();
}

std::uint64_t ShellStateFacade::heap_decode_slot_misses() const noexcept {
    return st.heap_decode_slot_misses();
}

LsmShellCache& ShellStateFacade::lsm() noexcept {
    return st.lsm();
}

SidecarShellTuning& ShellStateFacade::sidecar_tuning() noexcept {
    return st.sidecar();
}

std::string& ShellStateFacade::log_file_path() noexcept {
    return st.log_file_path();
}

const std::string& ShellStateFacade::log_file_path() const noexcept {
    return st.log_file_path();
}

std::string& ShellStateFacade::data_dir() noexcept {
    return st.data_dir();
}

const std::string& ShellStateFacade::data_dir() const noexcept {
    return st.data_dir();
}

bool ShellStateFacade::verbose() const noexcept {
    return st.verbose();
}

int ShellStateFacade::mirror_output_fd() const noexcept {
    return st.mirror_output_fd();
}

ShellRuntimePolicy& ShellStateFacade::runtime_policy() noexcept {
    return st.runtime_policy();
}

const ShellRuntimePolicy& ShellStateFacade::runtime_policy() const noexcept {
    return st.runtime_policy();
}

void ShellStateFacade::bind_logging() const noexcept {
    logging_bind_shell(&st);
}

std::string ShellStateFacade::effective_data_path() const {
    return effective_data_path_paths(shell_paths_view(st));
}

std::filesystem::path ShellStateFacade::workspace_directory() const {
    return workspace_directory_paths(shell_paths_view(st));
}

std::string ShellStateFacade::resolve_table_file(const std::string& rel_or_abs) const {
    return resolve_table_file_paths(shell_paths_view(st), rel_or_abs);
}

void ShellStateFacade::reset_session_heap_guard() noexcept {
    st.reset_session_heap_guard();
}

bool ShellStateFacade::encrypt_log() const noexcept {
    return st.encrypt_log();
}

bool& ShellStateFacade::encrypt_log() noexcept {
    return st.encrypt_log();
}

newdb::Status ShellStateFacade::ensure_loaded() {
    return st.session_ensure_loaded();
}

void ShellStateFacade::invalidate_session() {
    st.session_invalidate();
}

bool ShellStateFacade::emit_where_plan_json(const char* log_path,
                                            const WhereCond* conds,
                                            const std::size_t cond_count,
                                            std::string* out_json) {
    return st.emit_where_plan_json(log_path, conds, cond_count, out_json);
}

void logging_bind_shell(const ShellState* st) {
    if (st == nullptr) {
        logging_bind_sink(nullptr);
        return;
    }
    ShellState& m = *const_cast<ShellState*>(st);
    LoggingShellSink sink{};
    sink.mirror_output_fd = m.mirror_output_fd();
    sink.encrypt_log = m.encrypt_log();
    logging_bind_sink(&sink);
}
