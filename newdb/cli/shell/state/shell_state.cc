#include <waterfall/config.h>

#include <newdb/engine_session_access.h>
#include <newdb/engine_session_handle.h>

#include <filesystem>
#include <string>

#include "cli/shell/state/shell_state.h"
#include "cli/shell/state/shell_state_heap_guard_internal.h"
#include "cli/shell/state/shell_state_impl.h"
#include "cli/shell/state/shell_state_owner.h"

ShellState::ShellState()
    : impl_(std::make_unique<Impl>()) {
    impl_->session_ = std::make_unique<newdb::Session>();
    impl_->heap_guard_box_ = std::make_unique<shell_state_detail::HeapGuardBox>();
    impl_->txn_where_ = std::make_unique<ShellTxnWhereRuntime>();
    impl_->lsm_sidecar_ = std::make_unique<ShellLsmSidecarRuntime>();
}

ShellState::ShellState(ShellStateEngineBorrowedTag, newdb_engine_session_t* engine_host)
    : impl_(std::make_unique<Impl>()) {
    impl_->engine_session_borrow_ = engine_host;
    impl_->session_.reset();
    impl_->heap_guard_box_ = std::make_unique<shell_state_detail::HeapGuardBox>();
    impl_->txn_where_ = std::make_unique<ShellTxnWhereRuntime>();
    impl_->lsm_sidecar_ = std::make_unique<ShellLsmSidecarRuntime>();
}

ShellState::~ShellState() = default;

ShellState::ShellState(ShellState&&) noexcept = default;

ShellState& ShellState::operator=(ShellState&&) noexcept = default;

newdb::Session& ShellState::session() noexcept {
    if (impl_->engine_session_borrow_ != nullptr) {
        return *newdb::engine_session_borrow_cpp_session(impl_->engine_session_borrow_);
    }
    return *impl_->session_;
}

const newdb::Session& ShellState::session() const noexcept {
    if (impl_->engine_session_borrow_ != nullptr) {
        return *newdb::engine_session_borrow_cpp_session(impl_->engine_session_borrow_);
    }
    return *impl_->session_;
}

std::string& ShellState::session_table_name() noexcept {
    return session().table_name;
}

const std::string& ShellState::session_table_name() const noexcept {
    return session().table_name;
}

std::string& ShellState::session_data_path() noexcept {
    return session().data_path;
}

const std::string& ShellState::session_data_path() const noexcept {
    return session().data_path;
}

newdb::TableSchema& ShellState::session_schema() noexcept {
    return session().schema;
}

const newdb::TableSchema& ShellState::session_schema() const noexcept {
    return session().schema;
}

newdb::HeapTable& ShellState::session_heap_table() noexcept {
    return session().table;
}

const newdb::HeapTable& ShellState::session_heap_table() const noexcept {
    return session().table;
}

newdb::Status ShellState::session_ensure_loaded() {
    return session().ensure_loaded();
}

void ShellState::session_invalidate() {
    session().invalidate();
}

void ShellState::reset_session_heap_guard() noexcept {
    if (impl_->heap_guard_box_) {
        impl_->heap_guard_box_->session_heap_guard.reset();
    }
}

std::uint64_t ShellState::bump_txn_stmt_savepoint_seq() noexcept {
    return ++impl_->txn_stmt_savepoint_seq;
}

TxnCoordinator& ShellState::txn() {
    return impl_->txn_where_->bundle.txn;
}

const TxnCoordinator& ShellState::txn() const {
    return impl_->txn_where_->bundle.txn;
}

WhereQueryContext& ShellState::where_ctx() {
    return impl_->txn_where_->bundle.where_ctx;
}

const WhereQueryContext& ShellState::where_ctx() const {
    return impl_->txn_where_->bundle.where_ctx;
}

LsmShellCache& ShellState::lsm() {
    return impl_->lsm_sidecar_->lsm;
}

const LsmShellCache& ShellState::lsm() const {
    return impl_->lsm_sidecar_->lsm;
}

SidecarShellTuning& ShellState::sidecar() {
    return impl_->lsm_sidecar_->sidecar;
}

const SidecarShellTuning& ShellState::sidecar() const {
    return impl_->lsm_sidecar_->sidecar;
}

std::string& ShellState::log_file_path() noexcept {
    return impl_->paths_and_io_.log_file_path;
}

const std::string& ShellState::log_file_path() const noexcept {
    return impl_->paths_and_io_.log_file_path;
}

std::string& ShellState::data_dir() noexcept {
    return impl_->paths_and_io_.data_dir;
}

const std::string& ShellState::data_dir() const noexcept {
    return impl_->paths_and_io_.data_dir;
}

int& ShellState::mirror_output_fd() noexcept {
    return impl_->paths_and_io_.mirror_output_fd;
}

int ShellState::mirror_output_fd() const noexcept {
    return impl_->paths_and_io_.mirror_output_fd;
}

bool& ShellState::encrypt_log() noexcept {
    return impl_->paths_and_io_.encrypt_log;
}

bool ShellState::encrypt_log() const noexcept {
    return impl_->paths_and_io_.encrypt_log;
}

bool& ShellState::verbose() noexcept {
    return impl_->paths_and_io_.verbose;
}

bool ShellState::verbose() const noexcept {
    return impl_->paths_and_io_.verbose;
}

ShellRuntimePolicy& ShellState::runtime_policy() noexcept {
    return impl_->paths_and_io_.runtime_policy;
}

const ShellRuntimePolicy& ShellState::runtime_policy() const noexcept {
    return impl_->paths_and_io_.runtime_policy;
}

std::uint64_t ShellState::heap_decode_slot_calls() const noexcept {
    return session().table.decode_heap_slot_calls;
}

std::uint64_t ShellState::heap_decode_slot_hits() const noexcept {
    return session().table.decode_heap_slot_hits;
}

std::uint64_t ShellState::heap_decode_slot_misses() const noexcept {
    return session().table.decode_heap_slot_misses;
}

ShellStatePathsView shell_paths_view(const ShellState& st) noexcept {
    return ShellStatePathsView{st.data_dir(), st.session().data_path};
}

std::string resolve_table_file(const ShellState& st, const std::string& rel_or_abs) {
    return resolve_table_file_paths(shell_paths_view(st), rel_or_abs);
}

std::string effective_data_path(const ShellState& st) {
    return effective_data_path_paths(shell_paths_view(st));
}

std::filesystem::path workspace_directory(const ShellState& st) {
    return workspace_directory_paths(shell_paths_view(st));
}

ShellStateOwner::ShellStateOwner() {
    engine_ = newdb_engine_session_create("", nullptr, nullptr, 0);
    if (engine_ == nullptr) {
        shell_ = std::make_unique<ShellState>();
        return;
    }
    shell_ = std::make_unique<ShellState>(ShellStateEngineBorrowedTag{}, engine_);
}

ShellStateOwner::~ShellStateOwner() {
    shell_.reset();
    newdb_engine_session_destroy(engine_);
    engine_ = nullptr;
}

ShellStateOwner::ShellStateOwner(ShellStateOwner&& o) noexcept
    : engine_(o.engine_), shell_(std::move(o.shell_)) {
    o.engine_ = nullptr;
}

ShellStateOwner& ShellStateOwner::operator=(ShellStateOwner&& o) noexcept {
    if (this == &o) {
        return *this;
    }
    shell_.reset();
    newdb_engine_session_destroy(engine_);
    engine_ = o.engine_;
    shell_ = std::move(o.shell_);
    o.engine_ = nullptr;
    return *this;
}

ShellState& ShellStateOwner::shell() noexcept {
    return *shell_;
}

const ShellState& ShellStateOwner::shell() const noexcept {
    return *shell_;
}
