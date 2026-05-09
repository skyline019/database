#pragma once

/// Internal layout for [`ShellState`](shell_state.h). Include only from
/// `shell_state.cc` and `shell_state_ops.cc`. Dispatch handlers should use
/// [`ShellStateFacade`](shell_state_facade.h) / [`shell_session_view.h`](shell_session_view.h).

#include "cli/shell/state/shell_state.h"
#include "cli/shell/state/shell_state_lsm_sidecar_runtime_impl.h"
#include "cli/shell/state/shell_state_txn_where_runtime_impl.h"

#include <newdb/engine_session_opaque.h>

#include <memory>
#include <string>

namespace newdb {
struct Session;
}

namespace shell_state_detail {
struct HeapGuardBox;
}

/// Paths, logging, and shell I/O flags (split from txn/WHERE/LSM bundles to keep `Impl` readable).
struct ShellStatePathsAndIoFlags {
    std::string log_file_path{"demo_log.bin"};
    std::string data_dir;
    int mirror_output_fd{-1};
    bool encrypt_log{false};
    bool verbose{false};
    ShellRuntimePolicy runtime_policy{};
};

struct ShellState::Impl {
    std::unique_ptr<newdb::Session> session_;
    /// When non-null, `session()` is borrowed from this handle; `session_` must be null.
    newdb_engine_session_t* engine_session_borrow_{nullptr};
    std::unique_ptr<shell_state_detail::HeapGuardBox> heap_guard_box_;
    std::unique_ptr<ShellTxnWhereRuntime> txn_where_;
    std::unique_ptr<ShellLsmSidecarRuntime> lsm_sidecar_;
    ShellStatePathsAndIoFlags paths_and_io_;
};
