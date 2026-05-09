#pragma once

/// **Internal:** heap lock box for [`ShellState::Impl`](shell_state_impl.h). Include only from
/// translation units that already own session layout (`shell_state.cc`, `shell_state_ops.cc`).
/// Keeps [`shell_state_impl.h`](shell_state_impl.h) free of `<newdb/session.h>`.

#include <newdb/session.h>

#include <optional>

namespace shell_state_detail {

struct HeapGuardBox {
    std::optional<newdb::Session::HeapAccess> session_heap_guard;
};

} // namespace shell_state_detail
