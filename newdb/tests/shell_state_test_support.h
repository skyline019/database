#pragma once

#include "cli/shell/state/shell_state_owner.h"

#include <memory>

class ShellState;

/// Custom deleter so call sites may hold `std::unique_ptr<ShellState>` without including
/// `shell_state.h` in the same translation unit (destruction runs here).
struct ShellStateTestHeapDeleter {
    void operator()(ShellState* p) const noexcept;
};

using ShellStateHeapUniqueForTest = std::unique_ptr<ShellState, ShellStateTestHeapDeleter>;

/// Default-constructed heap `ShellState` (embedded session path); prefer
/// [`make_shell_state_for_test`](shell_state_test_support.h) for engine-backed parity with demo/C API.
[[nodiscard]] ShellStateHeapUniqueForTest make_shell_state_heap_unique_for_test();

/// Preferred shell fixture for tests: matches `newdb_demo` / full C API — engine-owned session
/// and borrowed `ShellState` when `newdb_engine_session_create` succeeds.
[[nodiscard]] ShellStateOwner make_shell_state_for_test();
