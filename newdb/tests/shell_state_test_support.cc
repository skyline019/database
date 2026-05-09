#include "shell_state_test_support.h"

#include "cli/shell/state/shell_state.h"

void ShellStateTestHeapDeleter::operator()(ShellState* p) const noexcept {
    delete p;
}

ShellStateHeapUniqueForTest make_shell_state_heap_unique_for_test() {
    return ShellStateHeapUniqueForTest(new ShellState(), ShellStateTestHeapDeleter{});
}

ShellStateOwner make_shell_state_for_test() {
    return ShellStateOwner{};
}
