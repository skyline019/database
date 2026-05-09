#include "shell_state_test_support.h"

#include <gtest/gtest.h>

TEST(ShellStateTestSupport, MakeShellStateForTestConstructs) {
    ShellStateOwner h = make_shell_state_for_test();
    (void)h;
}

TEST(ShellStateTestSupport, HeapUniqueConstructsAndDestroys) {
    ShellStateHeapUniqueForTest p = make_shell_state_heap_unique_for_test();
    ASSERT_NE(p.get(), nullptr);
}
