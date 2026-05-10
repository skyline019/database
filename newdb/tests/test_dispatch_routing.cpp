#include <gtest/gtest.h>

#include "cli/shell/dispatch/router/dispatch_routing.h"

TEST(DispatchRouting, Phase2PrefixesSkipPhase1) {
    EXPECT_TRUE(shell_line_targets_phase2_only("PAGE(0,10)"));
    EXPECT_TRUE(shell_line_targets_phase2_only("  WHERE id = 1"));
    EXPECT_TRUE(shell_line_targets_phase2_only("SET PRIMARY KEY(id)"));
    EXPECT_TRUE(shell_line_targets_phase2_only("EXPORT out.csv"));
    EXPECT_TRUE(shell_line_targets_phase2_only("FIND(1)"));
    EXPECT_TRUE(shell_line_targets_phase2_only("DELETE(0)"));
    EXPECT_TRUE(shell_line_targets_phase2_only("DELETEWHERE(age, >, 1)"));
    EXPECT_TRUE(shell_line_targets_phase2_only("UPDATEWHERE(name, x, WHERE, age, =, 1)"));
    EXPECT_FALSE(shell_line_targets_phase2_only("USE(t)"));
    EXPECT_FALSE(shell_line_targets_phase2_only("BEGIN"));
    EXPECT_FALSE(shell_line_targets_phase2_only("CREATE TABLE(x)"));
    EXPECT_FALSE(shell_line_targets_phase2_only("WHEREabouts x"));
    EXPECT_FALSE(shell_line_targets_phase2_only(nullptr));
}
