#include "cli/modules/where/executor/internal/query_internal.h"
#include "cli/modules/where/executor/where.h"

#include <gtest/gtest.h>

#include <cstdlib>

#if defined(_WIN32)
#include <stdlib.h>
#endif

namespace {

#if defined(_WIN32)
void set_env(const char* k, const char* v) {
    (void)_putenv_s(k, v);
}
void unset_env(const char* k) {
    (void)_putenv_s(k, "");
}
#else
void set_env(const char* k, const char* v) {
    (void)setenv(k, v, 1);
}
void unset_env(const char* k) {
    (void)unsetenv(k);
}
#endif

struct ScopedEnv {
    const char* key;
    ScopedEnv(const char* k, const char* v) : key(k) {
        set_env(k, v);
    }
    ScopedEnv(const ScopedEnv&) = delete;
    ScopedEnv& operator=(const ScopedEnv&) = delete;
    ~ScopedEnv() {
        unset_env(key);
    }
};

} // namespace

TEST(WhereHeapScanBudgetBinding, IncrementsWhenHeapCapTightensBelowRatioCap) {
    ScopedEnv e0("NEWDB_WHERE_HEAP_SCAN_BUDGET_ROWS", "100");
    ScopedEnv e1("NEWDB_WHERE_POLICY_MIN_ROWS", "1");
    ScopedEnv e2("NEWDB_WHERE_POLICY_MODE", "ratelimit");

    WhereQueryContext ctx;
    const bool ok =
        where_policy_gate("fallback_scan", 10000u, 1u, 5u, false, ctx);
    EXPECT_TRUE(ok);
    EXPECT_EQ(ctx.where_heap_scan_budget_binding_events.load(std::memory_order_relaxed), 1u);
    EXPECT_GE(ctx.policy_checks.load(std::memory_order_relaxed), 1u);
}
