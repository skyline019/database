#include "cli/modules/where/executor/where.h"

#include <newdb/memory_registry.h>
#include <newdb/schema.h>

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

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

}  // namespace

TEST(WhereQueryTempCap, EmptyCondsBlockedWhenEstimateExceedsCap) {
    ScopedEnv e("NEWDB_QUERY_TEMP_MAX_BYTES", "32");
    newdb::TableSchema schema;
    schema.primary_key = "id";
    newdb::HeapTable tbl;
    tbl.rows = {newdb::Row{1, {}, {}}, newdb::Row{2, {}, {}}};
    tbl.rebuild_indexes(schema);
    WhereQueryContext ctx;
    const std::vector<WhereCond> empty;
    const auto idx = query_with_index(tbl, schema, empty, &ctx);
    EXPECT_TRUE(idx.empty());
    EXPECT_TRUE(where_policy_last_blocked(&ctx));
    EXPECT_NE(where_policy_last_message(&ctx).find("query_temp"), std::string::npos);
    const std::uint64_t rj = newdb::memory_registry_totals().query_temp_admit_rejects;
    EXPECT_GE(rj, 1u);
}

TEST(WhereQueryTempCap, NonEmptyQueryBlockedWhenEstimateExceedsCap) {
    ScopedEnv e("NEWDB_QUERY_TEMP_MAX_BYTES", "32");
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs.push_back(newdb::AttrMeta{"n", newdb::AttrType::String});
    newdb::HeapTable tbl;
    tbl.rows = {newdb::Row{1, {{"n", "a"}}, {}}, newdb::Row{2, {{"n", "b"}}, {}}};
    tbl.rebuild_indexes(schema);
    WhereQueryContext ctx;
    std::vector<WhereCond> conds;
    conds.push_back(WhereCond{"n", CondOp::Eq, "a", ""});
    const auto idx = query_with_index(tbl, schema, conds, &ctx);
    EXPECT_TRUE(idx.empty());
    EXPECT_TRUE(where_policy_last_blocked(&ctx));
}
