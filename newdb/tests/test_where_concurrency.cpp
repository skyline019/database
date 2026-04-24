#include "where.h"

#include <newdb/schema.h>

#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using newdb::AttrMeta;
using newdb::AttrType;
using newdb::HeapTable;
using newdb::Row;
using newdb::TableSchema;

TEST(WhereConcurrency, SharedContextParallelQueryIsStable) {
#ifdef _WIN32
    _putenv_s("NEWDB_WHERE_POLICY_MODE", "off");
#else
    setenv("NEWDB_WHERE_POLICY_MODE", "off", 1);
#endif
    TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {AttrMeta{"dept", AttrType::String}, AttrMeta{"age", AttrType::Int}};

    HeapTable tbl;
    tbl.rows.reserve(30000);
    for (int i = 1; i <= 30000; ++i) {
        Row r;
        r.id = i;
        r.attrs["dept"] = (i % 3 == 0) ? "ENG" : "OPS";
        r.attrs["age"] = std::to_string(18 + (i % 40));
        tbl.rows.push_back(std::move(r));
    }
    tbl.rebuild_indexes(schema);

    std::vector<WhereCond> conds;
    conds.push_back({"dept", CondOp::Eq, "ENG", ""});
    conds.push_back({"age", CondOp::Ge, "30", "AND"});

    const auto baseline = query_with_index(tbl, schema, conds);
    ASSERT_FALSE(baseline.empty());

    WhereQueryContext shared_ctx;
    constexpr int kThreads = 10;
    constexpr int kLoops = 80;
    std::atomic<bool> ok{true};
    std::vector<std::thread> ths;
    ths.reserve(kThreads);

    for (int t = 0; t < kThreads; ++t) {
        ths.emplace_back([&]() {
            for (int i = 0; i < kLoops; ++i) {
                const auto out = query_with_index(tbl, schema, conds, &shared_ctx);
                if (out.size() != baseline.size()) {
                    ok.store(false, std::memory_order_relaxed);
                    return;
                }
                if (where_policy_last_blocked(&shared_ctx)) {
                    ok.store(false, std::memory_order_relaxed);
                    return;
                }
            }
        });
    }
    for (auto& th : ths) th.join();
    EXPECT_TRUE(ok.load(std::memory_order_relaxed));

#ifdef _WIN32
    _putenv_s("NEWDB_WHERE_POLICY_MODE", "");
#else
    unsetenv("NEWDB_WHERE_POLICY_MODE");
#endif
}
