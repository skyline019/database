#include "condition.h"
#include "where.h"

#include <newdb/schema.h>

#include <gtest/gtest.h>

using newdb::AttrMeta;
using newdb::AttrType;
using newdb::HeapTable;
using newdb::Row;
using newdb::TableSchema;

TEST(Condition, ParseCondOp) {
    EXPECT_EQ(parse_cond_op("="), CondOp::Eq);
    EXPECT_EQ(parse_cond_op("=="), CondOp::Eq);
    EXPECT_EQ(parse_cond_op("!="), CondOp::Ne);
    EXPECT_EQ(parse_cond_op("<>"), CondOp::Ne);
    EXPECT_EQ(parse_cond_op("<"), CondOp::Lt);
    EXPECT_EQ(parse_cond_op(">"), CondOp::Gt);
    EXPECT_EQ(parse_cond_op("<="), CondOp::Le);
    EXPECT_EQ(parse_cond_op(">="), CondOp::Ge);
    EXPECT_EQ(parse_cond_op("Contains"), CondOp::Contains);
    EXPECT_EQ(parse_cond_op("CONTAINS"), CondOp::Contains);
    EXPECT_EQ(parse_cond_op("??"), CondOp::Unknown);
}

TEST(Where, ParseWhereArgs) {
    std::vector<std::string> args = {"balance", ">=", "10"};
    std::vector<WhereCond> conds;
    std::string err;
    ASSERT_TRUE(parse_where_args_to_conds(args, conds, err)) << err;
    ASSERT_EQ(conds.size(), 1u);
    EXPECT_EQ(conds[0].attr, "balance");
    EXPECT_EQ(conds[0].op, CondOp::Ge);
    EXPECT_EQ(conds[0].value, "10");
}

TEST(Where, MultiConditionAndOr) {
    TableSchema schema;
    schema.attrs = {AttrMeta{"a", AttrType::Int}, AttrMeta{"b", AttrType::Int}};
    Row r;
    r.id = 1;
    r.attrs["a"] = "5";
    r.attrs["b"] = "20";

    std::vector<WhereCond> conds;
    conds.push_back({"a", CondOp::Eq, "5", ""});
    WhereCond c2;
    c2.attr = "b";
    c2.op = CondOp::Gt;
    c2.value = "10";
    c2.logic_with_prev = "AND";
    conds.push_back(c2);
    EXPECT_TRUE(row_match_multi_conditions(r, schema, conds));

    conds[1].value = "100";
    EXPECT_FALSE(row_match_multi_conditions(r, schema, conds));

    conds[1].logic_with_prev = "OR";
    EXPECT_TRUE(row_match_multi_conditions(r, schema, conds));
}

TEST(Where, QueryWithIndexEmptyCondsReturnsAll) {
    TableSchema schema;
    schema.primary_key = "id";
    HeapTable tbl;
    tbl.rows = {Row{1, {}, {}}, Row{2, {}, {}}};
    tbl.rebuild_indexes(schema);
    const std::vector<WhereCond> empty;
    const auto idx = query_with_index(tbl, schema, empty);
    ASSERT_EQ(idx.size(), 2u);
}

TEST(Where, QueryWithIndexById) {
    TableSchema schema;
    schema.primary_key = "id";
    HeapTable tbl;
    tbl.rows = {Row{1, {{"x", "1"}}, {}}, Row{99, {{"x", "2"}}, {}}};
    tbl.rebuild_indexes(schema);
    std::vector<WhereCond> conds;
    conds.push_back({"id", CondOp::Eq, "99", ""});
    const auto idx = query_with_index(tbl, schema, conds);
    ASSERT_EQ(idx.size(), 1u);
    EXPECT_EQ(tbl.rows[idx[0]].id, 99);
}

TEST(Where, ParseAggWithWhere) {
    std::vector<std::string> args = {"balance", "WHERE", "id", "=", "3"};
    std::string attr;
    std::vector<WhereCond> conds;
    std::string err;
    ASSERT_TRUE(parse_agg_args_with_optional_where(args, attr, conds, err)) << err;
    EXPECT_EQ(attr, "balance");
    ASSERT_EQ(conds.size(), 1u);
    EXPECT_EQ(conds[0].attr, "id");
}

TEST(Where, RowMatchConditionContainsAndComparisons) {
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {AttrMeta{"note", AttrType::String}};
    newdb::Row r{1, {{"note", "hello world"}}, {}};

    EXPECT_TRUE(row_match_condition(r, schema, "note", CondOp::Contains, "world"));
    EXPECT_FALSE(row_match_condition(r, schema, "note", CondOp::Contains, "zzz"));
    EXPECT_TRUE(row_match_condition(r, schema, "id", CondOp::Eq, "1"));
    EXPECT_TRUE(row_match_condition(r, schema, "id", CondOp::Ne, "2"));
    EXPECT_TRUE(row_match_condition(r, schema, "id", CondOp::Lt, "5"));
    EXPECT_TRUE(row_match_condition(r, schema, "id", CondOp::Le, "1"));
    EXPECT_TRUE(row_match_condition(r, schema, "id", CondOp::Gt, "0"));
    EXPECT_TRUE(row_match_condition(r, schema, "id", CondOp::Ge, "1"));
    EXPECT_FALSE(row_match_condition(r, schema, "id", CondOp::Unknown, "1"));
}

TEST(Where, ParseWhereArgsErrors) {
    std::vector<WhereCond> conds;
    std::string err;
    EXPECT_FALSE(parse_where_args_to_conds({}, conds, err));
    EXPECT_FALSE(parse_where_args_to_conds({"a", "??", "v"}, conds, err));

    EXPECT_FALSE(parse_where_args_to_conds({"a", "=", "1", "XOR", "b", "=", "2"}, conds, err));
    EXPECT_NE(err.find("AND/OR"), std::string::npos);

    EXPECT_FALSE(parse_where_args_to_conds({"a", "=", "1", "AND", "b", "badop", "2"}, conds, err));
}

TEST(Where, ParseAggArgsErrors) {
    std::string attr;
    std::vector<WhereCond> conds;
    std::string err;
    EXPECT_FALSE(parse_agg_args_with_optional_where({}, attr, conds, err));
    EXPECT_FALSE(parse_agg_args_with_optional_where({"x", "y"}, attr, conds, err));
    EXPECT_FALSE(parse_agg_args_with_optional_where({"x", "WHERE"}, attr, conds, err));
    EXPECT_FALSE(parse_agg_args_with_optional_where({"x", "ORDER", "id", "=", "1"}, attr, conds, err));
    EXPECT_FALSE(parse_agg_args_with_optional_where({"x", "WHERE", "a", "bad", "v"}, attr, conds, err));
}

TEST(Where, QueryWithIndexIdStoiFailureFallsBack) {
    newdb::TableSchema schema;
    schema.primary_key = "id";
    newdb::HeapTable tbl;
    tbl.rows = {Row{1, {}, {}}, Row{2, {}, {}}};
    tbl.rebuild_indexes(schema);
    std::vector<WhereCond> conds;
    conds.push_back({"id", CondOp::Eq, "not_int", ""});
    const auto idx = query_with_index(tbl, schema, conds);
    EXPECT_TRUE(idx.empty());
}

TEST(Where, QueryWithIndexMultiCondUsesFullScan) {
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {AttrMeta{"balance", AttrType::Int}};
    newdb::HeapTable tbl;
    tbl.rows = {Row{1, {{"balance", "10"}}, {}}, Row{2, {{"balance", "20"}}, {}}};
    tbl.rebuild_indexes(schema);
    std::vector<WhereCond> conds;
    conds.push_back({"balance", CondOp::Ge, "15", ""});
    conds.push_back({"balance", CondOp::Le, "25", "AND"});
    const auto idx = query_with_index(tbl, schema, conds);
    ASSERT_EQ(idx.size(), 1u);
    EXPECT_EQ(tbl.rows[idx[0]].id, 2);
}

TEST(Where, PolicyRejectBlocksHeavyFallback) {
#ifdef _WIN32
    _putenv_s("NEWDB_WHERE_POLICY_MODE", "reject");
    _putenv_s("NEWDB_WHERE_POLICY_MIN_ROWS", "1");
#else
    setenv("NEWDB_WHERE_POLICY_MODE", "reject", 1);
    setenv("NEWDB_WHERE_POLICY_MIN_ROWS", "1", 1);
#endif
    newdb::TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {AttrMeta{"balance", AttrType::Int}};
    newdb::HeapTable tbl;
    tbl.rows = {Row{1, {{"balance", "10"}}, {}}, Row{2, {{"balance", "20"}}, {}}};
    tbl.rebuild_indexes(schema);
    std::vector<WhereCond> conds;
    conds.push_back({"balance", CondOp::Ge, "15", ""});
    conds.push_back({"balance", CondOp::Le, "25", "AND"});
    const auto idx = query_with_index(tbl, schema, conds);
    EXPECT_TRUE(idx.empty());
    EXPECT_TRUE(where_policy_last_blocked());
    EXPECT_NE(where_policy_last_message().find("heavy path"), std::string::npos);
#ifdef _WIN32
    _putenv_s("NEWDB_WHERE_POLICY_MODE", "");
    _putenv_s("NEWDB_WHERE_POLICY_MIN_ROWS", "");
#else
    unsetenv("NEWDB_WHERE_POLICY_MODE");
    unsetenv("NEWDB_WHERE_POLICY_MIN_ROWS");
#endif
}

TEST(Where, PolicyRejectKeepsIdLookup) {
#ifdef _WIN32
    _putenv_s("NEWDB_WHERE_POLICY_MODE", "reject");
    _putenv_s("NEWDB_WHERE_POLICY_MIN_ROWS", "1");
#else
    setenv("NEWDB_WHERE_POLICY_MODE", "reject", 1);
    setenv("NEWDB_WHERE_POLICY_MIN_ROWS", "1", 1);
#endif
    TableSchema schema;
    schema.primary_key = "id";
    HeapTable tbl;
    tbl.rows = {Row{1, {{"x", "1"}}, {}}, Row{99, {{"x", "2"}}, {}}};
    tbl.rebuild_indexes(schema);
    std::vector<WhereCond> conds;
    conds.push_back({"id", CondOp::Eq, "99", ""});
    const auto idx = query_with_index(tbl, schema, conds);
    ASSERT_EQ(idx.size(), 1u);
    EXPECT_FALSE(where_policy_last_blocked());
#ifdef _WIN32
    _putenv_s("NEWDB_WHERE_POLICY_MODE", "");
    _putenv_s("NEWDB_WHERE_POLICY_MIN_ROWS", "");
#else
    unsetenv("NEWDB_WHERE_POLICY_MODE");
    unsetenv("NEWDB_WHERE_POLICY_MIN_ROWS");
#endif
}
