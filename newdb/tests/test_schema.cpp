#include <newdb/row.h>
#include <newdb/schema.h>

#include <gtest/gtest.h>

using newdb::AttrType;
using newdb::AttrMeta;
using newdb::Row;
using newdb::TableSchema;

TEST(Schema, ParseTypeCaseInsensitive) {
    EXPECT_EQ(TableSchema::parse_type("INT"), AttrType::Int);
    EXPECT_EQ(TableSchema::parse_type("Integer"), AttrType::Int);
    EXPECT_EQ(TableSchema::parse_type("STRING"), AttrType::String);
    EXPECT_EQ(TableSchema::parse_type("str"), AttrType::String);
    EXPECT_EQ(TableSchema::parse_type("Bool"), AttrType::Bool);
    EXPECT_EQ(TableSchema::parse_type("boolean"), AttrType::Bool);
    EXPECT_EQ(TableSchema::parse_type("FLOAT"), AttrType::Float);
    EXPECT_EQ(TableSchema::parse_type("Double"), AttrType::Double);
    EXPECT_EQ(TableSchema::parse_type("Timestamp"), AttrType::Timestamp);
    EXPECT_EQ(TableSchema::parse_type("DATE"), AttrType::Date);
    EXPECT_EQ(TableSchema::parse_type("DateTime"), AttrType::DateTime);
    EXPECT_EQ(TableSchema::parse_type("unknown_type"), AttrType::Unknown);
}

TEST(Schema, TypeNameRoundTrip) {
    const AttrType types[] = {
        AttrType::Int, AttrType::Char, AttrType::String, AttrType::Timestamp,
        AttrType::Date, AttrType::DateTime, AttrType::Float, AttrType::Double,
        AttrType::Bool, AttrType::Unknown};
    for (AttrType t : types) {
        const char* name = TableSchema::type_name(t);
        ASSERT_NE(name, nullptr);
        if (t != AttrType::Unknown) {
            EXPECT_EQ(TableSchema::parse_type(name), t);
        }
    }
}

TEST(Schema, FindAttrAndPrimaryKeyValidity) {
    TableSchema s;
    s.attrs = {AttrMeta{"name", AttrType::String}, AttrMeta{"age", AttrType::Int}};
    s.primary_key = "id";
    EXPECT_TRUE(s.valid_primary_key());
    EXPECT_EQ(s.find_attr("name")->type, AttrType::String);
    EXPECT_EQ(s.find_attr("missing"), nullptr);

    s.primary_key = "name";
    EXPECT_TRUE(s.valid_primary_key());

    s.primary_key = "nope";
    EXPECT_FALSE(s.valid_primary_key());
}

TEST(Schema, DefaultIntPredicateAttrPrefersBalance) {
    TableSchema s;
    s.attrs = {AttrMeta{"age", AttrType::Int}, AttrMeta{"score", AttrType::Int}};
    s.primary_key = "id";
    EXPECT_EQ(s.default_int_predicate_attr(), "age");
    s.attrs = {AttrMeta{"balance", AttrType::Int}, AttrMeta{"age", AttrType::Int}};
    EXPECT_EQ(s.default_int_predicate_attr(), "balance");
}

TEST(Schema, TypeOfUnknownAttrFallsBackToString) {
    TableSchema s;
    s.attrs = {AttrMeta{"age", AttrType::Int}};
    EXPECT_EQ(s.type_of("id"), AttrType::Int);
    EXPECT_EQ(s.type_of("age"), AttrType::Int);
    EXPECT_EQ(s.type_of("nosuch"), AttrType::String);
}

TEST(Schema, ValidateStorageRowBoolAndChar) {
    TableSchema s;
    s.attrs = {AttrMeta{"flag", AttrType::Bool}, AttrMeta{"tag", AttrType::Char}};
    Row r;
    r.id = 1;
    r.attrs["flag"] = "true";
    r.attrs["tag"] = "x";
    EXPECT_TRUE(newdb::validate_storage_row(s, r).ok);
    r.attrs["flag"] = "maybe";
    EXPECT_FALSE(newdb::validate_storage_row(s, r).ok);
    r.attrs["flag"] = "1";
    r.attrs["tag"] = "xx";
    EXPECT_FALSE(newdb::validate_storage_row(s, r).ok);
}

TEST(Schema, ValidateStorageRowRejectsBadInt) {
    TableSchema s;
    s.attrs = {AttrMeta{"balance", AttrType::Int}};
    Row r;
    r.id = 1;
    r.attrs["balance"] = "not_int";
    const newdb::Status st = newdb::validate_storage_row(s, r);
    EXPECT_FALSE(st.ok);
    r.attrs["balance"] = "42";
    EXPECT_TRUE(newdb::validate_storage_row(s, r).ok);
}

TEST(Schema, CompareAttrIntFloatStringBool) {
    TableSchema s;
    s.attrs = {AttrMeta{"v", AttrType::Int}, AttrMeta{"f", AttrType::Double},
               AttrMeta{"x", AttrType::Float}, AttrMeta{"b", AttrType::Bool},
               AttrMeta{"t", AttrType::String}};
    EXPECT_LT(s.compare_attr("v", "2", "10"), 0);
    EXPECT_GT(s.compare_attr("v", "10", "2"), 0);
    EXPECT_EQ(s.compare_attr("f", "1.5", "1.5"), 0);
    EXPECT_LT(s.compare_attr("f", "1.0", "2.0"), 0);
    EXPECT_EQ(s.compare_attr("x", "0.5", "0.5"), 0);
    EXPECT_LT(s.compare_attr("b", "false", "true"), 0);
    EXPECT_EQ(s.compare_attr("b", "yes", "no"), 1);
    EXPECT_LT(s.compare_attr("t", "apple", "banana"), 0);
    EXPECT_EQ(s.compare_attr("unknown_col", "a", "b"), -1);
}
