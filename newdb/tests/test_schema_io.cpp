#include <newdb/schema_io.h>

#include "test_util.h"

#include <fstream>
#include <gtest/gtest.h>

using newdb::AttrMeta;
using newdb::AttrType;
using newdb::TableSchema;

TEST(SchemaIo, SidecarPathReplacesBinSuffix) {
    EXPECT_EQ(newdb::schema_sidecar_path_for_data_file("/a/b/c.bin"), "/a/b/c.attr");
    EXPECT_EQ(newdb::schema_sidecar_path_for_data_file("x.bin"), "x.attr");
}

TEST(SchemaIo, SidecarPathAppendsAttrWhenNoBin) {
    EXPECT_EQ(newdb::schema_sidecar_path_for_data_file("foo"), "foo.attr");
}

TEST(SchemaIo, SaveLoadRoundTrip) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_schema_io");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "t.attr").string();

    TableSchema in;
    in.table_label = "acct";
    in.primary_key = "email";
    in.attrs = {AttrMeta{"email", AttrType::String}, AttrMeta{"age", AttrType::Int}};

    ASSERT_TRUE(newdb::save_schema_text(path, in).ok);
    TableSchema out;
    ASSERT_TRUE(newdb::load_schema_text(path, out).ok);
    EXPECT_EQ(out.table_label, "acct");
    EXPECT_EQ(out.primary_key, "email");
    ASSERT_EQ(out.attrs.size(), 2u);
    EXPECT_EQ(out.attrs[0].name, "email");
    EXPECT_EQ(out.attrs[0].type, AttrType::String);
    EXPECT_EQ(out.attrs[1].name, "age");
    EXPECT_EQ(out.attrs[1].type, AttrType::Int);
    EXPECT_EQ(out.heap_format_version, 1u);
}

TEST(SchemaIo, MissingFileYieldsDefaults) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_schema_missing");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "nope.attr").string();
    TableSchema out;
    out.primary_key = "x";
    out.attrs.push_back(AttrMeta{"a", AttrType::Int});
    ASSERT_TRUE(newdb::load_schema_text(path, out).ok);
    EXPECT_TRUE(out.attrs.empty());
    EXPECT_EQ(out.primary_key, "id");
}

TEST(SchemaIo, HeapFormatLineParsed) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_schema_hf");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "hf.attr").string();
    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out);
        out << "PRIMARY_KEY:id\n";
        out << "HEAP_FORMAT:7\n";
    }
    TableSchema out_s;
    ASSERT_TRUE(newdb::load_schema_text(path, out_s).ok);
    EXPECT_EQ(out_s.heap_format_version, 7u);
}

TEST(SchemaIo, InvalidPrimaryKeyInFileFallsBackToId) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_schema_badpk");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "bad.attr").string();
    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out);
        out << "PRIMARY_KEY:not_a_column\n";
        out << "name:string\n";
    }
    TableSchema out_s;
    ASSERT_TRUE(newdb::load_schema_text(path, out_s).ok);
    EXPECT_EQ(out_s.primary_key, "id");
}
