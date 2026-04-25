#include <newdb/heap_page.h>
#include <newdb/heap_storage.h>
#include <newdb/page_io.h>
#include <newdb/schema.h>
#include <newdb/tuple_codec.h>

#include "test_util.h"

#include <cstdio>

#include <gtest/gtest.h>

using newdb::HeapTable;
using newdb::Row;
using newdb::TableSchema;

static TableSchema minimal_schema() {
    TableSchema s;
    s.primary_key = "id";
    s.attrs = {};
    return s;
}

TEST(PageIo, CreateHeapFileAndLoad) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_heap");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "t.bin").string();

    std::vector<Row> seed = {
        Row{1, {{"balance", "100"}}, {}},
        Row{2, {{"balance", "200"}}, {}},
    };
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), seed).ok);

    HeapTable tbl;
    const TableSchema schema = minimal_schema();
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "t", schema, tbl).ok);
    ASSERT_EQ(tbl.rows.size(), 2u);
    EXPECT_EQ(tbl.rows[0].id, 1);
    EXPECT_EQ(tbl.rows[1].attrs.at("balance"), "200");
}

TEST(PageIo, AppendThenReload) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_append");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "u.bin").string();

    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), {Row{1, {{"k", "v"}}, {}}}).ok);
    ASSERT_TRUE(newdb::io::append_row(path.c_str(), Row{2, {{"k", "w"}}, {}}).ok);

    HeapTable tbl;
    const TableSchema schema = minimal_schema();
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "u", schema, tbl).ok);
    ASSERT_EQ(tbl.rows.size(), 2u);
    EXPECT_EQ(tbl.rows[1].id, 2);
}

TEST(PageIo, LoadFailsWhenTypedAttrInvalid) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_heap_badval");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "bad.bin").string();

    std::vector<Row> seed = {
        Row{1, {{"balance", "x"}}, {}},
    };
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), seed).ok);

    TableSchema schema;
    schema.primary_key = "id";
    schema.attrs = {newdb::AttrMeta{"balance", newdb::AttrType::Int}};
    HeapTable tbl;
    const auto st = newdb::io::load_heap_file(path.c_str(), "bad", schema, tbl);
    EXPECT_FALSE(st.ok);
}

TEST(PageIo, CreateManyRowsLoadsMultiPage) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_heap_many");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "big.bin").string();
    std::vector<Row> rows;
    rows.reserve(2500);
    for (int i = 1; i <= 2500; ++i) {
        rows.push_back(Row{i, {{"k", "v"}}, {}});
    }
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), rows).ok);
    newdb::HeapTable tbl;
    const TableSchema schema = minimal_schema();
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "big", schema, tbl).ok);
    EXPECT_EQ(tbl.rows.size(), 2500u);
}

TEST(PageIo, AppendRowNullPathFails) {
    const newdb::Status st = newdb::io::append_row(nullptr, Row{1, {}, {}});
    EXPECT_FALSE(st.ok);
}

TEST(PageIo, CreateEmptyHeapFileLoads) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_heap_empty");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "empty.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), {}).ok);
    newdb::HeapTable tbl;
    const TableSchema schema = minimal_schema();
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "e", schema, tbl).ok);
    EXPECT_TRUE(tbl.rows.empty());
}

TEST(PageIo, LoadMissingFileFails) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_missing");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "missing.bin").string();
    HeapTable tbl;
    const TableSchema schema = minimal_schema();
    const auto st = newdb::io::load_heap_file(path.c_str(), "m", schema, tbl);
    EXPECT_FALSE(st.ok);
}

TEST(PageIo, AppendCreatesFileWhenMissing) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_append_create");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "newonly.bin").string();
    ASSERT_TRUE(newdb::io::append_row(path.c_str(), Row{7, {{"k", "v"}}, {}}).ok);
    HeapTable tbl;
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "newonly", minimal_schema(), tbl).ok);
    ASSERT_EQ(tbl.rows.size(), 1u);
    EXPECT_EQ(tbl.rows[0].id, 7);
}

TEST(PageIo, LoadSkipsBadChecksumPageThenLoadsNext) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_bad_crc");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "mix.bin").string();
    const std::size_t psz = newdb::heap_page::byte_size();

    std::vector<unsigned char> bad = newdb::heap_page::allocate_fresh_page();
    newdb::heap_page::update_checksum(bad);
    ASSERT_TRUE(newdb::heap_page::verify_checksum(bad.data(), bad.size()));
    bad[std::min<std::size_t>(200, psz - 1)] ^= static_cast<unsigned char>(0x5A);
    ASSERT_FALSE(newdb::heap_page::verify_checksum(bad.data(), bad.size()));

    std::vector<unsigned char> good = newdb::heap_page::allocate_fresh_page();
    std::vector<unsigned char> enc;
    ASSERT_TRUE(newdb::codec::encode_row_to_heap_payload(Row{1, {{"a", "b"}}, {}}, enc).ok);
    ASSERT_TRUE(newdb::heap_page::append_encoded_record(good, enc.data(), enc.size()));
    newdb::heap_page::update_checksum(good);

    FILE* fp = std::fopen(path.c_str(), "wb");
    ASSERT_NE(fp, nullptr);
    ASSERT_EQ(std::fwrite(bad.data(), 1, psz, fp), psz);
    ASSERT_EQ(std::fwrite(good.data(), 1, psz, fp), psz);
    std::fclose(fp);

    HeapTable tbl;
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "mix", minimal_schema(), tbl).ok);
    ASSERT_EQ(tbl.rows.size(), 1u);
    EXPECT_EQ(tbl.rows[0].id, 1);
}

TEST(PageIo, LoadFailsOnPartialTrailingPage) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_partial");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "trail.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), {Row{1, {}, {}}}).ok);
    FILE* fp = std::fopen(path.c_str(), "ab");
    ASSERT_NE(fp, nullptr);
    const char junk[] = "trunc";
    ASSERT_EQ(std::fwrite(junk, 1, sizeof(junk) - 1, fp), sizeof(junk) - 1);
    std::fclose(fp);

    HeapTable tbl;
    const auto st = newdb::io::load_heap_file(path.c_str(), "trail", minimal_schema(), tbl);
    EXPECT_FALSE(st.ok);
}

TEST(PageIo, TombstoneDeletesRow) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_tomb");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "tomb.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), {Row{1, {{"n", "x"}}, {}}}).ok);
    ASSERT_TRUE(newdb::io::append_row(path.c_str(), Row{1, {{"__deleted", "1"}, {"n", "x"}}, {}}).ok);

    HeapTable tbl;
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "tomb", minimal_schema(), tbl).ok);
    EXPECT_TRUE(tbl.rows.empty());
}

TEST(PageIo, CompactHeapFileKeepsOnlyLatestLogicalRows) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_compact");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "compact.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), {Row{1, {{"name", "old"}}, {}}, Row{2, {{"name", "b"}}, {}}}).ok);
    ASSERT_TRUE(newdb::io::append_row(path.c_str(), Row{1, {{"name", "new"}}, {}}).ok);
    ASSERT_TRUE(newdb::io::append_row(path.c_str(), Row{2, {{"__deleted", "1"}, {"name", "b"}}, {}}).ok);

    std::size_t rows_after = 0;
    ASSERT_TRUE(newdb::io::compact_heap_file(path.c_str(), "compact", minimal_schema(), &rows_after).ok);
    EXPECT_EQ(rows_after, 1u);

    HeapTable tbl;
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "compact", minimal_schema(), tbl).ok);
    ASSERT_EQ(tbl.rows.size(), 1u);
    EXPECT_EQ(tbl.rows[0].id, 1);
    EXPECT_EQ(tbl.rows[0].attrs.at("name"), "new");
}

TEST(PageIo, ScanHeapFileAndQueryHelpersRun) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_scan_query");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "q.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), {Row{1, {{"balance", "42"}}, {}}}).ok);

    newdb::io::scan_heap_file(path.c_str());
    newdb::io::query_attr_int_ge(nullptr, "balance", 0);
    newdb::io::query_attr_int_ge(path.c_str(), "", 0);
    newdb::io::query_attr_int_ge(path.c_str(), "balance", 40);
    newdb::io::query_balance_ge(path.c_str(), 40);
    newdb::io::scan_heap_file((dir / "no_such_file.bin").string().c_str());
    newdb::io::query_attr_int_ge((dir / "no_such_file2.bin").string().c_str(), "balance", 0);
}

TEST(PageIo, QueryAttrSkipsNonNumericColumn) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_q_nan");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "nan.bin").string();
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), {Row{1, {{"balance", "notint"}}, {}}}).ok);
    newdb::io::query_attr_int_ge(path.c_str(), "balance", 0);
}

TEST(PageIo, LazyDecodeKeepsRowsEmptyThenMaterialize) {
    const auto dir = newdb::test::unique_temp_subdir("newdb_lazy_heap");
    std::filesystem::create_directories(dir);
    const std::string path = (dir / "lazy.bin").string();
    std::vector<Row> seed = {
        Row{1, {{"balance", "100"}}, {}},
        Row{2, {{"balance", "200"}}, {}},
    };
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), seed).ok);

    HeapTable tbl;
    const TableSchema schema = minimal_schema();
    newdb::HeapLoadOptions opts;
    opts.lazy_decode = true;
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "lazy", schema, tbl, opts).ok);
    EXPECT_TRUE(tbl.is_heap_storage_backed());
    EXPECT_TRUE(tbl.rows.empty());
    ASSERT_EQ(tbl.logical_row_count(), 2u);

    Row r0;
    ASSERT_TRUE(tbl.decode_heap_slot(0, r0));
    EXPECT_EQ(r0.id, 1);
    EXPECT_EQ(r0.attrs.at("balance"), "100");

    const Row* p2 = tbl.find_by_id(2);
    ASSERT_NE(p2, nullptr);
    EXPECT_EQ(p2->attrs.at("balance"), "200");

    ASSERT_TRUE(tbl.materialize_all_rows(schema).ok);
    EXPECT_FALSE(tbl.is_heap_storage_backed());
    ASSERT_EQ(tbl.rows.size(), 2u);
    EXPECT_EQ(tbl.rows[1].attrs.at("balance"), "200");
}
