#include "demo_shell.h"
#include "shell_state.h"
#include "test_util.h"

#include <newdb/page_io.h>
#include <newdb/schema_io.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace fs = std::filesystem;

namespace {

std::string read_all_text(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return {};
    }
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

} // namespace

TEST(DemoMdb, UpdateMismatchStopsScriptAndKeepsPreviousData) {
    const fs::path temp_dir = newdb::test::unique_temp_subdir("newdb_mdb_stop");
    ASSERT_TRUE(fs::create_directories(temp_dir));

    const fs::path script = temp_dir / "case.mdb";
    {
        std::ofstream out(script);
        ASSERT_TRUE(out.is_open());
        out << "CREATE TABLE(tu)\n";
        out << "USE(tu)\n";
        out << "DEFATTR(name:string,age:int)\n";
        out << "INSERT(1,Alice,20)\n";
        out << "UPDATE(1,Alice,a)\n";
        out << "UPDATE(1,Alice,30)\n";
    }

    ShellState st;
    st.data_dir = temp_dir.string();
    st.log_file_path = (temp_dir / "demo_log.bin").string();

    run_mdb_script(st, script.string().c_str());

    const fs::path data_file = temp_dir / "tu.bin";
    ASSERT_TRUE(fs::exists(data_file));

    newdb::TableSchema schema;
    const newdb::Status schema_st = newdb::load_schema_text(
        newdb::schema_sidecar_path_for_data_file(data_file.string()), schema);
    ASSERT_TRUE(schema_st.ok) << schema_st.message;

    newdb::HeapTable table;
    const newdb::Status load_st = newdb::io::load_heap_file(
        data_file.string().c_str(), "tu", schema, table, newdb::HeapLoadOptions{});
    ASSERT_TRUE(load_st.ok) << load_st.message;
    ASSERT_EQ(table.rows.size(), 1u);

    const newdb::Row& r = table.rows.front();
    const auto it_age = r.attrs.find("age");
    ASSERT_NE(it_age, r.attrs.end());
    EXPECT_EQ(it_age->second, "20");

    const std::string log_text = read_all_text(temp_dir / "demo_log.bin");
    EXPECT_NE(log_text.find("[UPDATE] attribute 'age' expects int, got 'a'"), std::string::npos);
    EXPECT_NE(log_text.find("[SCRIPT] stopped at line 5 due to command error."), std::string::npos);
    EXPECT_EQ(log_text.find("[UPDATE] ok: id=1 updated (table='tu')."), std::string::npos);

    std::error_code ec;
    fs::remove_all(temp_dir, ec);
}

TEST(DemoMdb, LegacyUpdateBalanceMismatchStopsScriptAndKeepsPreviousData) {
    const fs::path temp_dir = newdb::test::unique_temp_subdir("newdb_mdb_legacy_stop");
    ASSERT_TRUE(fs::create_directories(temp_dir));

    const fs::path script = temp_dir / "case_legacy.mdb";
    {
        std::ofstream out(script);
        ASSERT_TRUE(out.is_open());
        out << "CREATE TABLE(tu_legacy)\n";
        out << "USE(tu_legacy)\n";
        out << "INSERT(1,Alice,100)\n";
        out << "UPDATE(1,Alice,a)\n";
        out << "UPDATE(1,Alice,200)\n";
    }

    ShellState st;
    st.data_dir = temp_dir.string();
    st.log_file_path = (temp_dir / "demo_log.bin").string();

    run_mdb_script(st, script.string().c_str());

    const fs::path data_file = temp_dir / "tu_legacy.bin";
    ASSERT_TRUE(fs::exists(data_file));

    newdb::TableSchema schema;
    const newdb::Status schema_st = newdb::load_schema_text(
        newdb::schema_sidecar_path_for_data_file(data_file.string()), schema);
    ASSERT_TRUE(schema_st.ok) << schema_st.message;

    newdb::HeapTable table;
    const newdb::Status load_st = newdb::io::load_heap_file(
        data_file.string().c_str(), "tu_legacy", schema, table, newdb::HeapLoadOptions{});
    ASSERT_TRUE(load_st.ok) << load_st.message;
    ASSERT_EQ(table.rows.size(), 1u);

    const newdb::Row& r = table.rows.front();
    const auto it_bal = r.attrs.find("balance");
    ASSERT_NE(it_bal, r.attrs.end());
    EXPECT_EQ(it_bal->second, "100");

    const std::string log_text = read_all_text(temp_dir / "demo_log.bin");
    EXPECT_NE(log_text.find("[UPDATE] attribute 'balance' expects int, got 'a'"), std::string::npos);
    EXPECT_NE(log_text.find("[SCRIPT] stopped at line 4 due to command error."), std::string::npos);
    EXPECT_EQ(log_text.find("[UPDATE] ok: id=1 updated (table='tu_legacy')."), std::string::npos);

    std::error_code ec;
    fs::remove_all(temp_dir, ec);
}

TEST(DemoMdb, LowercaseCommandsAreAccepted) {
    const fs::path temp_dir = newdb::test::unique_temp_subdir("newdb_mdb_lowercase");
    ASSERT_TRUE(fs::create_directories(temp_dir));

    const fs::path script = temp_dir / "case_lower.mdb";
    {
        std::ofstream out(script);
        ASSERT_TRUE(out.is_open());
        out << "create table(tl)\n";
        out << "use(tl)\n";
        out << "defattr(name:string,age:int)\n";
        out << "insert(1,Alice,20)\n";
        out << "count\n";
        out << "page(1,10,id,desc)\n";
    }

    ShellState st;
    st.data_dir = temp_dir.string();
    st.log_file_path = (temp_dir / "demo_log.bin").string();

    run_mdb_script(st, script.string().c_str());

    const fs::path data_file = temp_dir / "tl.bin";
    ASSERT_TRUE(fs::exists(data_file));

    newdb::TableSchema schema;
    const newdb::Status schema_st = newdb::load_schema_text(
        newdb::schema_sidecar_path_for_data_file(data_file.string()), schema);
    ASSERT_TRUE(schema_st.ok) << schema_st.message;

    newdb::HeapTable table;
    const newdb::Status load_st = newdb::io::load_heap_file(
        data_file.string().c_str(), "tl", schema, table, newdb::HeapLoadOptions{});
    ASSERT_TRUE(load_st.ok) << load_st.message;
    ASSERT_EQ(table.rows.size(), 1u);

    const std::string log_text = read_all_text(temp_dir / "demo_log.bin");
    EXPECT_EQ(log_text.find("[ERR] unknown command"), std::string::npos);
    EXPECT_NE(log_text.find("[COUNT]"), std::string::npos);
    EXPECT_NE(log_text.find("[PAGE] table="), std::string::npos);

    std::error_code ec;
    fs::remove_all(temp_dir, ec);
}

TEST(DemoMdb, DelattrPersistsSchemaSidecarAndRemovesColumn) {
    const fs::path temp_dir = newdb::test::unique_temp_subdir("newdb_mdb_delattr_schema");
    ASSERT_TRUE(fs::create_directories(temp_dir));

    const fs::path script = temp_dir / "case_delattr.mdb";
    {
        std::ofstream out(script);
        ASSERT_TRUE(out.is_open());
        out << "CREATE TABLE(tda)\n";
        out << "USE(tda)\n";
        out << "DEFATTR(name:string,age:int)\n";
        out << "INSERT(1,Alice,20)\n";
        out << "INSERT(2,Bob,30)\n";
        out << "DELATTR(age)\n";
    }

    ShellState st;
    st.data_dir = temp_dir.string();
    st.log_file_path = (temp_dir / "demo_log.bin").string();

    run_mdb_script(st, script.string().c_str());

    const fs::path data_file = temp_dir / "tda.bin";
    ASSERT_TRUE(fs::exists(data_file));

    newdb::TableSchema schema;
    const newdb::Status schema_st = newdb::load_schema_text(
        newdb::schema_sidecar_path_for_data_file(data_file.string()), schema);
    ASSERT_TRUE(schema_st.ok) << schema_st.message;

    EXPECT_EQ(schema.find_attr("age"), nullptr);

    const std::string sidecar_text = read_all_text(newdb::schema_sidecar_path_for_data_file(data_file.string()));
    EXPECT_EQ(sidecar_text.find("age:"), std::string::npos);

    const std::string log_text = read_all_text(temp_dir / "demo_log.bin");
    EXPECT_NE(log_text.find("[DELATTR] ok: key=age"), std::string::npos);

    std::error_code ec;
    fs::remove_all(temp_dir, ec);
}

