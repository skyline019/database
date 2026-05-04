#include <gtest/gtest.h>

#include <newdb/heap_table.h>
#include <newdb/memory_budget.h>
#include <newdb/page_cache.h>
#include <newdb/page_io.h>
#include <newdb/row.h>
#include <newdb/schema.h>

#include <cstdlib>
#include <filesystem>
#include <vector>

#if defined(_WIN32)
#include <stdlib.h>
#endif

namespace {

newdb::TableSchema minimal_schema() {
    newdb::TableSchema s;
    s.primary_key = "id";
    s.attrs = {newdb::AttrMeta{"balance", newdb::AttrType::String}};
    return s;
}

void set_page_cache_max_bytes(const char* val) {
#if defined(_WIN32)
    if (val == nullptr || val[0] == '\0') {
        (void)_putenv_s("NEWDB_PAGE_CACHE_MAX_BYTES", "");
    } else {
        (void)_putenv_s("NEWDB_PAGE_CACHE_MAX_BYTES", val);
    }
#else
    if (val == nullptr || val[0] == '\0') {
        unsetenv("NEWDB_PAGE_CACHE_MAX_BYTES");
    } else {
        setenv("NEWDB_PAGE_CACHE_MAX_BYTES", val, 1);
    }
#endif
}

} // namespace

TEST(PageCache, FreadHeapSecondOpenSharesGlobalCacheWhenEnabled) {
    namespace fs = std::filesystem;
    std::error_code ec;
    fs::path base = fs::temp_directory_path(ec);
    if (ec) {
        base = fs::current_path(ec);
    }
    const fs::path dir = base / "newdb_page_cache_test";
    fs::create_directories(dir, ec);
    const std::string path = (dir / "shared.bin").string();

    const std::vector<newdb::Row> seed = {
        newdb::Row{1, {{"balance", "100"}}, {}},
        newdb::Row{2, {{"balance", "200"}}, {}},
    };
    ASSERT_TRUE(newdb::io::create_heap_file(path.c_str(), seed).ok);

    set_page_cache_max_bytes("2097152");
    newdb::page_cache_reset_stats_for_test();

    const newdb::TableSchema schema = minimal_schema();
    newdb::HeapLoadOptions opts{};
    opts.lazy_decode = true;

    newdb::HeapTable t1;
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "shared", schema, t1, opts).ok);
    newdb::Row r1;
    ASSERT_TRUE(t1.decode_heap_slot(0, r1));

    newdb::HeapTable t2;
    ASSERT_TRUE(newdb::io::load_heap_file(path.c_str(), "shared", schema, t2, opts).ok);
    newdb::Row r2;
    ASSERT_TRUE(t2.decode_heap_slot(0, r2));

    const newdb::PageCacheGlobalStats st = newdb::page_cache_global_stats();
    if (st.misses > 0) {
        EXPECT_GE(st.hits, 1u);
    }

    set_page_cache_max_bytes("");
    fs::remove_all(dir, ec);
}

TEST(PageCache, EvictionAccumulatesBytesEvictedTotal) {
    set_page_cache_max_bytes("96");
    newdb::page_cache_reset_stats_for_test();
    const std::string path = "/tmp/newdb_page_cache_evict.bin";
    std::vector<unsigned char> p40(40, 1);
    newdb::page_cache_put(path, 0, p40.size(), p40.data());
    newdb::page_cache_put(path, 1, p40.size(), p40.data());
    newdb::page_cache_put(path, 2, p40.size(), p40.data());
    const newdb::PageCacheGlobalStats st = newdb::page_cache_global_stats();
    EXPECT_GE(st.evictions, 1u);
    EXPECT_GE(st.bytes_evicted_total, 40u);
    EXPECT_LE(st.bytes_in_cache, 96u);
    const newdb::MemoryBudgetSnapshot mb = newdb::memory_budget_snapshot();
    EXPECT_EQ(mb.bytes_evicted_total, st.bytes_evicted_total);
    set_page_cache_max_bytes("");
}

TEST(PageCache, PutRejectsWhenPageExceedsCap) {
    set_page_cache_max_bytes("64");
    newdb::page_cache_reset_stats_for_test();
    std::vector<unsigned char> big(128, 7);
    newdb::page_cache_put("/tmp/reject_test.bin", 0, big.size(), big.data());
    const newdb::PageCacheGlobalStats st = newdb::page_cache_global_stats();
    EXPECT_GE(st.reject_oversized_page, 1u);
    EXPECT_EQ(st.bytes_in_cache, 0u);
    set_page_cache_max_bytes("");
}
