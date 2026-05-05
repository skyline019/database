#include <gtest/gtest.h>

#include <newdb/memory_registry.h>

#include <atomic>
#include <cstdlib>

namespace {

void set_env(const char* k, const char* v) {
#if defined(_WIN32)
    if (v == nullptr || v[0] == '\0') {
        (void)_putenv_s(k, "");
    } else {
        (void)_putenv_s(k, v);
    }
#else
    if (v == nullptr || v[0] == '\0') {
        unsetenv(k);
    } else {
        setenv(k, v, 1);
    }
#endif
}

class MemoryRegistryTest : public ::testing::Test {
protected:
    void SetUp() override {
        newdb::memory_registry_reset_for_test();
        set_env("NEWDB_PAGE_CACHE_MAX_BYTES", "");
        set_env("NEWDB_SIDECAR_CACHE_MAX_BYTES", "");
        set_env("NEWDB_QUERY_TEMP_MAX_BYTES", "");
        set_env("NEWDB_MEMORY_BUDGET_MAX_BYTES", "");
    }
    void TearDown() override {
        SetUp();
    }
};

}  // namespace

TEST_F(MemoryRegistryTest, AdmitReleaseRoundTripWithoutCap) {
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::PageCache, 64));
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::EqSidecar, 32));
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, 16));
    auto t = newdb::memory_registry_totals();
    EXPECT_EQ(t.page_cache_used_bytes, 64u);
    EXPECT_EQ(t.sidecar_used_bytes, 32u);
    EXPECT_EQ(t.query_temp_used_bytes, 16u);
    EXPECT_EQ(t.global_used_bytes, 112u);
    newdb::memory_registry_release(newdb::MemoryKind::PageCache, 64);
    newdb::memory_registry_release(newdb::MemoryKind::EqSidecar, 32);
    newdb::memory_registry_release(newdb::MemoryKind::QueryTemp, 16);
    t = newdb::memory_registry_totals();
    EXPECT_EQ(t.global_used_bytes, 0u);
}

TEST_F(MemoryRegistryTest, OversizeKindRejectsAndCountsAdmitReject) {
    set_env("NEWDB_SIDECAR_CACHE_MAX_BYTES", "128");
    EXPECT_FALSE(newdb::memory_registry_try_admit(newdb::MemoryKind::EqSidecar, 256));
    auto t = newdb::memory_registry_totals();
    EXPECT_EQ(t.sidecar_admit_rejects, 1u);
    EXPECT_GE(t.global_admit_rejects, 1u);
    EXPECT_EQ(t.sidecar_used_bytes, 0u);
}

TEST_F(MemoryRegistryTest, EvictorRunsToFreeKindCap) {
    set_env("NEWDB_QUERY_TEMP_MAX_BYTES", "128");
    std::atomic<int> evictor_calls{0};
    newdb::memory_registry_register_evictor(
        newdb::MemoryKind::QueryTemp, [&evictor_calls](std::uint64_t target) {
            ++evictor_calls;
            const std::uint64_t freed = std::min<std::uint64_t>(target, 64);
            newdb::memory_registry_release(newdb::MemoryKind::QueryTemp, freed);
            newdb::memory_registry_record_eviction(newdb::MemoryKind::QueryTemp, freed);
            return freed;
        });
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, 96));
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::QueryTemp, 64));
    EXPECT_GE(evictor_calls.load(), 1);
    auto t = newdb::memory_registry_totals();
    EXPECT_GE(t.query_temp_evictions, 1u);
    EXPECT_LE(t.query_temp_used_bytes, 128u);
}

TEST_F(MemoryRegistryTest, CrossKindEvictionViaGlobalCap) {
    set_env("NEWDB_MEMORY_BUDGET_MAX_BYTES", "128");
    std::atomic<int> sidecar_evicts{0};
    newdb::memory_registry_register_evictor(
        newdb::MemoryKind::EqSidecar, [&sidecar_evicts](std::uint64_t target) {
            ++sidecar_evicts;
            const std::uint64_t freed = std::min<std::uint64_t>(target, 96);
            newdb::memory_registry_release(newdb::MemoryKind::EqSidecar, freed);
            newdb::memory_registry_record_eviction(newdb::MemoryKind::EqSidecar, freed);
            return freed;
        });
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::EqSidecar, 96));
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::PageCache, 64));
    EXPECT_GE(sidecar_evicts.load(), 1);
    auto t = newdb::memory_registry_totals();
    EXPECT_GE(t.sidecar_evictions, 1u);
    EXPECT_LE(t.global_used_bytes, 128u);
    EXPECT_EQ(t.page_cache_used_bytes, 64u);
}

TEST_F(MemoryRegistryTest, NoCapAdmitsAlwaysSucceed) {
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::PageCache, 1ULL << 30));
    EXPECT_TRUE(newdb::memory_registry_try_admit(newdb::MemoryKind::EqSidecar, 1ULL << 30));
    auto t = newdb::memory_registry_totals();
    EXPECT_EQ(t.global_admit_rejects, 0u);
}
