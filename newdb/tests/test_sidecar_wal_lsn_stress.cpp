#include "sidecar_wal_lsn.h"
#include "txn_manager.h"

#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>

namespace fs = std::filesystem;

TEST(SidecarWalLsn, EpochFileRoundTrip) {
    const fs::path base = fs::temp_directory_path() / ("wal_lsn_epoch_" + std::to_string(std::rand()));
    fs::create_directories(base);
    const std::string dir = base.string();
    EXPECT_EQ(read_wal_lsn_for_workspace(dir), 0u);
    write_wal_lsn_for_workspace(dir, 12345u);
    EXPECT_EQ(read_wal_lsn_for_workspace(dir), 12345u);
    write_wal_lsn_for_workspace(dir, 12346u);
    EXPECT_EQ(read_wal_lsn_for_workspace(dir), 12346u);
    fs::remove_all(base);
}

TEST(TxnCoordinatorStress, WalCompactAfterCommit) {
#ifdef _WIN32
    _putenv_s("NEWDB_WAL_COMPACT_BYTES", "900");
#else
    setenv("NEWDB_WAL_COMPACT_BYTES", "900", 1);
#endif
    const fs::path base = fs::temp_directory_path() / ("txn_compact_" + std::to_string(std::rand()));
    fs::create_directories(base);
    const std::string dir = base.string();
    {
        TxnCoordinator coord;
        coord.set_workspace_root(dir);
        for (int i = 0; i < 40; ++i) {
            auto b = coord.begin("stress_t");
            ASSERT_TRUE(b.isOk());
            auto c = coord.commit();
            ASSERT_TRUE(c.isOk());
        }
        const std::string wal = (fs::path(dir) / "demodb.wal").string();
        ASSERT_TRUE(fs::exists(wal));
        const auto sz = fs::file_size(wal);
        EXPECT_LT(sz, static_cast<std::uintmax_t>(5000)) << "WAL should be compacted below loose cap";
    } // close WAL / release file handles before removing temp tree

    fs::remove_all(base);
#ifdef _WIN32
    (void)_putenv("NEWDB_WAL_COMPACT_BYTES=");
#else
    unsetenv("NEWDB_WAL_COMPACT_BYTES");
#endif
}
