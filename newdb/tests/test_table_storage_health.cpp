#include "cli/modules/storage/table_storage_health.h"

#include <newdb/heap_table.h>
#include <newdb/mvcc.h>

#include <gtest/gtest.h>

TEST(TableStorageHealth, TombstoneRatio) {
    newdb::HeapTable t;
    t.rows.resize(4);
    t.row_meta.resize(4);
    t.row_meta[1].deleted_lsn = 1;
    t.row_meta[3].is_tombstone = true;
    const newdb::TableStorageHealth h = newdb::measure_table_storage_health(t);
    EXPECT_EQ(h.logical_rows, 4u);
    EXPECT_EQ(h.physical_rows, 4u);
    EXPECT_EQ(h.tombstone_slots, 2u);
    EXPECT_EQ(h.tombstone_rows, 2u);
    EXPECT_DOUBLE_EQ(h.tombstone_ratio, 0.5);
    EXPECT_EQ(h.data_file_bytes, 0u);
    EXPECT_DOUBLE_EQ(h.fragmentation_ratio, 0.5);
}
