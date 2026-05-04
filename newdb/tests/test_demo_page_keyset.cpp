#include "cli/modules/common/view/table_view.h"

#include <newdb/heap_table.h>

#include <gtest/gtest.h>

TEST(PageKeyset, AscSkipsUpToAfterId) {
    newdb::HeapTable tbl;
    tbl.rows = {
        newdb::Row{1, {{"n", "a"}}, {}},
        newdb::Row{3, {{"n", "b"}}, {}},
        newdb::Row{5, {{"n", "c"}}, {}},
        newdb::Row{7, {{"n", "d"}}, {}},
    };
    const std::vector<std::size_t> sorted{0, 1, 2, 3};
    const auto slice = table_view::page_indices_keyset_after_id(tbl, sorted, false, 3, true);
    ASSERT_EQ(slice.size(), 2u);
    EXPECT_EQ(slice[0], 2u);
    EXPECT_EQ(slice[1], 3u);
}

TEST(PageKeyset, DescSkipsDownToAfterId) {
    newdb::HeapTable tbl;
    tbl.rows = {
        newdb::Row{7, {{"n", "d"}}, {}},
        newdb::Row{5, {{"n", "c"}}, {}},
        newdb::Row{3, {{"n", "b"}}, {}},
        newdb::Row{1, {{"n", "a"}}, {}},
    };
    const std::vector<std::size_t> sorted{0, 1, 2, 3};
    const auto slice = table_view::page_indices_keyset_after_id(tbl, sorted, true, 5, true);
    ASSERT_EQ(slice.size(), 2u);
    EXPECT_EQ(slice[0], 2u);
    EXPECT_EQ(slice[1], 3u);
}

TEST(PageKeyset, NoKeysetReturnsCopy) {
    newdb::HeapTable tbl;
    tbl.rows = {newdb::Row{1, {}, {}}, newdb::Row{2, {}, {}}};
    const std::vector<std::size_t> sorted{0, 1};
    const auto slice = table_view::page_indices_keyset_after_id(tbl, sorted, false, 99, false);
    ASSERT_EQ(slice.size(), 2u);
}
