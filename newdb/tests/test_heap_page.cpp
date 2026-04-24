#include <newdb/heap_page.h>

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

TEST(HeapPage, ByteSizePositive) {
    EXPECT_GT(newdb::heap_page::byte_size(), 0u);
}

TEST(HeapPage, FreshPageChecksumRoundTrip) {
    std::vector<unsigned char> page = newdb::heap_page::allocate_fresh_page();
    ASSERT_EQ(page.size(), newdb::heap_page::byte_size());
    EXPECT_FALSE(newdb::heap_page::verify_checksum(page.data(), page.size()));
    newdb::heap_page::update_checksum(page);
    EXPECT_TRUE(newdb::heap_page::verify_checksum(page.data(), page.size()));
}

TEST(HeapPage, WalkEmptyPageNoVisitorCalls) {
    std::vector<unsigned char> page = newdb::heap_page::allocate_fresh_page();
    newdb::heap_page::update_checksum(page);
    int calls = 0;
    ASSERT_TRUE(newdb::heap_page::walk_record_slices(page, [&](const newdb::heap_page::RecordSlice&) {
        ++calls;
        return true;
    }));
    EXPECT_EQ(calls, 0);
}

TEST(HeapPage, AppendAndWalkOneRecord) {
    std::vector<unsigned char> page = newdb::heap_page::allocate_fresh_page();
    const unsigned char blob[] = {0x01, 0x02, 0x03, 0x04};
    ASSERT_TRUE(newdb::heap_page::append_encoded_record(page, blob, sizeof(blob)));
    newdb::heap_page::update_checksum(page);
    ASSERT_TRUE(newdb::heap_page::verify_checksum(page.data(), page.size()));
    int seen = 0;
    ASSERT_TRUE(newdb::heap_page::walk_record_slices(page, [&](const newdb::heap_page::RecordSlice& s) -> bool {
        ++seen;
        EXPECT_EQ(s.payload_length, sizeof(blob));
        EXPECT_EQ(std::memcmp(s.payload, blob, sizeof(blob)), 0);
        return true;
    }));
    EXPECT_EQ(seen, 1);
}

TEST(HeapPage, WrongLengthFailsVerify) {
    std::vector<unsigned char> page(16);
    EXPECT_FALSE(newdb::heap_page::verify_checksum(page.data(), page.size()));
}
