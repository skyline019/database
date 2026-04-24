#include <newdb/wal_codec.h>

#include <gtest/gtest.h>

TEST(WalCodec, BuildAndDecodeTableOnlyPayload) {
    std::vector<uint8_t> payload;
    ASSERT_TRUE(newdb::walcodec::build_payload("users", nullptr, nullptr, payload).ok);

    const uint8_t* p = payload.data();
    const uint8_t* end = payload.data() + payload.size();
    std::string table;
    ASSERT_TRUE(newdb::walcodec::decode_table_name(p, end, table).ok);
    EXPECT_EQ(table, "users");
    EXPECT_EQ(p, end);
}

TEST(WalCodec, BuildAndDecodeRowFieldsPayload) {
    const uint32_t row_id_in = 42;
    const std::vector<uint8_t> row_payload_in = {1, 2, 3, 4, 5};
    std::vector<uint8_t> payload;
    ASSERT_TRUE(newdb::walcodec::build_payload("users", &row_id_in, &row_payload_in, payload).ok);

    const uint8_t* p = payload.data();
    const uint8_t* end = payload.data() + payload.size();
    std::string table;
    ASSERT_TRUE(newdb::walcodec::decode_table_name(p, end, table).ok);
    EXPECT_EQ(table, "users");

    uint32_t row_id_out = 0;
    const uint8_t* row_payload_out = nullptr;
    uint32_t row_payload_len_out = 0;
    ASSERT_TRUE(newdb::walcodec::decode_row_fields(
                    p, end, row_id_out, row_payload_out, row_payload_len_out)
                    .ok);
    EXPECT_EQ(row_id_out, row_id_in);
    ASSERT_EQ(row_payload_len_out, row_payload_in.size());
    EXPECT_EQ(std::vector<uint8_t>(row_payload_out, row_payload_out + row_payload_len_out), row_payload_in);
}

TEST(WalCodec, RejectsMismatchedRowFieldPresence) {
    const uint32_t row_id = 10;
    std::vector<uint8_t> payload;
    EXPECT_FALSE(newdb::walcodec::build_payload("users", &row_id, nullptr, payload).ok);

    const std::vector<uint8_t> row_payload = {1, 2};
    EXPECT_FALSE(newdb::walcodec::build_payload("users", nullptr, &row_payload, payload).ok);
}

TEST(WalCodec, RejectsOverlongTableName) {
    std::string very_long(70000, 'a');
    std::vector<uint8_t> payload;
    EXPECT_FALSE(newdb::walcodec::build_payload(very_long, nullptr, nullptr, payload).ok);
}

TEST(WalCodec, DecodeFailsOnTruncatedTableName) {
    const std::vector<uint8_t> payload = {0x05, 0x00, 'u', 's'};
    const uint8_t* p = payload.data();
    const uint8_t* end = payload.data() + payload.size();
    std::string table;
    EXPECT_FALSE(newdb::walcodec::decode_table_name(p, end, table).ok);
}

TEST(WalCodec, DecodeFailsOnTruncatedRowHeader) {
    const std::vector<uint8_t> payload = {0x01, 0x00, 't', 0x02, 0x00};
    const uint8_t* p = payload.data();
    const uint8_t* end = payload.data() + payload.size();
    std::string table;
    ASSERT_TRUE(newdb::walcodec::decode_table_name(p, end, table).ok);
    uint32_t row_id = 0;
    const uint8_t* row_payload = nullptr;
    uint32_t row_len = 0;
    EXPECT_FALSE(newdb::walcodec::decode_row_fields(p, end, row_id, row_payload, row_len).ok);
}

TEST(WalCodec, DecodeFailsOnDeclaredPayloadOverflow) {
    const std::vector<uint8_t> payload = {
        0x01, 0x00, 't',                // table "t"
        0x2A, 0x00, 0x00, 0x00,         // row id = 42
        0x05, 0x00, 0x00, 0x00,         // payload len = 5
        0x01, 0x02                      // only 2 bytes present
    };
    const uint8_t* p = payload.data();
    const uint8_t* end = payload.data() + payload.size();
    std::string table;
    ASSERT_TRUE(newdb::walcodec::decode_table_name(p, end, table).ok);
    uint32_t row_id = 0;
    const uint8_t* row_payload = nullptr;
    uint32_t row_len = 0;
    EXPECT_FALSE(newdb::walcodec::decode_row_fields(p, end, row_id, row_payload, row_len).ok);
}
