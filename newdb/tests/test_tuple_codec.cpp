#include <newdb/tuple_codec.h>

#include <gtest/gtest.h>

using newdb::Row;
using newdb::codec::decode_heap_payload_to_row;
using newdb::codec::encode_row_to_heap_payload;

TEST(TupleCodec, EncodeDecodeRoundTrip) {
    Row in;
    in.id = 42;
    in.attrs["balance"] = "7";
    in.attrs["name"] = "x";

    std::vector<unsigned char> enc;
    ASSERT_TRUE(encode_row_to_heap_payload(in, enc).ok);
    Row out;
    ASSERT_TRUE(decode_heap_payload_to_row(enc.data(), enc.size(), out));
    EXPECT_EQ(out.id, 42);
    EXPECT_EQ(out.attrs.at("balance"), "7");
    EXPECT_EQ(out.attrs.at("name"), "x");
}

TEST(TupleCodec, DecodeGarbageReturnsFalse) {
    const unsigned char junk[] = {0xff, 0xfe, 0xfd};
    Row out;
    EXPECT_FALSE(decode_heap_payload_to_row(junk, sizeof(junk), out));
}

TEST(TupleCodec, EncodeIdOnlyRoundTrip) {
    Row in;
    in.id = 99;
    std::vector<unsigned char> enc;
    ASSERT_TRUE(encode_row_to_heap_payload(in, enc).ok);
    Row out;
    ASSERT_TRUE(decode_heap_payload_to_row(enc.data(), enc.size(), out));
    EXPECT_EQ(out.id, 99);
    EXPECT_TRUE(out.attrs.empty());
}

TEST(TupleCodec, DecodeRejectsZeroId) {
    Row in;
    in.id = 0;
    in.attrs["x"] = "y";
    std::vector<unsigned char> enc;
    ASSERT_TRUE(encode_row_to_heap_payload(in, enc).ok);
    Row out;
    EXPECT_FALSE(decode_heap_payload_to_row(enc.data(), enc.size(), out));
}
