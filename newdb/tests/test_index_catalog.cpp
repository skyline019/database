#include "cli/modules/sidecar/common/index_catalog.h"

#include <gtest/gtest.h>
#include <set>

TEST(IndexCatalog, ExplainDescriptorMismatch) {
    IndexDescriptor d{.table_name = "t",
                      .index_name = "i",
                      .kind = IndexKind::Eq,
                      .data_lsn = 9,
                      .schema_version = 0,
                      .built_at_ms = 0,
                      .valid = true};
    EXPECT_TRUE(explain_index_descriptor_mismatch(d, 0, 10).empty());
    EXPECT_NE(explain_index_descriptor_mismatch(d, 0, 3).find("data_lsn"), std::string::npos);
    IndexDescriptor d2 = d;
    d2.schema_version = 4;
    EXPECT_NE(explain_index_descriptor_mismatch(d2, 5, 10).find("schema_version"), std::string::npos);
}

TEST(IndexCatalog, InvalidateIncrementsRequestCounter) {
    const std::uint64_t before = index_catalog_sidecar_invalidate_request_count();
    sidecar_invalidate_all_indexes_for_data_file("/nonexistent/path/table.bin");
    EXPECT_GE(index_catalog_sidecar_invalidate_request_count(), before + 1u);
}

TEST(IndexCatalog, DescriptorMatchesRuntime) {
    IndexDescriptor d{.table_name = "t",
                      .index_name = "eq_color",
                      .kind = IndexKind::Eq,
                      .data_lsn = 5,
                      .schema_version = 0,
                      .built_at_ms = 0,
                      .valid = true};
    EXPECT_TRUE(index_descriptor_matches_runtime(d, 0, 10));
    EXPECT_FALSE(index_descriptor_matches_runtime(d, 0, 3));
    IndexDescriptor d2 = d;
    d2.schema_version = 7;
    EXPECT_TRUE(index_descriptor_matches_runtime(d2, 7, 10));
    EXPECT_FALSE(index_descriptor_matches_runtime(d2, 8, 10));
}

TEST(IndexCatalog, InvalidateEmptyPathNoThrow) {
    sidecar_invalidate_all_indexes_for_data_file("");
    sidecar_invalidate_all_indexes_for_data_file("", std::set<std::string>{"x"});
}

TEST(IndexCatalog, Fnv1a64Stable) {
    EXPECT_EQ(index_catalog_fnv1a64("a"), index_catalog_fnv1a64("a"));
    EXPECT_NE(index_catalog_fnv1a64("a"), index_catalog_fnv1a64("b"));
}

TEST(IndexCatalog, SidecarMetaSuffixParseRoundTrip) {
    IndexCatalogParsedTail written{};
    const std::string suf = index_catalog_sidecar_meta_suffix(IndexKind::Eq, 42, 7, "/tmp/t.bin", "dept", &written);
    EXPECT_NE(suf.find(";idx_kind=eq"), std::string::npos);
    EXPECT_NE(suf.find(";idx_sv=42"), std::string::npos);
    EXPECT_NE(suf.find(";idx_dl=7"), std::string::npos);
    EXPECT_NE(suf.find(";bld=2"), std::string::npos);
    EXPECT_EQ(written.idx_sv, 42u);
    EXPECT_EQ(written.idx_dl, 7u);
    EXPECT_EQ(written.tbl_fnv, index_catalog_fnv1a64("/tmp/t.bin"));
    EXPECT_EQ(written.inx_fnv, index_catalog_fnv1a64("dept"));
    EXPECT_EQ(written.catalog_build_state, static_cast<std::uint8_t>(2));
    IndexCatalogParsedTail parsed{};
    index_catalog_parse_header_tail("prefix" + suf, parsed);
    EXPECT_EQ(parsed.idx_sv, written.idx_sv);
    EXPECT_EQ(parsed.idx_dl, written.idx_dl);
    EXPECT_EQ(parsed.tbl_fnv, written.tbl_fnv);
    EXPECT_EQ(parsed.inx_fnv, written.inx_fnv);
    EXPECT_EQ(parsed.built_ms, written.built_ms);
    EXPECT_EQ(parsed.catalog_build_state, static_cast<std::uint8_t>(2));
    EXPECT_TRUE(index_catalog_tail_identity_matches(parsed, "/tmp/t.bin", "dept"));
    EXPECT_FALSE(index_catalog_tail_identity_matches(parsed, "/other/t.bin", "dept"));
}

TEST(IndexCatalog, PlaintextSuffixRoundTrip) {
    IndexCatalogParsedTail written{};
    const std::string suf = index_catalog_sidecar_meta_suffix(IndexKind::Eq, 1, 2, "/tmp/users.bin", "city", &written,
                                                               "users", "city");
    EXPECT_NE(suf.find(";tbl_n="), std::string::npos);
    EXPECT_NE(suf.find(";inx_n="), std::string::npos);
    const std::string hdr = "x" + suf;
    IndexCatalogPlaintextNames plain{};
    index_catalog_parse_plaintext_names(hdr, plain);
    ASSERT_TRUE(plain.has_plaintext);
    EXPECT_FALSE(plain.malformed_plaintext);
    EXPECT_EQ(plain.table_name, "users");
    EXPECT_EQ(plain.index_name, "city");
    IndexCatalogParsedTail tail{};
    index_catalog_parse_header_tail(hdr, tail);
    EXPECT_TRUE(index_catalog_header_identity_ok(hdr, tail, "/tmp/users.bin", "city", "users", "city", IndexKind::Eq));
    EXPECT_FALSE(index_catalog_header_identity_ok(hdr, tail, "/tmp/users.bin", "city", "other", "city", IndexKind::Eq));
}

TEST(IndexCatalog, PctEncodeSemicolon) {
    const std::string enc = index_catalog_pct_encode("a;b");
    EXPECT_EQ(index_catalog_pct_decode(enc), "a;b");
}

TEST(IndexCatalog, BuildStateBuildingVsReadyParsed) {
    IndexCatalogParsedTail building{};
    index_catalog_parse_header_tail("hdr;bld=1;idx_sv=9;idx_dl=3", building);
    EXPECT_EQ(building.catalog_build_state, static_cast<std::uint8_t>(1));
    EXPECT_EQ(building.idx_sv, 9u);
    IndexCatalogParsedTail ready{};
    index_catalog_parse_header_tail("hdr;bld=2;idx_sv=9;idx_dl=3", ready);
    EXPECT_EQ(ready.catalog_build_state, static_cast<std::uint8_t>(2));
    EXPECT_EQ(ready.idx_sv, building.idx_sv);
}

TEST(IndexCatalog, ParseBuildStateFromBldToken) {
    IndexCatalogParsedTail t1{};
    index_catalog_parse_header_tail("hdr;bld=1", t1);
    EXPECT_EQ(t1.catalog_build_state, static_cast<std::uint8_t>(1));
    IndexCatalogParsedTail t2{};
    index_catalog_parse_header_tail("hdr;bld=2", t2);
    EXPECT_EQ(t2.catalog_build_state, static_cast<std::uint8_t>(2));
    IndexCatalogParsedTail t0{};
    index_catalog_parse_header_tail("hdr;idx_sv=1", t0);
    EXPECT_EQ(t0.catalog_build_state, static_cast<std::uint8_t>(0));
}

TEST(IndexCatalog, InferTablePlainFromDataFile) {
    EXPECT_EQ(index_catalog_infer_table_plain_from_data_file("t.bin"), "t");
    EXPECT_TRUE(index_catalog_infer_table_plain_from_data_file("noext").empty());
}
