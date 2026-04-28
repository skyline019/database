#pragma once

#include <newdb/error.h>

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

namespace newdb::walcodec {

struct RowImageView {
    const uint32_t* row_id{nullptr};
    const std::vector<uint8_t>* row_payload{nullptr};
};

struct PayloadMetaView {
    const std::uint64_t* db_object_id{nullptr};
    const std::uint64_t* savepoint_id{nullptr};
    const std::uint64_t* undo_prev_lsn{nullptr};
    const std::uint64_t* pitr_target_lsn{nullptr};
    const std::uint64_t* pitr_target_ts_ms{nullptr};
    const std::uint64_t* record_ts_ms{nullptr};
};

// Legacy v0 payload:
// [u16 table_name_len][table_name_bytes][optional: u32 row_id][optional: u32 row_payload_len][row_payload]
// Versioned v1 payload:
// [u8 marker=0xA1][u16 table_name_len][table_name_bytes][u8 flags][u64 op_seq]
// [optional before image: u32 row_id + u32 len + bytes]
// [optional after image:  u32 row_id + u32 len + bytes]
Status build_payload(const std::string& table,
                     const RowImageView* before_image,
                     const RowImageView* after_image,
                     const PayloadMetaView* meta,
                     std::uint64_t op_seq_in_txn,
                     std::vector<uint8_t>& out);
Status build_payload(const std::string& table,
                     const uint32_t* row_id,
                     const std::vector<uint8_t>* row_payload,
                     std::vector<uint8_t>& out);

Status build_payload_legacy(const std::string& table,
                            const uint32_t* row_id,
                            const std::vector<uint8_t>* row_payload,
                            std::vector<uint8_t>& out);

Status decode_table_name(const uint8_t*& p, const uint8_t* end, std::string& out);

Status decode_row_fields(const uint8_t*& p,
                         const uint8_t* end,
                         uint32_t& row_id,
                         const uint8_t*& row_payload,
                         uint32_t& row_payload_len);

Status decode_u32(const uint8_t*& p, const uint8_t* end, uint32_t& out);
Status decode_u64(const uint8_t*& p, const uint8_t* end, std::uint64_t& out);

struct DecodedPayloadV1 {
    std::string table;
    bool has_before{false};
    uint32_t before_row_id{0};
    const uint8_t* before_row_payload{nullptr};
    uint32_t before_row_payload_len{0};
    bool has_after{false};
    uint32_t after_row_id{0};
    const uint8_t* after_row_payload{nullptr};
    uint32_t after_row_payload_len{0};
    std::uint64_t op_seq_in_txn{0};
    bool has_db_object_id{false};
    std::uint64_t db_object_id{0};
    bool has_savepoint_id{false};
    std::uint64_t savepoint_id{0};
    bool has_undo_prev_lsn{false};
    std::uint64_t undo_prev_lsn{0};
    bool has_pitr_target_lsn{false};
    std::uint64_t pitr_target_lsn{0};
    bool has_pitr_target_ts_ms{false};
    std::uint64_t pitr_target_ts_ms{0};
    bool has_record_ts_ms{false};
    std::uint64_t record_ts_ms{0};
};

bool is_v1_payload(const uint8_t* data, std::size_t len);
Status decode_payload_v1(const uint8_t* data, std::size_t len, DecodedPayloadV1& out);

} // namespace newdb::walcodec
