#pragma once

#include <newdb/error.h>

#include <cstdint>
#include <string>
#include <vector>

namespace newdb::walcodec {

// Encodes table + optional row fields into WAL payload bytes.
// Layout:
// [u16 table_name_len][table_name_bytes][optional: u32 row_id][optional: u32 row_payload_len][row_payload]
Status build_payload(const std::string& table,
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

} // namespace newdb::walcodec
