#pragma once

#include <newdb/error.h>
#include <newdb/row.h>

#include <cstddef>
#include <vector>

namespace newdb::codec {

// Encode one logical row into heap record bytes (payload placed in a page slot).
Status encode_row_to_heap_payload(const Row& row, std::vector<unsigned char>& out_payload);

// Decode one heap record payload into a Row (inverse of encode_row_to_heap_payload).
bool decode_heap_payload_to_row(const unsigned char* payload, std::size_t len, Row& out_row);

} // namespace newdb::codec
