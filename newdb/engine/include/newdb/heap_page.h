#pragma once

#include <cstddef>
#include <functional>
#include <vector>

namespace newdb::heap_page {

// Physical heap page size (bytes). Implemented by the active backend (Waterfall v1).
[[nodiscard]] std::size_t byte_size();

// Zero-filled page with valid heap header and checksum.
[[nodiscard]] std::vector<unsigned char> allocate_fresh_page();

// `page` must point to a buffer of length `byte_size()`.
[[nodiscard]] bool verify_checksum(const unsigned char* page, std::size_t length);

void update_checksum(std::vector<unsigned char>& page);

// One stored record inside a page (payload is the encoded tuple bytes for that slot).
struct RecordSlice {
    unsigned slot_index{};
    const unsigned char* payload{nullptr};
    std::size_t payload_length{0};
};

// Walks valid slots on a page whose CRC already matches. Visitor returns false to stop.
bool walk_record_slices(const std::vector<unsigned char>& page,
                         const std::function<bool(const RecordSlice&)>& visitor);

// Same as overload above but reads directly from `page` without copying (length must equal byte_size()).
bool walk_record_slices(const unsigned char* page,
                         std::size_t length,
                         const std::function<bool(const RecordSlice&)>& visitor);

// Append one encoded record payload; returns false if the page has no room.
bool append_encoded_record(std::vector<unsigned char>& page,
                           const unsigned char* encoded,
                           std::size_t encoded_length);

} // namespace newdb::heap_page
