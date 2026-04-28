#pragma once

#include <cstdint>

namespace newdb {

// Physical location of one encoded heap row (page-local offset + length).
struct HeapRowFinger {
    std::uint64_t page_no{};
    std::uint32_t byte_off_in_page{};
    std::uint32_t payload_len{};
};

// Options for `io::load_heap_file(..., opts)`.
struct HeapLoadOptions {
    // When true: keep rows on disk / mmap and expose them via `HeapTable` storage slots
    // (`decode_heap_slot` / `materialize_all_rows`). When false: fill `HeapTable::rows` eagerly
    // (classic behaviour).
    bool lazy_decode{false};
};

} // namespace newdb
