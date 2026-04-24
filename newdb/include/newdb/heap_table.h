#pragma once

#include <newdb/heap_storage.h>
#include <newdb/mvcc.h>
#include <newdb/row.h>
#include <newdb/schema.h>

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace newdb {

class HeapFileReadView;

enum class SortDir { Asc, Desc };

struct HeapTable {
    std::string name;
    std::vector<Row> rows;
    std::unordered_map<int, std::size_t> index_by_id;
    std::unordered_map<std::string, std::size_t> index_by_pk_value;
    std::unordered_map<std::string, std::size_t> attr_index;
    std::map<std::string, std::vector<std::size_t>> sorted_cache_asc;
    std::map<std::string, std::vector<std::size_t>> sorted_cache_desc;

    // When `load_heap_file(..., HeapLoadOptions{.lazy_decode=true})` succeeds, rows stay empty and
    // logical rows are served from `heap_file_` + finger table (mmap or fread per page).
    std::shared_ptr<const HeapFileReadView> heap_file_;
    std::vector<int> heap_sorted_ids_;
    std::vector<HeapRowFinger> heap_fingers_;
    mutable Row heap_find_scratch_;
    mutable std::size_t heap_cached_page_no_ = static_cast<std::size_t>(-1);
    mutable std::vector<unsigned char> heap_cached_page_buf_;
    std::vector<RecordMetadata> row_meta;
    std::optional<MVCCSnapshot> active_snapshot;
    mutable std::uint64_t decode_heap_slot_calls{0};
    mutable std::uint64_t decode_heap_slot_hits{0};
    mutable std::uint64_t decode_heap_slot_misses{0};

    void clear_data();
    void set_snapshot(const MVCCSnapshot& snapshot) { active_snapshot = snapshot; }
    void clear_snapshot() { active_snapshot.reset(); }
    [[nodiscard]] bool is_row_visible(std::size_t slot, const Row& row) const;

    [[nodiscard]] bool is_heap_storage_backed() const noexcept {
        return heap_file_ != nullptr;
    }

    [[nodiscard]] std::size_t logical_row_count() const noexcept {
        return is_heap_storage_backed() ? heap_fingers_.size() : rows.size();
    }

    // Decode one logical row by slot index (same numbering as `index_by_id` values when storage-backed).
    [[nodiscard]] bool decode_heap_slot(std::size_t slot, Row& out) const;

    // Expand storage-backed rows into `rows` and drop the mmap/file view (classic in-memory table).
    [[nodiscard]] Status materialize_all_rows(const TableSchema& schema);

    // Called by `io::load_heap_file` when `HeapLoadOptions::lazy_decode` is set.
    void adopt_lazy_heap_storage(std::string table_name,
                                 std::shared_ptr<const HeapFileReadView> view,
                                 std::vector<int> sorted_ids,
                                 std::vector<HeapRowFinger> fingers,
                                 std::vector<RecordMetadata> metadata,
                                 const TableSchema& schema);

    // Drop mmap/file view without touching `rows` (used before eager materialize in `load_heap_file`).
    void discard_lazy_storage_without_rebuild();

    void rebuild_indexes(const TableSchema& schema);

    const Row* find_by_id(int id) const;

    bool primary_key_value_exists(const TableSchema& schema,
                                  const std::string& pk,
                                  const std::string& val,
                                  int exclude_row_id) const;

    static bool row_get_pk_value(const Row& r,
                                 const std::string& pk,
                                 std::string& out_val);

    const std::vector<std::size_t>& sorted_indices(const TableSchema& schema,
                                                   const std::string& order_key,
                                                   SortDir dir);

    struct DecodeStats {
        std::uint64_t calls{0};
        std::uint64_t hits{0};
        std::uint64_t misses{0};
    };
    [[nodiscard]] DecodeStats decode_stats() const noexcept {
        return DecodeStats{decode_heap_slot_calls, decode_heap_slot_hits, decode_heap_slot_misses};
    }
    void reset_decode_stats() const noexcept {
        decode_heap_slot_calls = 0;
        decode_heap_slot_hits = 0;
        decode_heap_slot_misses = 0;
    }
};

} // namespace newdb
