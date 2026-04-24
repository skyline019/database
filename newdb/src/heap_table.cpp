#include <newdb/heap_table.h>

#include <newdb/error.h>
#include <newdb/heap_file_read_view.h>
#include <newdb/heap_page.h>
#include <newdb/tuple_codec.h>

#include <algorithm>
#include <cstddef>
#include <numeric>
#include <unordered_map>

namespace newdb {

namespace {

// Cap per-direction sort caches so PAGE with many different order keys does not grow unbounded.
constexpr std::size_t kMaxSortedCacheKeys = 16;

void trim_sorted_cache_before_insert(std::map<std::string, std::vector<std::size_t>>& cache) {
    while (cache.size() >= kMaxSortedCacheKeys) {
        cache.erase(cache.begin());
    }
}

bool is_tombstone_row(const Row& r) {
    const auto it = r.attrs.find("__deleted");
    return it != r.attrs.end() && it->second == "1";
}

void strip_mvcc_internal_attrs(Row& row) {
    row.attrs.erase("__mvcc_created_lsn");
    row.attrs.erase("__mvcc_deleted_lsn");
    row.attrs.erase("__mvcc_txn_id");
}

} // namespace

void HeapTable::discard_lazy_storage_without_rebuild() {
    heap_file_.reset();
    heap_sorted_ids_.clear();
    heap_fingers_.clear();
    heap_find_scratch_ = Row{};
    heap_cached_page_no_ = static_cast<std::size_t>(-1);
    heap_cached_page_buf_.clear();
    row_meta.clear();
}

void HeapTable::adopt_lazy_heap_storage(std::string table_name,
                                        std::shared_ptr<const HeapFileReadView> view,
                                        std::vector<int> sorted_ids,
                                        std::vector<HeapRowFinger> fingers,
                                        std::vector<RecordMetadata> metadata,
                                        const TableSchema& schema) {
    rows.clear();
    rows.shrink_to_fit();
    index_by_id.clear();
    index_by_pk_value.clear();
    attr_index.clear();
    sorted_cache_asc.clear();
    sorted_cache_desc.clear();

    name = std::move(table_name);
    heap_file_ = std::move(view);
    heap_sorted_ids_ = std::move(sorted_ids);
    heap_fingers_ = std::move(fingers);
    heap_cached_page_no_ = static_cast<std::size_t>(-1);
    heap_cached_page_buf_.clear();
    row_meta = std::move(metadata);
    if (row_meta.size() != heap_fingers_.size()) {
        row_meta.assign(heap_fingers_.size(), RecordMetadata{});
    }
    rebuild_indexes(schema);
}

void HeapTable::clear_data() {
    rows.clear();
    rows.shrink_to_fit();
    index_by_id.clear();
    index_by_pk_value.clear();
    attr_index.clear();
    sorted_cache_asc.clear();
    sorted_cache_desc.clear();
    heap_file_.reset();
    heap_sorted_ids_.clear();
    heap_fingers_.clear();
    heap_find_scratch_ = Row{};
    heap_cached_page_no_ = static_cast<std::size_t>(-1);
    heap_cached_page_buf_.clear();
    row_meta.clear();
    active_snapshot.reset();
}

bool HeapTable::is_row_visible(const std::size_t slot, const Row& row) const {
    if (is_tombstone_row(row)) {
        return false;
    }
    if (!active_snapshot.has_value()) {
        return true;
    }
    const RecordMetadata* meta = nullptr;
    if (slot < row_meta.size()) {
        meta = &row_meta[slot];
    }
    if (meta == nullptr) {
        return true;
    }
    return active_snapshot->is_visible(*meta);
}

bool HeapTable::decode_heap_slot(const std::size_t slot, Row& out) const {
    ++decode_heap_slot_calls;
    if (!is_heap_storage_backed() || slot >= heap_fingers_.size()) {
        ++decode_heap_slot_misses;
        return false;
    }
    const HeapRowFinger& f = heap_fingers_[slot];
    const std::shared_ptr<const HeapFileReadView> file = heap_file_;
    if (!file || f.page_no >= file->num_pages()) {
        ++decode_heap_slot_misses;
        return false;
    }
    const std::size_t psz = file->page_size();
    if (static_cast<std::size_t>(f.byte_off_in_page) + static_cast<std::size_t>(f.payload_len) > psz) {
        ++decode_heap_slot_misses;
        return false;
    }

    const unsigned char* page_ptr = file->page_data(f.page_no);
    if (page_ptr == nullptr) {
        if (heap_cached_page_no_ != static_cast<std::size_t>(f.page_no) ||
            heap_cached_page_buf_.size() != psz) {
            heap_cached_page_buf_.resize(psz);
            if (!file->read_page_copy(f.page_no, heap_cached_page_buf_.data())) {
                ++decode_heap_slot_misses;
                return false;
            }
            heap_cached_page_no_ = static_cast<std::size_t>(f.page_no);
        }
        page_ptr = heap_cached_page_buf_.data();
    } else {
        ++decode_heap_slot_hits;
    }
    if (!codec::decode_heap_payload_to_row(page_ptr + f.byte_off_in_page, f.payload_len, out)) {
        ++decode_heap_slot_misses;
        return false;
    }
    strip_mvcc_internal_attrs(out);
    return out.id != 0;
}

Status HeapTable::materialize_all_rows(const TableSchema& schema) {
    if (!is_heap_storage_backed()) {
        return Status::Ok();
    }
    std::vector<Row> built;
    built.reserve(heap_fingers_.size());
    Row tmp;
    for (std::size_t i = 0; i < heap_fingers_.size(); ++i) {
        if (!decode_heap_slot(i, tmp)) {
            return Status::Fail("decode failed while materializing heap rows");
        }
        built.push_back(std::move(tmp));
    }
    rows = std::move(built);
    if (row_meta.size() != rows.size()) {
        row_meta.assign(rows.size(), RecordMetadata{});
    }
    heap_file_.reset();
    heap_sorted_ids_.clear();
    heap_fingers_.clear();
    heap_find_scratch_ = Row{};
    heap_cached_page_no_ = static_cast<std::size_t>(-1);
    heap_cached_page_buf_.clear();
    rebuild_indexes(schema);
    return Status::Ok();
}

bool HeapTable::row_get_pk_value(const Row& r, const std::string& pk, std::string& out_val) {
    if (pk == "id") {
        out_val = std::to_string(r.id);
        return true;
    }
    const auto it = r.attrs.find(pk);
    if (it == r.attrs.end()) {
        return false;
    }
    out_val = it->second;
    return true;
}

void HeapTable::rebuild_indexes(const TableSchema& schema) {
    index_by_id.clear();
    index_by_pk_value.clear();
    attr_index.clear();
    sorted_cache_asc.clear();
    sorted_cache_desc.clear();

    if (is_heap_storage_backed()) {
        const bool build_pk = (!schema.primary_key.empty() && schema.primary_key != "id");
        Row r;
        for (std::size_t i = 0; i < heap_fingers_.size(); ++i) {
            if (!decode_heap_slot(i, r)) {
                continue;
            }
            if (!is_row_visible(i, r)) {
                continue;
            }
            index_by_id[r.id] = i;
            if (build_pk) {
                std::string pkv;
                if (row_get_pk_value(r, schema.primary_key, pkv)) {
                    index_by_pk_value[pkv] = i;
                }
            }
        }
    } else {
        for (std::size_t i = 0; i < rows.size(); ++i) {
            if (!is_row_visible(i, rows[i])) {
                continue;
            }
            index_by_id[rows[i].id] = i;
        }
    }

    for (std::size_t i = 0; i < schema.attrs.size(); ++i) {
        attr_index[schema.attrs[i].name] = i;
    }

    if (!is_heap_storage_backed()) {
        for (auto& r : rows) {
            if (r.values.size() < schema.attrs.size()) {
                r.values.resize(schema.attrs.size());
            }
            for (std::size_t i = 0; i < schema.attrs.size(); ++i) {
                const auto& meta = schema.attrs[i];
                const auto it = r.attrs.find(meta.name);
                r.values[i] = (it != r.attrs.end()) ? it->second : std::string{};
            }
        }
    }

    if (!schema.primary_key.empty() && schema.primary_key != "id") {
        if (!is_heap_storage_backed()) {
            for (const auto& kv : index_by_id) {
                const std::size_t i = kv.second;
                const Row& row = rows[i];
                std::string v;
                if (!row_get_pk_value(row, schema.primary_key, v)) {
                    continue;
                }
                index_by_pk_value[v] = i;
            }
        }
    }
}

const Row* HeapTable::find_by_id(const int id) const {
    const auto it = index_by_id.find(id);
    if (it == index_by_id.end()) {
        if (is_heap_storage_backed()) {
            return nullptr;
        }
        for (std::size_t i = 0; i < rows.size(); ++i) {
            const Row& r = rows[i];
            if (r.id == id && is_row_visible(i, r)) {
                return &r;
            }
        }
        return nullptr;
    }
    const std::size_t idx = it->second;
    if (is_heap_storage_backed()) {
        if (idx >= heap_fingers_.size()) {
            return nullptr;
        }
        if (!decode_heap_slot(idx, heap_find_scratch_)) {
            return nullptr;
        }
        if (!is_row_visible(idx, heap_find_scratch_)) {
            return nullptr;
        }
        return &heap_find_scratch_;
    }
    if (idx < rows.size() && rows[idx].id == id && is_row_visible(idx, rows[idx])) {
        return &rows[idx];
    }
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const Row& r = rows[i];
        if (r.id == id && is_row_visible(i, r)) {
            return &r;
        }
    }
    return nullptr;
}

bool HeapTable::primary_key_value_exists(const TableSchema& schema,
                                         const std::string& pk,
                                         const std::string& val,
                                         const int exclude_row_id) const {
    if (pk == "id") {
        if (exclude_row_id != 0 && std::to_string(exclude_row_id) == val) {
            return false;
        }
        if (!index_by_id.empty()) {
            try {
                const int id = std::stoi(val);
                return index_by_id.find(id) != index_by_id.end();
            } catch (...) {
                return false;
            }
        }
    } else {
        if (!index_by_pk_value.empty()) {
            const auto it = index_by_pk_value.find(val);
            if (it != index_by_pk_value.end()) {
                const std::size_t idx = it->second;
                if (is_heap_storage_backed()) {
                    if (idx < heap_fingers_.size()) {
                        Row r;
                        if (decode_heap_slot(idx, r)) {
                            if (exclude_row_id != 0 && r.id == exclude_row_id) {
                                return false;
                            }
                            return true;
                        }
                    }
                } else if (idx < rows.size()) {
                    const Row& r = rows[idx];
                    if (exclude_row_id != 0 && r.id == exclude_row_id) {
                        return false;
                    }
                    return true;
                }
            }
        }
        Row r;
        const std::size_t n = logical_row_count();
        for (std::size_t i = 0; i < n; ++i) {
            if (is_heap_storage_backed()) {
                if (!decode_heap_slot(i, r)) {
                    continue;
                }
            } else {
                r = rows[i];
            }
            if (!is_row_visible(i, r)) {
                continue;
            }
            if (exclude_row_id != 0 && r.id == exclude_row_id) {
                continue;
            }
            std::string v;
            if (!row_get_pk_value(r, pk, v)) {
                continue;
            }
            if (v == val) {
                return true;
            }
        }
        return false;
    }
    Row r;
    const std::size_t n = logical_row_count();
    for (std::size_t i = 0; i < n; ++i) {
        if (is_heap_storage_backed()) {
            if (!decode_heap_slot(i, r)) {
                continue;
            }
        } else {
            r = rows[i];
        }
        if (exclude_row_id != 0 && r.id == exclude_row_id) {
            continue;
        }
        std::string v;
        if (!row_get_pk_value(r, pk, v)) {
            continue;
        }
        if (v == val) {
            return true;
        }
    }
    return false;
}

const std::vector<std::size_t>& HeapTable::sorted_indices(const TableSchema& schema,
                                                          const std::string& order_key,
                                                          const SortDir dir) {
    auto& cache = (dir == SortDir::Asc) ? sorted_cache_asc : sorted_cache_desc;
    const auto it = cache.find(order_key);
    if (it != cache.end()) {
        return it->second;
    }

    if (is_heap_storage_backed() && order_key != "id") {
        std::vector<std::size_t> indices;
        indices.reserve(heap_fingers_.size());
        Row probe;
        for (std::size_t i = 0; i < heap_fingers_.size(); ++i) {
            if (!decode_heap_slot(i, probe)) {
                continue;
            }
            if (!is_row_visible(i, probe)) {
                continue;
            }
            indices.push_back(i);
        }
        // Decode once per candidate slot, then sort by cached key to avoid O(n log n) repeated decode.
        std::unordered_map<std::size_t, std::string> sort_keys;
        sort_keys.reserve(indices.size());
        for (const std::size_t slot : indices) {
            Row row;
            if (!decode_heap_slot(slot, row)) {
                continue;
            }
            const auto it = row.attrs.find(order_key);
            sort_keys.emplace(slot, (it != row.attrs.end()) ? it->second : std::string{});
        }
        std::sort(indices.begin(), indices.end(), [&](const std::size_t ia, const std::size_t ib) {
            const auto ita = sort_keys.find(ia);
            const auto itb = sort_keys.find(ib);
            if (ita == sort_keys.end() || itb == sort_keys.end()) {
                return ia < ib;
            }
            const int cmp = schema.compare_attr(order_key, ita->second, itb->second);
            if (cmp == 0) {
                return ia < ib;
            }
            return dir == SortDir::Asc ? (cmp < 0) : (cmp > 0);
        });
        trim_sorted_cache_before_insert(cache);
        const auto [ins, _] = cache.emplace(order_key, std::move(indices));
        return ins->second;
    }

    if (is_heap_storage_backed() && order_key == "id") {
        std::vector<std::size_t> indices;
        indices.reserve(heap_fingers_.size());
        Row probe;
        for (std::size_t i = 0; i < heap_fingers_.size(); ++i) {
            if (!decode_heap_slot(i, probe)) {
                continue;
            }
            if (!is_row_visible(i, probe)) {
                continue;
            }
            indices.push_back(i);
        }
        std::unordered_map<std::size_t, int> sort_ids;
        sort_ids.reserve(indices.size());
        for (const std::size_t slot : indices) {
            Row row;
            if (!decode_heap_slot(slot, row)) {
                continue;
            }
            sort_ids.emplace(slot, row.id);
        }
        std::sort(indices.begin(), indices.end(), [&](const std::size_t ia, const std::size_t ib) {
            const auto ita = sort_ids.find(ia);
            const auto itb = sort_ids.find(ib);
            if (ita == sort_ids.end() || itb == sort_ids.end()) {
                return ia < ib;
            }
            if (ita->second == itb->second) {
                return ia < ib;
            }
            return ita->second < itb->second;
        });
        if (dir == SortDir::Desc) {
            std::reverse(indices.begin(), indices.end());
        }
        trim_sorted_cache_before_insert(cache);
        const auto [ins, _] = cache.emplace(order_key, std::move(indices));
        return ins->second;
    }

    std::vector<std::size_t> indices;
    indices.reserve(rows.size());
    for (std::size_t i = 0; i < rows.size(); ++i) {
        if (!is_row_visible(i, rows[i])) {
            continue;
        }
        indices.push_back(i);
    }
    std::sort(indices.begin(), indices.end(), [&](const std::size_t ia, const std::size_t ib) {
        const Row& a = rows[ia];
        const Row& b = rows[ib];
        int cmp = 0;
        if (order_key == "id") {
            cmp = (a.id < b.id) ? -1 : (a.id > b.id ? 1 : 0);
        } else {
            std::string va;
            std::string vb;
            const auto it_idx = attr_index.find(order_key);
            if (it_idx != attr_index.end()) {
                const std::size_t idx = it_idx->second;
                if (idx < a.values.size()) {
                    va = a.values[idx];
                }
                if (idx < b.values.size()) {
                    vb = b.values[idx];
                }
            } else {
                const auto ita = a.attrs.find(order_key);
                const auto itb = b.attrs.find(order_key);
                if (ita != a.attrs.end()) {
                    va = ita->second;
                }
                if (itb != b.attrs.end()) {
                    vb = itb->second;
                }
            }
            cmp = schema.compare_attr(order_key, va, vb);
        }
        return dir == SortDir::Asc ? cmp < 0 : cmp > 0;
    });
    trim_sorted_cache_before_insert(cache);
    const auto [ins, _] = cache.emplace(order_key, std::move(indices));
    return ins->second;
}

} // namespace newdb
