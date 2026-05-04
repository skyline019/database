#include "cli/modules/storage/table_storage_health.h"

#include <algorithm>
#include <cmath>
#include <filesystem>

#include <newdb/heap_file_read_view.h>
#include <newdb/heap_table.h>

namespace newdb {

namespace {

std::uint64_t heap_backed_file_bytes(const HeapTable& tbl) {
    if (!tbl.is_heap_storage_backed() || !tbl.heap_file_) {
        return 0;
    }
    const auto* v = tbl.heap_file_.get();
    return static_cast<std::uint64_t>(v->num_pages() * v->page_size());
}

std::uint64_t infer_file_bytes_from_name(const HeapTable& tbl) {
    if (tbl.name.empty()) {
        return 0;
    }
    std::error_code ec;
    const std::string path = tbl.name + ".bin";
    if (!std::filesystem::exists(path, ec)) {
        return 0;
    }
    return static_cast<std::uint64_t>(std::filesystem::file_size(path, ec));
}

}  // namespace

TableStorageHealth measure_table_storage_health(const HeapTable& tbl) {
    TableStorageHealth h{};
    h.logical_rows = static_cast<std::uint64_t>(tbl.logical_row_count());
    const std::size_t logical = tbl.logical_row_count();
    const std::size_t n = std::min(logical, tbl.row_meta.size());
    for (std::size_t i = 0; i < n; ++i) {
        const auto& m = tbl.row_meta[i];
        if (m.is_tombstone || m.deleted_lsn != 0) {
            ++h.tombstone_slots;
        }
    }
    h.tombstone_rows = h.tombstone_slots;
    if (h.logical_rows > 0) {
        h.tombstone_ratio =
            static_cast<double>(h.tombstone_slots) / static_cast<double>(h.logical_rows);
    }
    if (!tbl.row_meta.empty()) {
        h.physical_rows = static_cast<std::uint64_t>(tbl.row_meta.size());
    } else {
        h.physical_rows = h.logical_rows;
    }
    h.data_file_bytes = heap_backed_file_bytes(tbl);
    if (h.data_file_bytes == 0) {
        h.data_file_bytes = infer_file_bytes_from_name(tbl);
    }
    if (h.data_file_bytes > 0) {
        const double est_dead =
            std::floor(static_cast<double>(h.data_file_bytes) * h.tombstone_ratio + 0.5);
        h.dead_bytes = static_cast<std::uint64_t>(
            std::min<double>(est_dead, static_cast<double>(h.data_file_bytes)));
        h.live_bytes = h.data_file_bytes - h.dead_bytes;
        h.fragmentation_ratio =
            static_cast<double>(h.dead_bytes) / static_cast<double>(h.data_file_bytes);
    } else {
        h.fragmentation_ratio = h.tombstone_ratio;
    }
    return h;
}

}  // namespace newdb
