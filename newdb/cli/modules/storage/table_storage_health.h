#pragma once

#include <cstdint>

namespace newdb {

struct HeapTable;

/// Storage health snapshot (phase 9; aligned with OPTIMIZATION_PLAN §5.2 where computable).
struct TableStorageHealth {
    std::uint64_t logical_rows{0};
    /// Slot / metadata footprint (`row_meta` size when present, else `logical_rows`).
    std::uint64_t physical_rows{0};
    std::uint64_t tombstone_slots{0};
    /// Same count as `tombstone_slots` (document naming).
    std::uint64_t tombstone_rows{0};
    double tombstone_ratio{0.0};
    std::uint64_t data_file_bytes{0};
    std::uint64_t live_bytes{0};
    std::uint64_t dead_bytes{0};
    /// Share of file bytes estimated dead, or `tombstone_ratio` when bytes unknown.
    double fragmentation_ratio{0.0};
    /// Not yet tracked per table; reserved for future vacuum metadata.
    std::uint64_t last_vacuum_lsn{0};
    std::uint64_t last_vacuum_elapsed_ms{0};
};

TableStorageHealth measure_table_storage_health(const HeapTable& tbl);

}  // namespace newdb
