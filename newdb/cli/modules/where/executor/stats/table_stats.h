#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace newdb {
struct HeapTable;
struct TableSchema;
}  // namespace newdb

struct ColumnStats {
    std::uint64_t non_null_count{0};
    std::uint64_t distinct_count{0};
    /// Lexicographic min among non-null string samples (empty = unknown).
    std::string min_value;
    std::string max_value;
    /// Up to 3 most frequent non-null values (best-effort; may be empty).
    std::vector<std::string> top_k;
    /// Equi-depth histogram buckets (fixed 8 buckets over sorted non-null samples).
    std::vector<std::uint64_t> histogram_buckets;
};

struct TableStats {
    std::uint64_t row_count{0};
    /// Unix epoch ms when stats were last written (0 if unknown / legacy file).
    std::uint64_t stats_built_ts_ms{0};
    /// Schema fingerprint from the loaded `.tablestats` file (0 when built in-memory only).
    std::uint64_t stats_schema_fp{0};
    std::unordered_map<std::string, ColumnStats> columns;
};

/// Full scan of materialized rows; O(n) — use for ANALYZE-style paths only.
bool build_table_stats_from_heap(const newdb::HeapTable& tbl,
                                 const newdb::TableSchema& schema,
                                 TableStats* out);

/// Returns (0,1] selectivity for equality on `attr`, or 0 when unknown.
double eq_selectivity_from_stats(const TableStats* stats,
                                 const std::string& attr,
                                 std::size_t logical_rows);

/// Heuristic (0,1] span for range / inequality filters; derived from NDV via equality selectivity.
double range_selectivity_from_stats(const TableStats* stats,
                                    const std::string& attr,
                                    std::size_t logical_rows);

/// Sidecar path: `<data_file>.tablestats` (same directory as heap `.bin`).
std::string table_stats_file_path_for_data_file(const std::string& data_file);

/// Stable fingerprint of schema columns used to reject stale files after DDL.
std::uint64_t table_stats_schema_fingerprint(const newdb::TableSchema& schema);

/// Text format v1; returns false if missing, corrupt, or fingerprint mismatch.
bool load_table_stats_file(const std::string& data_file,
                           const newdb::TableSchema& schema,
                           TableStats* out);

bool save_table_stats_file(const std::string& data_file,
                           const newdb::TableSchema& schema,
                           const TableStats& stats);

void invalidate_table_stats_for_data_file(const std::string& data_file);

/// True when `stats` carries a non-zero fingerprint that matches the current schema.
bool table_stats_matches_schema(const TableStats& stats, const newdb::TableSchema& schema);
