#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include <newdb/row.h>

namespace newdb::lsm_lite {

enum class CompactionPolicy : std::uint8_t {
    SizeTiered = 0,
    LeveledLite = 1,
};

struct Hooks {
    // All hooks are optional; callers can wire them to runtime stats.
    std::function<void()> on_memtable_flush;
    std::function<void()> on_compaction;
    std::function<void(std::uint64_t n)> on_read_segments_scanned;
    std::function<void(bool hit)> on_cache_lookup;
    std::function<void(std::uint64_t pending, std::uint64_t inflight)> on_compaction_queue_depth;
    std::function<void()> on_compaction_enqueue_skipped_backpressure;
    std::function<void(std::uint64_t bytes_in, std::uint64_t bytes_out)> on_compaction_bytes;
    std::function<void(std::uint64_t memtable_bytes)> on_memtable_bytes;
    std::function<void(std::uint64_t segment_count)> on_segment_count;
};

struct Options {
    bool enabled{false};                 // LSM-lite enabled (e.g. HOTINDEX on)
    std::uint64_t segment_target_bytes{0}; // flush threshold; 0 => default

    // Compaction knobs (environment-driven in demo, but engine is explicit).
    std::uint64_t l0_compact_trigger{4};
    std::uint64_t l0_compact_batch{4};
    bool compaction_async{false};
    std::uint64_t compaction_workers{2};
    std::uint64_t compaction_max_pending{0}; // 0 => disabled
    std::uint64_t compaction_reap_budget{4};
    CompactionPolicy compaction_policy{CompactionPolicy::SizeTiered};
    std::uint64_t leveled_l1_soft_segments{24};
    std::uint64_t leveled_l1_hard_segments{48};
    std::string benchmark_profile{"newdb-default"};
};

struct TxnContext {
    bool in_txn{false};
    std::int64_t txn_id{0};
};

struct FindResult {
    bool found{false};
    bool deleted{false};
    newdb::Row row{};
};

// Drains pending async compaction jobs for one data file.
// Returns true if queue/inflight reach zero within timeout.
bool drain_compaction(const std::string& data_file, std::uint64_t timeout_ms = 2000);

// Stops all LSM-lite async workers and clears worker-local state.
void shutdown_background_workers();

// Records writes into the per-data-file memtable, triggering flush/compaction as needed.
void record_writes(const Options& opt,
                   const std::string& data_file,
                   const std::vector<newdb::Row>& rows,
                   bool deleted_flag,
                   const TxnContext* txn = nullptr,
                   Hooks* hooks = nullptr);

// Applies (commits) a txn-local LSM view into the global LSM view for this data_file.
void on_txn_commit(const Options& opt,
                   const std::string& data_file,
                   const TxnContext& txn,
                   Hooks* hooks = nullptr);

// Discards a txn-local LSM view for this data_file.
void on_txn_rollback(const Options& opt,
                     const std::string& data_file,
                     const TxnContext& txn,
                     Hooks* hooks = nullptr);

// Clears all txn-private views for the target data_file (e.g. session reset/switch safety).
void clear_txn_views_for_data_file(const std::string& data_file);

// Finds by id in the LSM-lite view (segments + cache). Returns nullopt if LSM-lite is disabled
// or no state is available for this data_file.
std::optional<FindResult> find_by_id(const Options& opt,
                                     const std::string& data_file,
                                     int id,
                                     const TxnContext* txn = nullptr,
                                     Hooks* hooks = nullptr);

} // namespace newdb::lsm_lite

