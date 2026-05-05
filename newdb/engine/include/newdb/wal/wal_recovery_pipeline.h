#pragma once

#include <newdb/wal_manager.h>

#include <cstdio>
#include <cstdint>
#include <vector>

namespace newdb {
namespace wal_recovery {

/// Thin wrapper over `WalManager::read_record` for recovery tooling / tests (Reader stage).
class WalRecordReader {
public:
    explicit WalRecordReader(const WalManager& wm) : wm_(&wm) {}

    Status read(FILE* fp, WalRecordHeader* hdr, std::vector<uint8_t>& payload) const {
        return wm_->read_record(fp, hdr, payload);
    }

private:
    const WalManager* wm_;
};

/// Serializable view of redo stages extracted from `WalRecoveryStats` (Planner/Applier summaries).
struct WalRedoPlanSummary {
    std::uint64_t records_after_checkpoint{0};
    std::uint64_t indexed_segments{0};
    std::uint64_t redo_plan_ms{0};
    std::uint64_t redo_apply_ms{0};
    std::uint64_t uncommitted_txn_discarded_count{0};
    std::uint64_t redo_records_skipped{0};
};

inline WalRedoPlanSummary summarize_recovery_stats(const WalRecoveryStats& s) {
    WalRedoPlanSummary o;
    o.records_after_checkpoint = s.records_after_checkpoint;
    o.indexed_segments = s.indexed_segments;
    o.redo_plan_ms = s.redo_plan_ms;
    o.redo_apply_ms = s.redo_apply_ms;
    o.uncommitted_txn_discarded_count = s.uncommitted_txn_discarded_count;
    o.redo_records_skipped = s.redo_records_skipped;
    return o;
}

} // namespace wal_recovery
} // namespace newdb
