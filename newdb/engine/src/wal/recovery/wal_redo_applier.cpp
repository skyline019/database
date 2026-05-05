#include <newdb/wal/wal_redo_applier.h>

#include <chrono>
#include <utility>

namespace newdb {
namespace wal_recovery {

namespace {

std::uint64_t monotonic_ms_now() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

}  // namespace

ApplyStats WalRedoApplier::apply(const RedoPlanSummary& plan) const {
    ApplyStats stats{};
    if (target_ == nullptr) {
        return stats;
    }
    const std::uint64_t t0 = monotonic_ms_now();
    for (const RedoOp& op : plan.committed_ops) {
        Row row = op.row;
        switch (op.op) {
        case WalOp::INSERT:
        case WalOp::UPDATE: {
            const auto idx = target_->index_by_id.find(row.id);
            if (idx != target_->index_by_id.end()) {
                target_->rows[static_cast<std::size_t>(idx->second)] = std::move(row);
            } else {
                target_->rows.push_back(std::move(row));
            }
            ++stats.records_applied;
            break;
        }
        case WalOp::DELETE: {
            const Row* target_row = target_->find_by_id(row.id);
            if (target_row != nullptr) {
                Row tombstone;
                tombstone.id = row.id;
                tombstone.attrs["__deleted"] = "1";
                target_->rows.push_back(std::move(tombstone));
                ++stats.records_applied;
            } else {
                ++stats.skipped;
            }
            break;
        }
        default:
            break;
        }
    }
    stats.apply_ms = monotonic_ms_now() - t0;
    return stats;
}

}  // namespace wal_recovery
}  // namespace newdb
