#include <newdb/wal/wal_redo_planner.h>

#include <newdb/tuple_codec.h>
#include <newdb/wal_codec.h>

#include <chrono>
#include <utility>

namespace newdb {
namespace wal_recovery {

namespace {

std::uint64_t monotonic_ms_now() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

bool decode_row_payload_local(const uint8_t* data, std::size_t len, Row& out) {
    return codec::decode_heap_payload_to_row(data, len, out);
}

bool decode_dml_payload(const std::vector<uint8_t>& payload,
                        const TableSchema& schema,
                        WalOp op,
                        std::string& table_name,
                        Row& row) {
    (void)schema;
    const uint8_t* p = payload.data();
    const uint8_t* end = payload.data() + payload.size();
    if (walcodec::is_v1_payload(payload.data(), payload.size())) {
        walcodec::DecodedPayloadV1 dec{};
        const Status st = walcodec::decode_payload_v1(payload.data(), payload.size(), dec);
        if (!st.ok) {
            return false;
        }
        table_name = dec.table;
        const bool need_before = (op == WalOp::DELETE);
        if ((need_before && !dec.has_before) || (!need_before && !dec.has_after)) {
            return false;
        }
        const uint8_t* row_payload_ptr = need_before ? dec.before_row_payload : dec.after_row_payload;
        const uint32_t row_payload_len = need_before ? dec.before_row_payload_len : dec.after_row_payload_len;
        return decode_row_payload_local(row_payload_ptr, row_payload_len, row);
    }
    Status dec_table = walcodec::decode_table_name(p, end, table_name);
    if (!dec_table.ok) {
        return false;
    }
    uint32_t row_id = 0;
    const uint8_t* row_payload_ptr = nullptr;
    uint32_t row_payload_len = 0;
    Status dec_row = walcodec::decode_row_fields(p, end, row_id, row_payload_ptr, row_payload_len);
    if (!dec_row.ok) {
        return false;
    }
    return decode_row_payload_local(row_payload_ptr, row_payload_len, row);
}

}  // namespace

void WalRedoPlanner::feed_record(const WalRecordHeader& hdr,
                                 const std::vector<uint8_t>& payload) {
    const std::uint64_t t0 = monotonic_ms_now();
    ++records_seen_;
    const WalOp op = static_cast<WalOp>(hdr.type);
    switch (op) {
    case WalOp::INSERT:
    case WalOp::UPDATE:
    case WalOp::DELETE: {
        std::string table_name;
        Row row;
        if (!decode_dml_payload(payload, *schema_, op, table_name, row)) {
            ++decode_failures_;
            plan_ms_ += monotonic_ms_now() - t0;
            return;
        }
        if (table_name != target_table_name_) {
            plan_ms_ += monotonic_ms_now() - t0;
            return;
        }
        PendingTxnOps& pending = txn_ops_[hdr.txn_id];
        const int row_id = row.id;
        const auto it = pending.by_row_id.find(row_id);
        if (it == pending.by_row_id.end()) {
            pending.order.push_back(row_id);
        }
        pending.by_row_id[row_id] = RedoOp{op, std::move(row)};
        break;
    }
    case WalOp::COMMIT: {
        const auto it = txn_ops_.find(hdr.txn_id);
        if (it != txn_ops_.end()) {
            for (const int row_id : it->second.order) {
                const auto rit = it->second.by_row_id.find(row_id);
                if (rit == it->second.by_row_id.end()) {
                    continue;
                }
                committed_ops_.push_back(std::move(rit->second));
            }
            txn_ops_.erase(it);
        }
        break;
    }
    case WalOp::ROLLBACK:
        txn_ops_.erase(hdr.txn_id);
        break;
    case WalOp::CHECKPOINT:
    case WalOp::CHECKPOINT_BEGIN:
        ++checkpoint_begin_count_;
        break;
    case WalOp::CHECKPOINT_END:
        ++checkpoint_end_count_;
        break;
    case WalOp::SESSION_SNAPSHOT:
    case WalOp::TXN_PREPARE:
    case WalOp::SAVEPOINT_SET:
    case WalOp::SAVEPOINT_ROLLBACK:
    case WalOp::TXN_ABORT_PARTIAL:
    case WalOp::PITR_MARK:
        break;
    default:
        // Unknown/future op types: ignore for redo planning (only INSERT/UPDATE/DELETE mutate rows).
        break;
    }
    plan_ms_ += monotonic_ms_now() - t0;
}

RedoPlanSummary WalRedoPlanner::finalize() {
    RedoPlanSummary out;
    out.committed_ops = std::move(committed_ops_);
    committed_ops_.clear();
    for (const auto& kv : txn_ops_) {
        if (!kv.second.by_row_id.empty()) {
            ++out.uncommitted_txn_discarded_count;
            ++out.recovery_uncommitted_records_ignored;
        }
    }
    txn_ops_.clear();
    out.plan_ms = plan_ms_;
    plan_ms_ = 0;
    return out;
}

}  // namespace wal_recovery
}  // namespace newdb
