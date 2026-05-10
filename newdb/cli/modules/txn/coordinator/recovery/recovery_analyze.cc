#include "cli/modules/txn/coordinator/recovery/recovery_analyze.h"

#include <algorithm>
#include <functional>
#include <string>
#include <unordered_set>

void recovery_analyze_wal_records(const std::vector<newdb::WalDecodedRecord>& recs,
                                   std::uint64_t recover_target_lsn,
                                   RecoveryAnalyzeOutput& out) {
    out.committed_by_txn.clear();
    out.dangling_by_txn.clear();
    out.checkpoint_begin = 0;
    out.checkpoint_end = 0;

    std::unordered_map<std::uint64_t, std::vector<TxnWalOp>> txn_records;
    std::unordered_set<std::uint64_t> committed;
    std::unordered_set<std::uint64_t> rolled_back;
    std::unordered_map<std::uint64_t, std::uint64_t> partial_cutoff_by_txn;

    for (const auto& wr : recs) {
        if (recover_target_lsn > 0 && wr.lsn > recover_target_lsn) {
            continue;
        }
        if (wr.op == newdb::WalOp::COMMIT) {
            committed.insert(wr.txn_id);
            continue;
        }
        if (wr.op == newdb::WalOp::ROLLBACK) {
            rolled_back.insert(wr.txn_id);
            continue;
        }
        if (!wr.has_row) {
            if (wr.op == newdb::WalOp::CHECKPOINT_BEGIN) {
                ++out.checkpoint_begin;
            } else if (wr.op == newdb::WalOp::CHECKPOINT_END) {
                ++out.checkpoint_end;
            } else if (wr.op == newdb::WalOp::TXN_ABORT_PARTIAL && wr.has_pitr_target_lsn) {
                partial_cutoff_by_txn[wr.txn_id] = wr.pitr_target_lsn;
            }
            continue;
        }
        if (wr.op != newdb::WalOp::INSERT && wr.op != newdb::WalOp::UPDATE && wr.op != newdb::WalOp::DELETE) {
            continue;
        }
        TxnWalOp oprec;
        TxnRecord& rec = oprec.rec;
        rec.txn_id = static_cast<int64_t>(wr.txn_id);
        rec.table_name = wr.table;
        rec.key = std::to_string(wr.row.id);
        const auto it_old = wr.row.attrs.find("__wal_old");
        const auto it_new = wr.row.attrs.find("__wal_new");
        const auto it_op = wr.row.attrs.find("__wal_op");
        rec.old_value = (it_old != wr.row.attrs.end()) ? it_old->second : std::string{};
        rec.new_value = (it_new != wr.row.attrs.end()) ? it_new->second : std::string{};
        if (it_op != wr.row.attrs.end()) {
            rec.operation = it_op->second;
        } else if (wr.op == newdb::WalOp::INSERT) {
            rec.operation = "INSERT";
        } else if (wr.op == newdb::WalOp::DELETE) {
            rec.operation = "DELETE";
        } else {
            rec.operation = "UPDATE";
        }
        oprec.op_seq = wr.op_seq_in_txn;
        oprec.record_lsn = wr.lsn;
        oprec.has_undo_prev_lsn = wr.has_undo_prev_lsn;
        oprec.undo_prev_lsn = wr.undo_prev_lsn;
        if (wr.has_db_object_id) {
            oprec.db_object_id = wr.db_object_id;
        } else {
            oprec.db_object_id = static_cast<std::uint64_t>(std::hash<std::string>{}(wr.table));
        }
        oprec.has_before = wr.has_before_row;
        oprec.before_row = wr.before_row;
        oprec.has_after = wr.has_after_row;
        oprec.after_row = wr.after_row;
        oprec.wal_op = wr.op;
        txn_records[wr.txn_id].push_back(std::move(oprec));
    }

    for (auto& kv : txn_records) {
        if (committed.find(kv.first) != committed.end()) {
            out.committed_by_txn.emplace(kv.first, std::move(kv.second));
            continue;
        }
        if (rolled_back.find(kv.first) != rolled_back.end()) {
            continue;
        }
        out.dangling_by_txn.emplace(kv.first, std::move(kv.second));
    }

    for (auto& kv : out.committed_by_txn) {
        const auto it = partial_cutoff_by_txn.find(kv.first);
        if (it == partial_cutoff_by_txn.end()) {
            continue;
        }
        const std::uint64_t cutoff = it->second;
        auto& vec = kv.second;
        vec.erase(std::remove_if(vec.begin(), vec.end(), [&](const TxnWalOp& x) {
            return x.record_lsn > cutoff;
        }),
                  vec.end());
    }
    for (auto& kv : out.dangling_by_txn) {
        const auto it = partial_cutoff_by_txn.find(kv.first);
        if (it == partial_cutoff_by_txn.end()) {
            continue;
        }
        const std::uint64_t cutoff = it->second;
        auto& vec = kv.second;
        vec.erase(std::remove_if(vec.begin(), vec.end(), [&](const TxnWalOp& x) {
            return x.record_lsn <= cutoff;
        }),
                  vec.end());
    }
}

std::size_t recovery_count_dangling_ops(const RecoveryAnalyzeOutput& out) {
    std::size_t n = 0;
    for (const auto& kv : out.dangling_by_txn) {
        n += kv.second.size();
    }
    return n;
}
