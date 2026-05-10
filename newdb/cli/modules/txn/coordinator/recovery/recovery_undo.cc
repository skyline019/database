#include "cli/modules/txn/coordinator/recovery/recovery_undo.h"

#include "cli/modules/txn/coordinator/recovery/heap_undo_apply.h"
#include "cli/modules/common/logging/logging.h"

#include <algorithm>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

constexpr std::size_t kNoIndex = std::numeric_limits<std::size_t>::max();

/// `WalDecodedRecord::lsn` is unique per append in normal operation; if duplicates exist (legacy/corrupt),
/// disambiguate by preferring the largest `op_seq` strictly less than `bound_op_seq`.
std::size_t resolve_lsn_to_index(const std::vector<TxnWalOp>& ops,
                                 const std::unordered_map<std::uint64_t, std::vector<std::size_t>>& lsn_to_indices,
                                 std::uint64_t target_lsn,
                                 std::uint64_t bound_op_seq) {
    const auto itm = lsn_to_indices.find(target_lsn);
    if (itm == lsn_to_indices.end()) {
        return kNoIndex;
    }
    const std::vector<std::size_t>& vec = itm->second;
    std::size_t best = kNoIndex;
    for (std::size_t idx : vec) {
        if (ops[idx].op_seq < bound_op_seq) {
            if (best == kNoIndex || ops[idx].op_seq > ops[best].op_seq) {
                best = idx;
            }
        }
    }
    if (best != kNoIndex) {
        return best;
    }
    return vec.back();
}

}  // namespace

bool recovery_plan_undo_ops_order(const std::vector<TxnWalOp>& ops,
                                  std::vector<std::size_t>& out_indices,
                                  bool* used_chain_fallback) {
    out_indices.clear();
    if (used_chain_fallback != nullptr) {
        *used_chain_fallback = false;
    }
    if (ops.empty()) {
        return true;
    }

    std::unordered_map<std::uint64_t, std::vector<std::size_t>> lsn_to_indices;
    for (std::size_t i = 0; i < ops.size(); ++i) {
        lsn_to_indices[ops[i].record_lsn].push_back(i);
    }
    for (auto& kv : lsn_to_indices) {
        std::vector<std::size_t>& v = kv.second;
        std::sort(v.begin(), v.end(), [&](std::size_t a, std::size_t b) {
            return ops[a].op_seq < ops[b].op_seq;
        });
    }

    std::size_t start = 0;
    for (std::size_t i = 1; i < ops.size(); ++i) {
        if (ops[i].record_lsn > ops[start].record_lsn) {
            start = i;
        } else if (ops[i].record_lsn == ops[start].record_lsn && ops[i].op_seq > ops[start].op_seq) {
            start = i;
        }
    }

    std::unordered_set<std::size_t> visited_index;
    std::vector<std::size_t> chain;
    chain.reserve(ops.size());
    std::size_t cur = start;
    bool broken = false;
    while (true) {
        if (!visited_index.insert(cur).second) {
            broken = true;
            break;
        }
        chain.push_back(cur);
        if (!ops[cur].has_undo_prev_lsn || ops[cur].undo_prev_lsn == 0) {
            break;
        }
        const std::size_t nxt =
            resolve_lsn_to_index(ops, lsn_to_indices, ops[cur].undo_prev_lsn, ops[cur].op_seq);
        if (nxt == kNoIndex) {
            broken = true;
            break;
        }
        cur = nxt;
    }

    if (!broken && visited_index.size() == ops.size()) {
        out_indices = std::move(chain);
        return true;
    }

    if (used_chain_fallback != nullptr) {
        *used_chain_fallback = true;
    }
    std::vector<std::size_t> order(ops.size());
    for (std::size_t i = 0; i < ops.size(); ++i) {
        order[i] = i;
    }
    std::sort(order.begin(), order.end(), [&](std::size_t a, std::size_t b) {
        if (ops[a].record_lsn != ops[b].record_lsn) {
            return ops[a].record_lsn > ops[b].record_lsn;
        }
        return ops[a].op_seq > ops[b].op_seq;
    });
    out_indices = std::move(order);
    return true;
}

RecoveryUndoResult recovery_apply_dangling_undo(
    const std::unordered_map<std::uint64_t, std::vector<TxnWalOp>>& dangling_by_txn,
    const std::function<std::string(const std::string& table_name)>& resolve_data_file) {
    RecoveryUndoResult result{};
    std::vector<std::uint64_t> txn_ids;
    txn_ids.reserve(dangling_by_txn.size());
    for (const auto& kv : dangling_by_txn) {
        txn_ids.push_back(kv.first);
    }
    auto max_lsn_in_txn = [&](std::uint64_t tid) -> std::uint64_t {
        std::uint64_t m = 0;
        const auto it = dangling_by_txn.find(tid);
        if (it == dangling_by_txn.end()) {
            return 0;
        }
        for (const auto& op : it->second) {
            m = std::max(m, op.record_lsn);
        }
        return m;
    };
    std::sort(txn_ids.begin(), txn_ids.end(),
              [&](std::uint64_t a, std::uint64_t b) { return max_lsn_in_txn(a) > max_lsn_in_txn(b); });

    for (std::uint64_t tid : txn_ids) {
        const auto it_vec = dangling_by_txn.find(tid);
        if (it_vec == dangling_by_txn.end()) {
            continue;
        }
        const std::vector<TxnWalOp>& vec = it_vec->second;
        std::vector<std::size_t> order;
        bool fallback = false;
        (void)recovery_plan_undo_ops_order(vec, order, &fallback);
        if (fallback) {
            ++result.chain_fallback_txns;
        }
        for (std::size_t idx : order) {
            const TxnWalOp& w = vec[idx];
            const TxnRecord& rec = w.rec;
            if (rec.operation == "TXN_BEGIN") {
                continue;
            }
            const std::string data_file = resolve_data_file(rec.table_name);
            cli_txn_heap_undo::TxnWalUndoView view;
            view.rec = &rec;
            view.has_before = w.has_before;
            view.before_row = w.has_before ? &w.before_row : nullptr;
            if (cli_txn_heap_undo::append_undo_row_to_heap(data_file.c_str(), view)) {
                if (rec.operation == "INSERT") {
                    logging_console_printf("[WAL] Undo INSERT id=%s on %s\n",
                                           rec.key.c_str(),
                                           rec.table_name.c_str());
                } else if (rec.operation == "UPDATE" || rec.operation == "DELETE") {
                    logging_console_printf("[WAL] Undo %s id=%s on %s\n",
                                           rec.operation.c_str(),
                                           rec.key.c_str(),
                                           rec.table_name.c_str());
                }
            } else {
                logging_console_printf("[WAL] Skip malformed WAL record key=%s\n", rec.key.c_str());
            }
        }
    }
    return result;
}
