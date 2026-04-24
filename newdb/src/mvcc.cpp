#include <newdb/mvcc.h>
#include <newdb/wal_manager.h>

#include <algorithm>

namespace newdb {

bool MVCCSnapshot::is_visible(uint64_t created_lsn, uint64_t deleted_lsn, uint64_t creator_txn) const {
    // Created after snapshot? Not visible
    if (created_lsn > snapshot_lsn) return false;
    
    // Deleted before or at snapshot? Not visible
    if (deleted_lsn != 0 && deleted_lsn <= snapshot_lsn) return false;
    
    // Creator transaction not yet committed at snapshot time?
    // If creator_txn is in active_txns, it hasn't committed yet
    if (active_txns.find(creator_txn) != active_txns.end()) {
        return false;
    }
    
    return true;
}

bool MVCCSnapshot::is_visible(const RecordMetadata& meta) const {
    return is_visible(meta.created_lsn, meta.deleted_lsn, meta.txn_id);
}

// ==================== MVCCManager Implementation ====================

uint64_t MVCCManager::begin_transaction() {
    std::lock_guard<std::mutex> lg(mut_);
    uint64_t txn_id = next_txn_id_++;
    active_txns_.insert(txn_id);
    return txn_id;
}

void MVCCManager::commit_transaction(uint64_t txn_id, uint64_t commit_lsn) {
    std::lock_guard<std::mutex> lg(mut_);
    active_txns_.erase(txn_id);
    committed_txn_lsn_[txn_id] = commit_lsn;
    
    // Update min_visible_lsn: smallest LSN of any committed txn we've seen
    // For simplicity, track the max commit LSN as the new watermark
    min_visible_lsn_ = std::max(min_visible_lsn_, commit_lsn);
}

void MVCCManager::rollback_transaction(uint64_t txn_id) {
    std::lock_guard<std::mutex> lg(mut_);
    active_txns_.erase(txn_id);
    // Don't add to committed_txns
}

MVCCSnapshot MVCCManager::create_snapshot() const {
    std::lock_guard<std::mutex> lg(mut_);
    MVCCSnapshot s(wal_.current_lsn(), std::unordered_set<uint64_t>(active_txns_));
    s.min_visible_lsn = min_visible_lsn_;
    return s;
}

void MVCCManager::purge_txns_before(uint64_t lsn) {
    std::lock_guard<std::mutex> lg(mut_);
    for (auto it = committed_txn_lsn_.begin(); it != committed_txn_lsn_.end();) {
        if (it->second <= lsn) {
            it = committed_txn_lsn_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace newdb