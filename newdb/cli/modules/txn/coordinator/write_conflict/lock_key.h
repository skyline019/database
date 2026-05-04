#pragma once

#include <string>

/// Structured write-intent key (phase-1 migration from raw `table#id` strings).
/// Storage key format remains backward-compatible with existing `g_write_intent_owner` maps.
enum class LockKeyKind {
    RowPkWriteIntent = 0,
};

struct LockKey {
    std::string table;
    std::string index;
    std::string key;
    LockKeyKind kind{LockKeyKind::RowPkWriteIntent};

    static LockKey row_pk_write_intent(std::string table_name, int row_id) {
        LockKey k;
        k.table = std::move(table_name);
        k.index.clear();
        k.key = std::to_string(row_id);
        k.kind = LockKeyKind::RowPkWriteIntent;
        return k;
    }

    /// Canonical map key used by `TxnCoordinator::tryReserveWriteKey` / `clearWriteIntents`.
    std::string to_storage_key() const {
        switch (kind) {
        case LockKeyKind::RowPkWriteIntent:
        default:
            return table + "#" + key;
        }
    }
};
