#pragma once

#include <cstdint>
#include <string>

/// Structured write-intent key (migrated from raw `table#id` strings; extended for index/range/predicate).
/// `RowPkWriteIntent` storage format remains `table#<pk>` for backward compatibility with existing maps.
enum class LockKeyKind : std::uint8_t {
    RowPkWriteIntent = 0,
    IndexEqWriteIntent = 1,
    RangeWriteIntent = 2,
    PredicateWriteIntent = 3,
};

struct LockKey {
    std::string table;
    std::string index;
    /// Row-PK and index-eq: single encoded key; predicate: opaque predicate id / expression blob.
    std::string key;
    /// Range lock endpoints (inclusive/exclusive semantics are path-specific; store-side uses opaque strings).
    std::string key_begin;
    std::string key_end;
    LockKeyKind kind{LockKeyKind::RowPkWriteIntent};
    /// Reserved for future serializer evolution (`to_storage_key` v2 prefix).
    std::uint32_t format_version{1};

    static LockKey row_pk_write_intent(std::string table_name, int row_id) {
        LockKey k;
        k.table = std::move(table_name);
        k.index.clear();
        k.key = std::to_string(row_id);
        k.key_begin.clear();
        k.key_end.clear();
        k.kind = LockKeyKind::RowPkWriteIntent;
        k.format_version = 1;
        return k;
    }

    static LockKey index_eq_write_intent(std::string table_name, std::string index_name, std::string encoded_key) {
        LockKey k;
        k.table = std::move(table_name);
        k.index = std::move(index_name);
        k.key = std::move(encoded_key);
        k.key_begin.clear();
        k.key_end.clear();
        k.kind = LockKeyKind::IndexEqWriteIntent;
        k.format_version = 1;
        return k;
    }

    static LockKey range_write_intent(std::string table_name,
                                      std::string index_name,
                                      std::string begin_key,
                                      std::string end_key) {
        LockKey k;
        k.table = std::move(table_name);
        k.index = std::move(index_name);
        k.key.clear();
        k.key_begin = std::move(begin_key);
        k.key_end = std::move(end_key);
        k.kind = LockKeyKind::RangeWriteIntent;
        k.format_version = 1;
        return k;
    }

    static LockKey predicate_write_intent(std::string table_name,
                                         std::string index_name,
                                         std::string predicate_id) {
        LockKey k;
        k.table = std::move(table_name);
        k.index = std::move(index_name);
        k.key = std::move(predicate_id);
        k.key_begin.clear();
        k.key_end.clear();
        k.kind = LockKeyKind::PredicateWriteIntent;
        k.format_version = 1;
        return k;
    }

    /// Canonical map key used by `TxnCoordinator::tryReserveWriteKey` / `tryReserveWriteLockKey` / `clearWriteIntents`.
    std::string to_storage_key() const {
        switch (kind) {
        case LockKeyKind::RowPkWriteIntent:
        default:
            return table + "#" + key;
        case LockKeyKind::IndexEqWriteIntent:
            return std::string("v2|idx|") + table + "|" + index + "|" + key;
        case LockKeyKind::RangeWriteIntent:
            return std::string("v2|range|") + table + "|" + index + "|" + key_begin + "|" + key_end;
        case LockKeyKind::PredicateWriteIntent:
            return std::string("v2|pred|") + table + "|" + index + "|" + key;
        }
    }
};
