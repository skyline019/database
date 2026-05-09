#include <waterfall/config.h>

#include "cli/shell/dispatch/services/lsm/lsm_lite_service.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include <newdb/lsm_lite.h>

#include "cli/shell/state/shell_state_benchmark.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/modules/txn/coordinator/txn_manager.h"

namespace {
std::uint64_t env_u64_or(const char* key, const std::uint64_t defv) {
    const char* env = std::getenv(key);
    if (!env) return defv;
    try {
        return static_cast<std::uint64_t>(std::stoull(env));
    } catch (...) {
        return defv;
    }
}

bool env_bool_or(const char* key, const bool defv) {
    const char* env = std::getenv(key);
    if (!env) return defv;
    std::string s(env);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s == "1" || s == "on" || s == "true" || s == "yes";
}

ShellBenchmarkProfile parse_profile(const std::string& raw) {
    if (raw == "leveldb-like") return ShellBenchmarkProfile::LeveldbLike;
    if (raw == "innodb-like") return ShellBenchmarkProfile::InnodbLike;
    if (raw == "hybrid-balanced") return ShellBenchmarkProfile::HybridBalanced;
    return ShellBenchmarkProfile::NewdbDefault;
}

ShellBenchmarkProfile resolve_profile() {
    const char* raw = std::getenv("NEWDB_BENCHMARK_PROFILE");
    if (!raw || !*raw) return ShellBenchmarkProfile::NewdbDefault;
    std::string s(raw);
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return parse_profile(s);
}

void apply_profile_defaults_once(ShellState& st, const ShellBenchmarkProfile profile) {
    ShellStateFacade f(st);
    if (f.runtime_policy().initialized && f.runtime_policy().profile == profile) {
        return;
    }
    f.runtime_policy().profile = profile;
    f.runtime_policy().initialized = true;
    switch (profile) {
    case ShellBenchmarkProfile::LeveldbLike:
        f.txn().setWalSyncMode(newdb::WalSyncMode::Normal);
        f.txn().setWalNormalSyncIntervalMs(40);
        f.txn().setGroupCommitWindowMs(30);
        f.txn().setGroupCommitMaxBatchCommits(64);
        f.txn().setWalAdaptiveEnabled(false);
        f.txn().setHybridAdaptiveEnabled(false);
        break;
    case ShellBenchmarkProfile::InnodbLike:
        f.txn().setWalSyncMode(newdb::WalSyncMode::Full);
        f.txn().setWalNormalSyncIntervalMs(0);
        f.txn().setGroupCommitWindowMs(0);
        f.txn().setGroupCommitMaxBatchCommits(1);
        f.txn().setWalAdaptiveEnabled(false);
        f.txn().setHybridAdaptiveEnabled(false);
        break;
    case ShellBenchmarkProfile::HybridBalanced:
        f.txn().setWalSyncMode(newdb::WalSyncMode::Normal);
        f.txn().setWalNormalSyncIntervalMs(20);
        f.txn().setGroupCommitWindowMs(10);
        f.txn().setGroupCommitMaxBatchCommits(16);
        f.txn().setWalAdaptiveEnabled(true);
        f.txn().setHybridAdaptiveEnabled(true);
        break;
    case ShellBenchmarkProfile::NewdbDefault:
        f.txn().setWalSyncMode(newdb::WalSyncMode::Normal);
        f.txn().setWalNormalSyncIntervalMs(20);
        f.txn().setGroupCommitWindowMs(10);
        f.txn().setGroupCommitMaxBatchCommits(16);
        f.txn().setWalAdaptiveEnabled(true);
        f.txn().setHybridAdaptiveEnabled(false);
        break;
    }
}

newdb::lsm_lite::CompactionPolicy policy_for_profile(const ShellBenchmarkProfile p) {
    if (p == ShellBenchmarkProfile::LeveldbLike) {
        return newdb::lsm_lite::CompactionPolicy::SizeTiered;
    }
    return newdb::lsm_lite::CompactionPolicy::LeveledLite;
}

std::string profile_name(const ShellBenchmarkProfile p) {
    switch (p) {
    case ShellBenchmarkProfile::LeveldbLike: return "leveldb-like";
    case ShellBenchmarkProfile::InnodbLike: return "innodb-like";
    case ShellBenchmarkProfile::HybridBalanced: return "hybrid-balanced";
    case ShellBenchmarkProfile::NewdbDefault:
    default: return "newdb-default";
    }
}

newdb::lsm_lite::Options build_options(ShellState& st) {
    ShellStateFacade f(st);
    newdb::lsm_lite::Options opt;
    opt.enabled = f.txn().hotIndexEnabled();
    opt.segment_target_bytes = f.txn().segmentTargetBytes();
    opt.l0_compact_trigger = env_u64_or("NEWDB_LSM_L0_COMPACT_TRIGGER", 4);
    opt.l0_compact_batch = env_u64_or("NEWDB_LSM_L0_COMPACT_BATCH", 4);
    opt.compaction_async = env_bool_or("NEWDB_LSM_COMPACTION_ASYNC", false);
    opt.compaction_workers = env_u64_or("NEWDB_LSM_COMPACTION_WORKERS", 2);
    opt.compaction_max_pending = env_u64_or("NEWDB_LSM_COMPACTION_MAX_PENDING", 0);
    opt.compaction_reap_budget = env_u64_or("NEWDB_LSM_COMPACTION_REAP_BUDGET", 4);
    opt.leveled_l1_soft_segments = env_u64_or("NEWDB_LSM_LEVELED_L1_SOFT_SEGMENTS", 24);
    opt.leveled_l1_hard_segments = env_u64_or("NEWDB_LSM_LEVELED_L1_HARD_SEGMENTS", 48);
    const auto p = f.runtime_policy().profile;
    opt.compaction_policy = policy_for_profile(p);
    if (const char* raw = std::getenv("NEWDB_LSM_COMPACTION_POLICY")) {
        std::string s(raw);
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (s == "leveled_lite" || s == "leveled-lite") {
            opt.compaction_policy = newdb::lsm_lite::CompactionPolicy::LeveledLite;
        } else if (s == "size_tiered" || s == "size-tiered") {
            opt.compaction_policy = newdb::lsm_lite::CompactionPolicy::SizeTiered;
        }
    }
    opt.benchmark_profile = profile_name(p);
    return opt;
}

newdb::lsm_lite::Hooks build_hooks(ShellState& st) {
    newdb::lsm_lite::Hooks h;
    // Capture `st`, not a stack `ShellStateFacade`: hooks may run after this returns.
    h.on_memtable_flush = [&]() { ShellStateFacade(st).txn().onLsmMemtableFlush(); };
    h.on_compaction = [&]() { ShellStateFacade(st).txn().onLsmCompaction(); };
    h.on_read_segments_scanned = [&](const std::uint64_t n) { ShellStateFacade(st).txn().onLsmReadSegmentsScanned(n); };
    h.on_cache_lookup = [&](const bool hit) { ShellStateFacade(st).txn().onLsmSegmentCacheLookup(hit); };
    h.on_compaction_queue_depth = [&](const std::uint64_t p, const std::uint64_t i) {
        ShellStateFacade(st).txn().onLsmCompactionQueueDepth(p, i);
    };
    h.on_compaction_enqueue_skipped_backpressure = [&]() {
        ShellStateFacade(st).txn().onLsmCompactionEnqueueSkippedBackpressure();
    };
    h.on_compaction_bytes = [&](const std::uint64_t inb, const std::uint64_t outb) {
        ShellStateFacade(st).txn().onLsmCompactionBytes(inb, outb);
    };
    h.on_memtable_bytes = [&](const std::uint64_t n) { ShellStateFacade(st).txn().setLsmMemtableBytes(n); };
    h.on_segment_count = [&](const std::uint64_t n) { ShellStateFacade(st).txn().setLsmSegmentCount(n); };
    return h;
}

newdb::lsm_lite::TxnContext build_txn_ctx(ShellState& st) {
    ShellStateFacade f(st);
    newdb::lsm_lite::TxnContext t;
    t.in_txn = f.txn().inTransaction();
    t.txn_id = f.txn().getTxnId();
    return t;
}
} // namespace

void lsm_lite_record_writes(ShellState& st,
                            const std::string& data_file,
                            const std::vector<newdb::Row>& rows,
                            const bool deleted_flag) {
    apply_profile_defaults_once(st, resolve_profile());
    const auto opt = build_options(st);
    auto hooks = build_hooks(st);
    const auto txn = build_txn_ctx(st);
    newdb::lsm_lite::record_writes(opt, data_file, rows, deleted_flag, &txn, &hooks);
}

std::optional<LsmFindResult> lsm_lite_find_by_id(ShellState& st,
                                                 const std::string& data_file,
                                                 const int id) {
    apply_profile_defaults_once(st, resolve_profile());
    const auto opt = build_options(st);
    auto hooks = build_hooks(st);
    const auto txn = build_txn_ctx(st);
    const auto r = newdb::lsm_lite::find_by_id(opt, data_file, id, &txn, &hooks);
    if (!r.has_value()) return std::nullopt;
    LsmFindResult out;
    out.found = r->found;
    out.deleted = r->deleted;
    out.row = r->row;
    return out;
}

void lsm_lite_on_txn_commit(ShellState& st, const std::string& data_file, const std::int64_t txn_id) {
    apply_profile_defaults_once(st, resolve_profile());
    const auto opt = build_options(st);
    auto hooks = build_hooks(st);
    newdb::lsm_lite::TxnContext txn;
    txn.in_txn = true;
    txn.txn_id = txn_id;
    newdb::lsm_lite::on_txn_commit(opt, data_file, txn, &hooks);
}

void lsm_lite_on_txn_rollback(ShellState& st, const std::string& data_file, const std::int64_t txn_id) {
    apply_profile_defaults_once(st, resolve_profile());
    const auto opt = build_options(st);
    auto hooks = build_hooks(st);
    newdb::lsm_lite::TxnContext txn;
    txn.in_txn = true;
    txn.txn_id = txn_id;
    newdb::lsm_lite::on_txn_rollback(opt, data_file, txn, &hooks);
}

void lsm_lite_clear_txn_views(ShellState& st, const std::string& data_file) {
    (void)st;
    if (data_file.empty()) return;
    newdb::lsm_lite::clear_txn_views_for_data_file(data_file);
}

