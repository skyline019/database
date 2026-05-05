// Wal recovery helpers (phase 7 split): segment index pass + redo replay pass.
// Phases (doc §3.3): scan_segments → checkpoint discovery (in index pass) → redo replay (apply_redo_plan).

#include <newdb/wal_manager.h>
#include <newdb/wal_codec.h>
#include <newdb/wal/wal_redo_applier.h>
#include <newdb/wal/wal_redo_planner.h>
#include <newdb/wal/wal_segment_scanner.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <unordered_map>
#include <vector>

namespace newdb {

namespace {

std::uint64_t monotonic_ms() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

struct RecoverCheckpointTracker {
    int depth{0};
    std::uint64_t last_complete{0};
};

std::uint64_t checkpoint_boundary_from_payload(std::uint64_t hdr_lsn, const std::vector<uint8_t>& payload) {
    if (payload.empty()) {
        return hdr_lsn;
    }
    if (!walcodec::is_v1_payload(payload.data(), payload.size())) {
        return hdr_lsn;
    }
    walcodec::DecodedPayloadV1 dec{};
    const Status st = walcodec::decode_payload_v1(payload.data(), payload.size(), dec);
    if (!st.ok) {
        return hdr_lsn;
    }
    if (dec.has_checkpoint_snapshot_lsn) {
        return dec.checkpoint_snapshot_lsn;
    }
    return hdr_lsn;
}

void recover_track_checkpoint(RecoverCheckpointTracker& cp,
                              WalOp op,
                              std::uint64_t hdr_lsn,
                              const std::vector<uint8_t>& payload) {
    switch (op) {
    case WalOp::CHECKPOINT:
        cp.last_complete = std::max(cp.last_complete, hdr_lsn);
        break;
    case WalOp::CHECKPOINT_BEGIN:
        ++cp.depth;
        break;
    case WalOp::CHECKPOINT_END: {
        const std::uint64_t boundary = checkpoint_boundary_from_payload(hdr_lsn, payload);
        if (cp.depth > 0) {
            --cp.depth;
            if (cp.depth == 0) {
                cp.last_complete = std::max(cp.last_complete, boundary);
            }
        } else {
            cp.last_complete = std::max(cp.last_complete, boundary);
        }
        break;
    }
    default:
        break;
    }
}

} // namespace

std::uint32_t WalManager::recover_scan_checkpoint_bracket_depth_at_eof_nolock() const {
    RecoverCheckpointTracker cp{};
    for (const auto& path : wal_read_paths_nolock()) {
        FILE* rf = std::fopen(path.c_str(), "rb");
        if (rf == nullptr) {
            continue;
        }
        while (true) {
            WalRecordHeader hdr{};
            std::vector<uint8_t> payload;
            const Status st = read_record(rf, &hdr, payload);
            if (!st.ok) {
                break;
            }
            recover_track_checkpoint(cp, static_cast<WalOp>(hdr.type), hdr.lsn, payload);
        }
        std::fclose(rf);
    }
    return static_cast<std::uint32_t>(cp.depth);
}

void WalManager::recover_build_segment_index(std::vector<RecoverSegmentEntry>& seg_index, WalRecoveryStats* out_stats) {
    seg_index.clear();
    RecoverCheckpointTracker cp{};
    const std::uint64_t segment_index_scan_begin_ms = monotonic_ms();
    {
        namespace fs = std::filesystem;
        std::string inv_root = wal_dir_;
        if (inv_root.empty()) {
            fs::path wp(wal_path_);
            inv_root = wp.parent_path().string();
        }
        out_stats->wal_dir_inventory_dot_wal_files =
            static_cast<std::uint64_t>(list_wal_segment_paths(inv_root).size());
    }
    // Phase: scan_segments (+ checkpoint discovery via `recover_track_checkpoint`).
    for (const auto& path : wal_read_paths_nolock()) {
        FILE* rf = std::fopen(path.c_str(), "rb");
        if (rf == nullptr) {
            continue;
        }
        WalRecordHeader hdr{};
        std::vector<uint8_t> payload;
        RecoverSegmentEntry ent{};
        ent.path = path;
        while (true) {
            const long rec_pos = std::ftell(rf);
            payload.clear();
            Status st = read_record(rf, &hdr, payload);
            if (!st.ok) {
                if (ent.records == 0) {
                    if (st.message != "EOF") {
                        ++out_stats->segment_index_bad_header_stops;
                    }
                } else {
                    if (st.message == "EOF" || st.message == "fread payload failed") {
                        ++out_stats->segment_index_partial_tail_stops;
                    } else {
                        ++out_stats->segment_index_bad_header_stops;
                    }
                }
                break;
            }
            recover_track_checkpoint(cp, static_cast<WalOp>(hdr.type), hdr.lsn, payload);
            if (ent.records == 0) {
                ent.min_lsn = hdr.lsn;
                ent.max_lsn = hdr.lsn;
            } else {
                ent.min_lsn = std::min(ent.min_lsn, hdr.lsn);
                ent.max_lsn = std::max(ent.max_lsn, hdr.lsn);
            }
            ++ent.records;
            if (ent.records == 1 || (ent.records % 128u) == 0u) {
                const std::uint64_t lsn_value = hdr.lsn;
                ent.lsn_offsets.emplace_back(lsn_value, rec_pos);
            }
        }
        std::fclose(rf);
        if (ent.records == 0) {
            ++out_stats->skipped_segments;
            continue;
        }
        ++out_stats->indexed_segments;
        out_stats->indexed_records += ent.records;
        out_stats->indexed_offsets += ent.lsn_offsets.size();
        seg_index.push_back(std::move(ent));
    }
    std::sort(seg_index.begin(), seg_index.end(), [](const RecoverSegmentEntry& a, const RecoverSegmentEntry& b) {
        return a.min_lsn < b.min_lsn;
    });
    const std::uint64_t segment_index_scan_end_ms = monotonic_ms();
    out_stats->checkpoint_scan_ms =
        (segment_index_scan_end_ms >= segment_index_scan_begin_ms)
            ? (segment_index_scan_end_ms - segment_index_scan_begin_ms)
            : 0;
    out_stats->last_complete_checkpoint_lsn = cp.last_complete;
    out_stats->incomplete_checkpoint_count = static_cast<std::uint64_t>(cp.depth);
    // Incomplete checkpoint bracketing at EOF (BEGIN without matching END): last_complete unchanged.
    if (cp.depth != 0) {
        out_stats->checkpoint_midpoint_recovery_count +=
            static_cast<std::uint64_t>(cp.depth);
    }
}

Status WalManager::recover_replay_segments(HeapTable* out_table,
                                           const TableSchema& schema,
                                           const std::vector<RecoverSegmentEntry>& seg_index,
                                           WalRecoveryStats* out_stats,
                                           const bool enable_offset_seek,
                                           const std::uint64_t replay_start_lsn) {
    wal_recovery::WalRedoPlanner planner(schema, out_table->name);
    const std::uint64_t cp_lsn = out_stats->last_complete_checkpoint_lsn;
    bool decode_failed = false;
    std::string decode_error;
    std::uint64_t commit_apply_ms = 0;
    std::uint64_t commit_apply_count = 0;
    std::uint64_t commit_apply_skipped = 0;

    for (const auto& ent : seg_index) {
        ++out_stats->scanned_segments;
        FILE* rf = std::fopen(ent.path.c_str(), "rb");
        if (rf == nullptr) {
            continue;
        }
        if (enable_offset_seek && replay_start_lsn > 0 && !ent.lsn_offsets.empty() && ent.max_lsn < replay_start_lsn) {
            std::fclose(rf);
            ++out_stats->skipped_segments;
            out_stats->seek_skipped_records += ent.records;
            continue;
        }
        if (enable_offset_seek && replay_start_lsn > 0 && !ent.lsn_offsets.empty() && ent.min_lsn < replay_start_lsn) {
            const auto it = std::lower_bound(
                ent.lsn_offsets.begin(), ent.lsn_offsets.end(), replay_start_lsn,
                [](const std::pair<std::uint64_t, long>& a, const std::uint64_t target) {
                    return a.first < target;
                });
            if (it != ent.lsn_offsets.end()) {
                if (std::fseek(rf, it->second, SEEK_SET) != 0) {
                    ++out_stats->offset_seek_fseek_fallback_count;
                    (void)std::fseek(rf, 0, SEEK_SET);
                }
            } else {
                ++out_stats->offset_seek_fseek_fallback_count;
                (void)std::fseek(rf, 0, SEEK_SET);
            }
        }
        WalRecordHeader hdr{};
        std::vector<uint8_t> payload;
        while (true) {
            payload.clear();
            Status st = read_record(rf, &hdr, payload);
            if (!st.ok) {
                break;
            }
            ++out_stats->records_read;
            if (cp_lsn > 0 && hdr.lsn > cp_lsn) {
                ++out_stats->records_after_checkpoint;
            }
            if (!verify_checksum(&hdr, payload.data())) {
                std::fclose(rf);
                ++out_stats->checksum_failures;
                out_stats->end_ms = wall_clock_ms();
                last_recovery_stats_ = *out_stats;
                return Status::Fail("WAL record checksum mismatch at LSN " + std::to_string(hdr.lsn));
            }

            const std::uint64_t prev_decode_failures = planner.decode_failures();
            const auto rec_start = monotonic_ms();
            const WalOp op = static_cast<WalOp>(hdr.type);
            const bool is_commit = (op == WalOp::COMMIT);
            const std::size_t commits_before =
                is_commit ? planner.records_seen() /* placeholder; we read committed_ops via probe below */
                          : 0;
            (void)commits_before;
            // Plan-side feed (no heap writes inside planner).
            std::size_t pending_committed_before = 0;
            if (is_commit) {
                // Snapshot committed_ops size by using `finalize_view` style probe: not exposed; instead
                // we look at txn_ops after feed. To keep apply scoped per-COMMIT, pull the staged ops by
                // calling a scratch finalize *only when a COMMIT is fed*. We cannot pre-know how many ops
                // belong to this COMMIT without coupling to internal state, so we apply per-segment below.
                pending_committed_before = 0;
            }
            planner.feed_record(hdr, payload);
            const std::uint64_t this_decode_failures = planner.decode_failures() - prev_decode_failures;
            if (this_decode_failures > 0) {
                std::fclose(rf);
                out_stats->decode_failures += this_decode_failures;
                out_stats->end_ms = wall_clock_ms();
                last_recovery_stats_ = *out_stats;
                decode_failed = true;
                decode_error = "failed to decode WAL payload";
                break;
            }
            switch (op) {
            case WalOp::COMMIT: {
                out_stats->redo_plan_ms += monotonic_ms() - rec_start;
                const auto apply_start = monotonic_ms();
                wal_recovery::RedoPlanSummary mid = planner.finalize();
                const wal_recovery::WalRedoApplier applier(*out_table);
                const wal_recovery::ApplyStats apply_stats = applier.apply(mid);
                commit_apply_count += apply_stats.records_applied;
                commit_apply_skipped += apply_stats.skipped;
                out_stats->apply_count += apply_stats.records_applied;
                commit_apply_ms += monotonic_ms() - apply_start;
                break;
            }
            case WalOp::CHECKPOINT:
            case WalOp::CHECKPOINT_BEGIN:
                ++out_stats->checkpoint_begin_count;
                out_stats->redo_plan_ms += monotonic_ms() - rec_start;
                break;
            case WalOp::CHECKPOINT_END:
                ++out_stats->checkpoint_end_count;
                out_stats->redo_plan_ms += monotonic_ms() - rec_start;
                break;
            default:
                out_stats->redo_plan_ms += monotonic_ms() - rec_start;
                break;
            }
            current_lsn_ = std::max(current_lsn_, hdr.lsn);
        }
        std::fclose(rf);
        if (decode_failed) {
            return Status::Fail(decode_error);
        }
    }

    wal_recovery::RedoPlanSummary tail = planner.finalize();
    out_stats->uncommitted_txn_discarded_count += tail.uncommitted_txn_discarded_count;
    out_stats->recovery_uncommitted_records_ignored +=
        tail.recovery_uncommitted_records_ignored;
    out_stats->redo_apply_ms += commit_apply_ms;
    (void)commit_apply_skipped;
    return Status::Ok();
}

Status WalManager::capture_recovery_scan_stats(WalRecoveryStats* out) {
    if (!out) {
        return Status::Fail("capture_recovery_scan_stats: null stats");
    }
    WalRecoveryStats partial{};
    std::vector<RecoverSegmentEntry> seg_index;
    recover_build_segment_index(seg_index, &partial);
    *out = partial;
    return Status::Ok();
}

} // namespace newdb
