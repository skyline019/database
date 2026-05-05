#pragma once

#include <cstdint>
#include <functional>

namespace newdb {

/// Memory-heavy subsystems registered with the global registry (Phase 5 v2 closed loop).
enum class MemoryKind : int {
    PageCache = 0,
    EqSidecar = 1,
    QueryTemp = 2,
};

constexpr int kMemoryKindCount = 3;

/// Eviction hook signature: caller asks for at least `target_free` bytes; implementation must
/// release that many bytes via `memory_registry_release` and return the actual bytes freed.
using MemoryEvictor = std::function<std::uint64_t(std::uint64_t target_free)>;

/// Per-kind aggregated counters (used / evictions / admit rejects / bytes evicted).
struct MemoryRegistryTotals {
    std::uint64_t page_cache_used_bytes{0};
    std::uint64_t page_cache_evictions{0};
    std::uint64_t page_cache_admit_rejects{0};
    std::uint64_t page_cache_bytes_evicted_total{0};

    std::uint64_t sidecar_used_bytes{0};
    std::uint64_t sidecar_evictions{0};
    std::uint64_t sidecar_admit_rejects{0};
    std::uint64_t sidecar_bytes_evicted_total{0};

    std::uint64_t query_temp_used_bytes{0};
    std::uint64_t query_temp_evictions{0};
    std::uint64_t query_temp_admit_rejects{0};
    std::uint64_t query_temp_bytes_evicted_total{0};

    std::uint64_t global_used_bytes{0};
    std::uint64_t global_admit_rejects{0};
};

/// Per-kind cap (bytes). 0 = no hard cap (kind admit always succeeds; eviction still informs registry).
std::uint64_t memory_registry_kind_cap(MemoryKind kind);
std::uint64_t memory_registry_global_cap();

/// Try to admit `bytes` for `kind`; returns true on success and reserves the bytes.
/// On failure, increments per-kind admit rejects (and global rejects) and returns false.
[[nodiscard]] bool memory_registry_try_admit(MemoryKind kind, std::uint64_t bytes);

/// Release previously admitted bytes (idempotent at zero).
void memory_registry_release(MemoryKind kind, std::uint64_t bytes);

/// Notify the registry that an entry was evicted (book-keeping); does not call any evictor.
void memory_registry_record_eviction(MemoryKind kind, std::uint64_t bytes);

/// Register an eviction hook used by `try_admit` when capacity is tight. Pass `nullptr` to clear.
void memory_registry_register_evictor(MemoryKind kind, MemoryEvictor evictor);

/// Snapshot all counters atomically (best-effort under mutex).
MemoryRegistryTotals memory_registry_totals();

/// Test hook: zero counters and clear evictors (does not touch caller-side state).
void memory_registry_reset_for_test();

}  // namespace newdb
