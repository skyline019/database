#pragma once

#include <cstdint>
#include <string>

/// Sidecar invalidation tuning held in `ShellState` (via `ShellLsmSidecarRuntime::sidecar`; accessor `ShellState::sidecar()`).
struct SidecarShellTuning {
    std::uint64_t sidecar_pending_writes{0};
    std::uint64_t sidecar_invalidate_every_n{0};
    std::uint8_t sidecar_invalidate_mode{0};
};
