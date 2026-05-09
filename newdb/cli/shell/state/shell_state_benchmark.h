#pragma once

#include <cstdint>

/// WAL/LSM benchmark presets for CLI demos (no `session.h` dependency).
enum class ShellBenchmarkProfile : std::uint8_t {
    NewdbDefault = 0,
    LeveldbLike = 1,
    InnodbLike = 2,
    HybridBalanced = 3,
};

struct ShellRuntimePolicy {
    ShellBenchmarkProfile profile{ShellBenchmarkProfile::NewdbDefault};
    bool initialized{false};
};
