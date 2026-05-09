#pragma once

#include "cli/shell/state/shell_state_lsm.h"
#include "cli/shell/state/shell_state_sidecar.h"

/// Bundles LSM shell cache + sidecar tuning (opaque to shell_state.h).
struct ShellLsmSidecarRuntime {
    LsmShellCache lsm;
    SidecarShellTuning sidecar;
};
