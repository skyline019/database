#pragma once

/// Documentation-only anchor for logical layering of shell session state.
/// See `docs/dev/SHELL_STATE_LAYERING.md`. `ShellState` remains a single aggregate in `shell_state.h`.
///
/// Thin surfaces (option C): `shell_state_fwd.h` (forward declare), `shell_state_paths.h` (path helpers), `shell_state_benchmark.h` (benchmark profile + runtime policy, no `session.h`).
/// Opaque bundles: `ShellTxnWhereRuntime`, `ShellLsmSidecarRuntime` (see `shell_state_txn_where_runtime_impl.h`, `shell_state_lsm_sidecar_runtime_impl.h`).
