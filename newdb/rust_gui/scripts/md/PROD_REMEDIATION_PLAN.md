# newdb Production Remediation Plan (Non-GUI)

This plan executes the findings from `newdb-prod-readiness-assessment.canvas.tsx` and focuses on non-GUI scope.

## P0 (Must Fix Before Production)

### 1) Build artifact portability / clean-room verification

- Issue: pre-generated build artifacts can retain absolute-path test metadata and break `ctest` on other machines.
- Action:
  - Always validate from a fresh build directory in CI.
  - Add a dedicated clean-room verification workflow.
  - Add a local script to reproduce CI behavior (`scripts/ci/verify_clean_reconfigure.ps1`).
- Acceptance:
  - `cmake -S . -B <fresh-dir>` then `ctest --output-on-failure` passes.
  - CI runs from clean workspace only (no reused generated test metadata).

## P1 (High Priority)

### 2) Security baseline pipeline

- Issue: no continuous security scanning baseline.
- Action:
  - Add CodeQL workflow for C/C++.
  - Add dependency review gate for PRs.
  - Add PR severity gate to block on open high/critical CodeQL alerts.
  - Add SBOM generation (SPDX JSON) and artifact retention.
  - Add provenance attestation for SBOM on protected-branch pushes.
- Acceptance:
  - Security workflows run on every PR.
  - High-severity findings are visible before merge.
  - PR fails when CodeQL reports open high/critical alerts for PR merge ref.
  - Every PR/push produces an SBOM artifact (`newdb-source-sbom`).
  - Pushes to `main/master` produce signed provenance attestation for SBOM.

### 3) CI hardening and path consistency

- Issue: legacy path assumptions (`new/`) can drift from current project layout (`newdb/`).
- Action:
  - Keep CI workflows colocated in this repo and use repository-root-relative paths.
  - Gate non-GUI C++ checks with deterministic build/test sequence.
- Acceptance:
  - Linux GCC and Linux Clang jobs pass using only current repo paths.
  - Windows clean-room job passes (configure/build/test + bench gate) via `verify_clean_reconfigure.ps1`.

## Branch Protection Baseline (Recommended)

- Require pull request reviews before merge (at least 1 approval).
- Require all status checks below to pass:
  - `observability-schema-gate`
  - `telemetry-schema-gate`
  - `c-api-abi-gate`
  - `linux-gcc`
  - `linux-clang`
  - `windows-clean-room`
  - `dependency-review`
  - `codeql`
  - `codeql-severity-gate`
  - `sbom`
- Require branches to be up to date before merging.
- Dismiss stale approvals when new commits are pushed.
- Restrict direct pushes to protected branches (`main` / `master`).

## P2 (Next Iteration)

### 4) Observability productization

- Action:
  - Export stable structured metrics (JSON/log fields) from soak/perf runs.
  - Define an operator-facing dashboard contract.
  - Version perf summary schema (`newdb.perf_summary.v1`) and validate in automation.
  - Version telemetry event schema (`newdb.telemetry.v1`) and validate in automation.
  - Standardize telemetry dimensions (`env/build/profile`) for downstream BI and log platforms.
- Acceptance:
  - `test_loop` summary includes `schema_version`.
  - Nightly soak runner validates summary JSON before trend append.
  - CI has `observability-schema-gate` to keep schema contract stable.
  - `test_loop` emits telemetry JSONL with `run_id` and phase events.
  - Telemetry events include stable dimensions: `env`, `build`, `profile`.
  - Nightly soak runner validates telemetry JSONL before trend append.
  - CI has `telemetry-schema-gate` to keep telemetry contract stable.

### 5) C API contract hardening

- Action:
  - Add explicit error code taxonomy and ABI compatibility checks.
  - Add C API version negotiation endpoints and last-error retrieval.
  - Add dedicated C API regression tests to prevent contract drift.
  - Publish explicit thread-safety contract and concurrency usage guidance.
  - Add ABI matrix doc and header symbol-surface CI gate.
  - Refine execution error semantics (e.g. `session_terminated`, `log_io`) for machine consumers.
  - Publish SDK-facing error recovery guide for caller retry/recreate/escalation policy.
- Acceptance:
  - Header exports ABI/version symbols and error code strings.
  - C API tests cover ABI negotiation and error code behavior.
  - C API tests include independent-handle concurrency smoke.
  - CI fails when C API symbol surface or ABI version drifts unexpectedly.
  - Session execute path distinguishes termination vs execution/log-IO failures via `last_error`.
  - Error handling guide maps each error code to recommended client action.

