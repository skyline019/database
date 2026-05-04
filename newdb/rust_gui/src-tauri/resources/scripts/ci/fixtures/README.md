# CI fixtures

## `runtime_stats_bench_gate_minimal.jsonl`

Two-line `newdb.runtime_stats.v1` sample used when `verify_clean_reconfigure.ps1` is run with **`-BenchGateStorage`** and/or **`-BenchGateWalRecovery`**: passed to `ci_bench_gate.py` as `--runtime-jsonl` so `newdb_runtime_report` runs storage / WAL-recovery gates against a **known-valid** contract.

Validate locally:

```bash
python3 newdb/scripts/validate/validate_runtime_stats.py newdb/scripts/ci/fixtures/runtime_stats_bench_gate_minimal.jsonl
```

## Archive manifest (v2)

From repo `newdb/`:

```bash
python3 scripts/ci/capture_baseline.py --emit-archive-contract
python3 scripts/ci/capture_baseline.py --write-archive-manifest out/baseline_manifest.json --ctest-config RelWithDebInfo
```

See [`PERF_AND_CI_BUDGETS.md`](../../docs/ci/PERF_AND_CI_BUDGETS.md) §4.
