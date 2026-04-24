# newdb C API ABI Matrix

This matrix records ABI evolution and contract checks for `include/newdb/c_api.h`.

## ABI History

| ABI Version | C API Version | Status | Notes |
| --- | --- | --- | --- |
| 1 | 0.3.0 | current | Adds finer-grained C API error codes (`log_io`, `session_terminated`) while keeping ABI stable. |

## Compatibility Rule

- Consumers should call `newdb_negotiate_abi(NEWDB_C_API_ABI_VERSION)` during startup.
- A return value of `1` means compatibility; `0` means incompatible ABI.

## Stability Gates

- Header symbol surface is checked by:
  - `scripts/validate/check_c_api_abi.py`
  - expected list: `scripts/validate/c_api_expected_symbols.txt`
- CI gate: `c-api-abi-gate`

Related docs:

- `docs/C_API_THREAD_SAFETY.md`
- `docs/C_API_ERROR_HANDLING.md`

If symbols are intentionally added/removed:
1. Update `scripts/validate/c_api_expected_symbols.txt`
2. If breaking, increment `NEWDB_C_API_ABI_VERSION`
3. Update this matrix with rationale

