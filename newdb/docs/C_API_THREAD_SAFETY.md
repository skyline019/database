# newdb C API Thread-Safety Contract

Scope: `include/newdb/c_api.h`

## Guarantees (current)

- Global metadata APIs are thread-safe:
  - `newdb_version_string`
  - `newdb_api_version_major/minor/patch`
  - `newdb_abi_version`
  - `newdb_negotiate_abi`
  - `newdb_error_code_string`
- Per-session APIs are safe when each thread uses a different `newdb_session_handle`.

## Non-Guarantees (current)

- Concurrent calls on the same session handle are **not** guaranteed thread-safe.
- Cross-thread sharing of one session requires external synchronization.

## Recommended Usage

- One thread ↔ one session handle.
- If sharing a handle is required, guard all C API calls with a mutex in the caller.

## Validation

- See `tests/test_c_api.cpp` for regression checks:
  - ABI/version negotiation
  - Error-code behavior
  - Independent-handle concurrency smoke

See also:

- `docs/C_API_ABI_MATRIX.md`
- `docs/C_API_ERROR_HANDLING.md`

