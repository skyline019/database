# newdb C API SDK Error Recovery Guide

Scope: `include/newdb/c_api.h`, `src/c_api.cpp`

This document is SDK-facing guidance for handling `newdb_error_code` in production integrations.

## Recovery Classification

- Retryable after short backoff: `NEWDB_ERR_EXECUTION_FAILED`, `NEWDB_ERR_LOG_IO`
- Requires session rebuild: `NEWDB_ERR_INVALID_HANDLE`, `NEWDB_ERR_INTERNAL`, `NEWDB_ERR_SESSION_TERMINATED`
- Caller bug / contract violation (do not retry unchanged): `NEWDB_ERR_INVALID_ARGUMENT`

## Error Code to Recovery Strategy

| Error Code | Meaning | Retry? | Session Recreate? | Recommended SDK Action |
| --- | --- | --- | --- | --- |
| `NEWDB_OK` | Success | N/A | No | Continue normal flow. |
| `NEWDB_ERR_INVALID_ARGUMENT` | Invalid API input | No | No | Fail fast. Validate command text, table name, and output buffer contract before next call. |
| `NEWDB_ERR_INVALID_HANDLE` | Session handle invalid/stale | No | Yes | Destroy stale handle if owned, create a fresh session, then replay idempotent initialization. |
| `NEWDB_ERR_EXECUTION_FAILED` | Command execution failed | Yes (bounded) | Maybe | Retry 1-2 times with jitter for idempotent commands. If still failing, collect command context and escalate as business failure. |
| `NEWDB_ERR_INTERNAL` | Internal unexpected failure | No | Yes | Recreate session immediately, emit high-severity diagnostic, and stop blind retries. |
| `NEWDB_ERR_LOG_IO` | Log/tail IO failed | Yes (after remediation) | Maybe | Check disk/path/permission state first, then retry boundedly. Rebuild session if repeated IO failures persist. |
| `NEWDB_ERR_SESSION_TERMINATED` | Session ended by command (for example `EXIT`) | No | Yes | Treat as terminal session state. Create a new session for subsequent commands. |

## SDK Handling Flow (Recommended)

1. Call API (`newdb_session_execute`, `newdb_session_set_table`, etc.).
2. On non-zero return, call `newdb_session_last_error(handle)`.
3. Convert code to stable label with `newdb_error_code_string(code)`.
4. Route by class:
   - caller-error -> return typed SDK error to user
   - retryable -> bounded retry policy
   - rebuild-required -> close and recreate session
5. Emit one structured log with:
   - `error_code` / `error_name`
   - request id / run id
   - command category (not full sensitive payload)
   - retry count and whether session was rebuilt

## Suggested Retry Policy

- Max retries: 2
- Backoff: exponential (`100ms`, `300ms`) with jitter
- Retry only for idempotent/read commands unless caller explicitly opts in
- Stop retrying immediately when code changes from retryable to non-retryable

## Session Rebuild Checklist

When session rebuild is required:

1. `newdb_session_destroy(old_handle)` (if non-null and still owned by SDK).
2. `newdb_session_create(data_dir, table_name, log_file_path)`.
3. Re-apply SDK bootstrap actions (table selection, feature flags).
4. Resume traffic only after a successful health command.

## Integration Notes

- `newdb_session_last_error(nullptr)` returns `NEWDB_ERR_INVALID_HANDLE`; do not treat it as transport failure.
- `NEWDB_ERR_SESSION_TERMINATED` should not be logged as crash by default; it is often an expected lifecycle event.
- Keep caller logs stable by storing both integer code and `newdb_error_code_string` output.

## Minimal SDK Pseudocode Templates

### C/C++ (direct C API wrapper)

```cpp
// Pseudocode: bounded retry + optional session rebuild
int execute_with_recovery(newdb_session_handle* h, const char* cmd) {
    int max_retry = 2;
    for (int attempt = 0; attempt <= max_retry; ++attempt) {
        char out[4096] = {0};
        int rc = newdb_session_execute(*h, cmd, out, sizeof(out));
        if (rc == 0) return 0;

        int ec = newdb_session_last_error(*h);
        const char* name = newdb_error_code_string(ec);
        log_error("execute_failed", ec, name, attempt);

        if (ec == NEWDB_ERR_INVALID_ARGUMENT) return rc;  // caller bug
        if (ec == NEWDB_ERR_INVALID_HANDLE ||
            ec == NEWDB_ERR_INTERNAL ||
            ec == NEWDB_ERR_SESSION_TERMINATED) {
            newdb_session_destroy(*h);
            *h = newdb_session_create("data", "table", "newdb.log");
            return (*h == nullptr) ? -1 : rc;
        }

        // Retryable: EXECUTION_FAILED / LOG_IO
        if (attempt < max_retry) sleep_with_jitter_ms(100 * (attempt + 1));
    }
    return -1;
}
```

### Python (ctypes/cffi wrapper style)

```python
def execute_with_recovery(client, cmd: str):
    max_retry = 2
    for attempt in range(max_retry + 1):
        rc, output = client.execute(cmd)  # wraps newdb_session_execute
        if rc == 0:
            return output

        ec = client.last_error()          # wraps newdb_session_last_error
        name = client.error_name(ec)      # wraps newdb_error_code_string
        logger.warning("newdb_execute_failed", extra={"code": ec, "name": name, "attempt": attempt})

        if ec == client.ERR_INVALID_ARGUMENT:
            raise ValueError(f"invalid command/input: {cmd}")
        if ec in (client.ERR_INVALID_HANDLE, client.ERR_INTERNAL, client.ERR_SESSION_TERMINATED):
            client.recreate_session()
            raise RuntimeError(f"session rebuilt due to {name}; caller should retry at higher layer")
        if ec in (client.ERR_EXECUTION_FAILED, client.ERR_LOG_IO) and attempt < max_retry:
            sleep_with_jitter_ms(100 * (attempt + 1))
            continue
        raise RuntimeError(f"newdb execute failed: code={ec} name={name}")
```

### Go (SDK facade)

```go
func (c *Client) ExecuteWithRecovery(cmd string) (string, error) {
    const maxRetry = 2
    for attempt := 0; attempt <= maxRetry; attempt++ {
        out, rc := c.Execute(cmd) // wraps newdb_session_execute
        if rc == 0 {
            return out, nil
        }

        ec := c.LastError()  // wraps newdb_session_last_error
        name := c.ErrorName(ec)
        c.log.Warn("newdb execute failed", "code", ec, "name", name, "attempt", attempt)

        switch ec {
        case ErrInvalidArgument:
            return "", fmt.Errorf("invalid argument: %w", ErrBadInput)
        case ErrInvalidHandle, ErrInternal, ErrSessionTerminated:
            if err := c.RecreateSession(); err != nil {
                return "", fmt.Errorf("session rebuild failed: %w", err)
            }
            return "", fmt.Errorf("session rebuilt after %s; retry at caller boundary", name)
        case ErrExecutionFailed, ErrLogIO:
            if attempt < maxRetry {
                sleepWithJitter(100 * time.Millisecond * time.Duration(attempt+1))
                continue
            }
            fallthrough
        default:
            return "", fmt.Errorf("execute failed: code=%d name=%s", ec, name)
        }
    }
    return "", fmt.Errorf("unreachable")
}
```

