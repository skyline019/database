#pragma once

/// Opaque engine-owned session handle (future ABI). C entrypoints: [`engine_session_handle.h`](engine_session_handle.h);
/// ADR: [`docs/dev/ENGINE_SESSION_HANDLE.md`](../../../docs/dev/ENGINE_SESSION_HANDLE.md).

#ifdef __cplusplus
extern "C" {
#endif

typedef struct newdb_engine_session_opaque newdb_engine_session_t;

#ifdef __cplusplus
}
#endif
