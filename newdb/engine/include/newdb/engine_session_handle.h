#pragma once

#include <newdb/engine_session_opaque.h>

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Create an engine-owned session backed by `newdb::Session` (see
/// [`docs/dev/ENGINE_SESSION_HANDLE.md`](../../../docs/dev/ENGINE_SESSION_HANDLE.md)).
newdb_engine_session_t* newdb_engine_session_create(const char* data_dir,
                                                    const char* default_table,
                                                    const char* log_path,
                                                    uint32_t flags);

/// Idempotent destroy (safe on `NULL`).
void newdb_engine_session_destroy(newdb_engine_session_t* session);

#ifdef __cplusplus
}
#endif
