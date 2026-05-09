#pragma once

/**
 * Optional future hook: load a CLI/backend DLL that implements session execution for `newdb_shared`
 * without statically linking `newdb_capi_adapter`. Default builds ignore this header.
 *
 * Design sketch only — no runtime loader is wired in `c_api.cpp` yet.
 */

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct newdb_session_handle_opaque* newdb_session_backend_handle;

typedef struct newdb_cli_backend_vtable {
    int (*session_create)(void** out_impl,
                          const char* data_dir,
                          const char* table_name,
                          const char* log_file_path);
    void (*session_destroy)(void* impl);
    int (*session_execute)(void* impl, const char* command_line, char* out_buf, size_t out_cap);
} newdb_cli_backend_vtable;

#ifdef __cplusplus
}
#endif
