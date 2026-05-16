add_library(structdb_storage STATIC
  src/versioned_kv.cpp
  src/wal.cpp
  src/recovery_phase.cpp
  src/memtable.cpp
  src/memtable_skiplist.cpp
  src/memtable_manager.cpp
  src/byte_token_bucket.cpp
  src/manifest.cpp
  src/buffer_pool.cpp
  src/redo_log.cpp
  src/undo_log.cpp
  src/checkpoint.cpp
  src/lsm_state.cpp
  src/storage_engine_detail.cpp
  src/storage_engine.cpp
  src/storage_engine_open_wal.cpp
  src/storage_engine_put_undo.cpp
  src/storage_engine_read.cpp
  src/storage_engine_segments_worker_checkpoint.cpp
  src/storage_engine_compaction_lsm.cpp
  src/wal_coordinator.cpp
  src/checkpoint_undo_coordinator.cpp
  src/wal_replay_applier.cpp
  src/recovery_coordinator.cpp
  src/storage_telemetry.cpp
  src/compaction_coordinator.cpp
  src/compaction_io_executor.cpp
)

target_include_directories(structdb_storage PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_storage PUBLIC structdb_infra wf_waterfall)

if(MSVC)
  target_compile_options(structdb_storage PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_storage PRIVATE -Wall -Wextra)
endif()
