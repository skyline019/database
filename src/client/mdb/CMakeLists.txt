add_library(structdb_client_mdb STATIC
  src/mdb_engine_ports.cpp
  src/mdb_runner.cpp
  src/mdb_ops_string_wire.cpp
  src/mdb_ops_logical_index.cpp
  src/mdb_ops_predicate.cpp
  src/mdb_persistence.cpp
  src/mdb_ops_reorder.cpp
  src/mdb_ops_pages_journal_import.cpp
  src/mdb_query_paging.cpp
  src/mdb_ops_txn_log.cpp
  src/mdb_dispatch.cpp
  src/mdb_command_parser.cpp
  src/mdb_session_flags.cpp
  src/mdb_session_durability.cpp
  src/mdb_named_index.cpp
  src/mdb_ops_aggregate.cpp
  src/mdb_ops_agg_cache.cpp
)

target_include_directories(structdb_client_mdb PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_include_directories(structdb_client_mdb PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_client_mdb PUBLIC structdb_client_embed structdb_facade structdb_infra)

if(MSVC)
  target_compile_options(structdb_client_mdb PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_client_mdb PRIVATE -Wall -Wextra)
endif()
