add_library(structdb_client_embed STATIC
  src/embed_client.cpp
)

target_include_directories(structdb_client_embed PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_client_embed PUBLIC structdb_facade structdb_infra)

if(MSVC)
  target_compile_options(structdb_client_embed PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_client_embed PRIVATE -Wall -Wextra)
endif()
