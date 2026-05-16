add_executable(structdb_app
  main.cpp
)

target_link_libraries(structdb_app PRIVATE structdb_facade structdb_infra structdb_client_mdb structdb_client_embed)

if(MSVC)
  target_compile_options(structdb_app PRIVATE /W4 /utf-8 /permissive-)
else()
  target_compile_options(structdb_app PRIVATE -Wall -Wextra)
endif()
