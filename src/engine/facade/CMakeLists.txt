add_library(structdb_facade STATIC
  src/service_container.cpp
  src/config.cpp
  src/engine.cpp
)

target_include_directories(structdb_facade PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_facade PUBLIC
  structdb_orchestrator
  structdb_storage
)

if(MSVC)
  target_compile_options(structdb_facade PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_facade PRIVATE -Wall -Wextra)
endif()
