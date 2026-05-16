add_library(structdb_orchestrator STATIC
  src/orchestrator.cpp
)

target_include_directories(structdb_orchestrator PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_orchestrator PUBLIC structdb_runtime structdb_scheduler structdb_planner structdb_infra)

if(MSVC)
  target_compile_options(structdb_orchestrator PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_orchestrator PRIVATE -Wall -Wextra)
endif()
