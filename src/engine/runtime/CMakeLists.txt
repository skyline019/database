add_library(structdb_runtime STATIC
  src/graph_executor.cpp
)

target_include_directories(structdb_runtime PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_runtime PUBLIC structdb_scheduler structdb_planner structdb_infra)

if(MSVC)
  target_compile_options(structdb_runtime PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_runtime PRIVATE -Wall -Wextra)
endif()
