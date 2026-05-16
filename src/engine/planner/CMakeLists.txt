add_library(structdb_planner STATIC
  src/execution_plan.cpp
)

target_include_directories(structdb_planner PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_planner PUBLIC structdb_infra)

if(MSVC)
  target_compile_options(structdb_planner PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_planner PRIVATE -Wall -Wextra)
endif()
