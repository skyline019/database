add_library(structdb_scheduler STATIC
  src/budget.cpp
  src/scheduler.cpp
)

target_include_directories(structdb_scheduler PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_scheduler PUBLIC structdb_planner structdb_infra)

if(MSVC)
  target_compile_options(structdb_scheduler PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_scheduler PRIVATE -Wall -Wextra)
endif()
