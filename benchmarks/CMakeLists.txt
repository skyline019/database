add_executable(structdb_bench
  memtable_bench.cpp
  wal_bench.cpp
  compaction_bench.cpp
  engine_bench.cpp
)

target_link_libraries(structdb_bench PRIVATE
  benchmark::benchmark_main
  structdb_storage
)

if(MSVC)
  target_compile_options(structdb_bench PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_bench PRIVATE -Wall -Wextra)
endif()

option(STRUCTDB_ENABLE_PERF_GATE
  "Register ctest structdb_perf_gate (Python 3.8+; compares structdb_bench JSON to baselines/structdb_bench_baseline.json)"
  OFF)

if(STRUCTDB_ENABLE_PERF_GATE)
  if(NOT STRUCTDB_BUILD_BENCHMARKS)
    message(FATAL_ERROR "STRUCTDB_ENABLE_PERF_GATE requires STRUCTDB_BUILD_BENCHMARKS=ON")
  endif()
  find_package(Python3 3.8 COMPONENTS Interpreter REQUIRED)
  add_test(
    NAME structdb_perf_gate
    COMMAND Python3::Interpreter
      "${CMAKE_CURRENT_SOURCE_DIR}/scripts/compare_bench.py"
      "--bench-exe" "$<TARGET_FILE:structdb_bench>"
      "--baseline" "${CMAKE_CURRENT_SOURCE_DIR}/baselines/structdb_bench_baseline.json"
      "--max-ratio" "1.5"
  )
  set_tests_properties(structdb_perf_gate PROPERTIES
    LABELS "perf"
    RESOURCE_LOCK structdb_perf_gate
  )
endif()
