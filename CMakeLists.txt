cmake_minimum_required(VERSION 3.20)
project(StructDB VERSION 0.1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(STRUCTDB_STATIC_MSVC_RUNTIME "Use /MT with MSVC (match gtest_capi default)" ON)
option(STRUCTDB_BUILD_TESTS "Build StructDB tests" ON)
option(STRUCTDB_BUILD_BENCHMARKS "Build StructDB microbenchmarks" ON)
option(STRUCTDB_ENABLE_PERF_GATE
  "Register ctest structdb_perf_gate (needs STRUCTDB_BUILD_BENCHMARKS, Python 3.8+; see POLICY §6.2)"
  OFF)
option(STRUCTDB_BUILD_CAPI_SHARED
  "Also build structdb_capi_shared (FFI) and structdb_capi_shared_smoke; static structdb_capi remains the default link for in-tree C++ tests"
  ON)
option(STRUCTDB_FETCH_FMT_SPDLOG_BENCHMARK
  "Use CMake FetchContent for fmt, spdlog, and Google Benchmark (OFF = ThirdParty/ vendors)"
  OFF)

if(MSVC AND STRUCTDB_STATIC_MSVC_RUNTIME)
  set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

if(MINGW OR (WIN32 AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU"))
  message(STATUS "StructDB: MinGW/MSYS2 detected — STRUCTDB_STATIC_MSVC_RUNTIME applies only to MSVC.")
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# fmt / spdlog / benchmark: FetchContent (plan) or vendored sources (default, offline-friendly).
include(StructDBThirdParty)

set(CRC32C_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(CRC32C_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(CRC32C_USE_GLOG OFF CACHE BOOL "" FORCE)
set(CRC32C_INSTALL OFF CACHE BOOL "" FORCE)
add_subdirectory(ThirdParty/crc32c-main EXCLUDE_FROM_ALL)

set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
set(ROARING_USE_CPM OFF CACHE BOOL "" FORCE)
set(ENABLE_ROARING_MICROBENCHMARKS OFF CACHE BOOL "" FORCE)
add_subdirectory(ThirdParty/CRoaring-master EXCLUDE_FROM_ALL)

# GoogleTest C API (uses local googletest under ThirdParty/gtest_capi when configured there)
set(GTEST_CAPI_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
set(GTEST_CAPI_MSVC_STATIC_RUNTIME "${STRUCTDB_STATIC_MSVC_RUNTIME}" CACHE BOOL "" FORCE)
# With /MT, shared gtest.dll + test EXE use separate CRT heaps → MSVC Debug heap assertions in gtest.
# Static gtest + static gtest_capi keeps a single heap for the test process.
if(MSVC AND STRUCTDB_STATIC_MSVC_RUNTIME)
  set(GTEST_CAPI_BUILD_SHARED OFF CACHE BOOL "" FORCE)
endif()
add_subdirectory(ThirdParty/gtest_capi EXCLUDE_FROM_ALL)

add_subdirectory(src/Base)
add_subdirectory(src/engine/infra)
add_subdirectory(src/engine/planner)
add_subdirectory(src/engine/scheduler)
add_subdirectory(src/engine/runtime)
add_subdirectory(src/engine/orchestrator)
add_subdirectory(src/engine/facade)
add_subdirectory(src/engine/storage)
add_subdirectory(src/client/embed)
add_subdirectory(src/client/mdb)
add_subdirectory(src/c_api)
add_subdirectory(src/app)

if(STRUCTDB_BUILD_TESTS OR STRUCTDB_ENABLE_PERF_GATE)
  enable_testing()
endif()

if(STRUCTDB_BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(STRUCTDB_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
