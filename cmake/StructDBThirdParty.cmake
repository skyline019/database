# fmt / spdlog / Google Benchmark: either FetchContent (plan) or vendored ThirdParty/ (default).

if(STRUCTDB_FETCH_FMT_SPDLOG_BENCHMARK)
  include(FetchContent)
  set(FETCHCONTENT_UPDATES_DISCONNECTED ON CACHE BOOL "" FORCE)
  message(STATUS "StructDB: third-party fmt/spdlog/benchmark via FetchContent")

  set(FMT_TEST OFF CACHE BOOL "" FORCE)
  set(FMT_DOC OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG 12.1.1
    GIT_SHALLOW TRUE
  )
  FetchContent_MakeAvailable(fmt)

  set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
  FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.15.3
    GIT_SHALLOW TRUE
  )
  FetchContent_MakeAvailable(spdlog)

  if(STRUCTDB_BUILD_BENCHMARKS)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
    FetchContent_Declare(
      benchmark
      GIT_REPOSITORY https://github.com/google/benchmark.git
      GIT_TAG v1.9.2
      GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(benchmark)
  endif()
else()
  message(STATUS "StructDB: third-party fmt/spdlog/benchmark from ThirdParty/ (set STRUCTDB_FETCH_FMT_SPDLOG_BENCHMARK=ON for FetchContent)")
  set(FMT_TEST OFF CACHE BOOL "" FORCE)
  set(FMT_DOC OFF CACHE BOOL "" FORCE)
  add_subdirectory(ThirdParty/fmt-main EXCLUDE_FROM_ALL)

  set(SPDLOG_FMT_EXTERNAL ON CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(SPDLOG_BUILD_EXAMPLE OFF CACHE BOOL "" FORCE)
  add_subdirectory(ThirdParty/spdlog-1.x EXCLUDE_FROM_ALL)

  if(STRUCTDB_BUILD_BENCHMARKS)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
    add_subdirectory(ThirdParty/benchmark-main EXCLUDE_FROM_ALL)
  endif()
endif()
