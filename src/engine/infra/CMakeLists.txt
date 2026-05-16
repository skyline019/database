add_library(structdb_infra STATIC
  src/long_task_progress.cpp
  src/logging.cpp
  src/tracer.cpp
  src/thread_compaction_sched.cpp
  src/metrics.cpp
  src/file_handle.cpp
  src/thread_pool.cpp
  src/leak_detector.cpp
  src/io_uring_seq.cpp
)

option(STRUCTDB_WITH_IOCP "Build IOCP sequential file writer for WAL (MSVC + Windows only)" ON)
option(STRUCTDB_WITH_IO_URING "Build io_uring sequential file writer for WAL (Linux + liburing only)" OFF)
if(MSVC AND WIN32 AND STRUCTDB_WITH_IOCP)
  target_sources(structdb_infra PRIVATE src/io_iocp.cpp)
  target_compile_definitions(structdb_infra PUBLIC STRUCTDB_HAVE_IOCP=1)
else()
  target_sources(structdb_infra PRIVATE src/io_iocp.cpp)
endif()

if(STRUCTDB_WITH_IO_URING)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "STRUCTDB_WITH_IO_URING is only supported on Linux (found ${CMAKE_SYSTEM_NAME})")
  endif()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(LIBURING REQUIRED liburing)
  target_compile_definitions(structdb_infra PUBLIC STRUCTDB_HAVE_IO_URING=1)
  target_include_directories(structdb_infra PUBLIC ${LIBURING_INCLUDE_DIRS})
  target_link_directories(structdb_infra PUBLIC ${LIBURING_LIBRARY_DIRS})
  target_link_libraries(structdb_infra PUBLIC ${LIBURING_LIBRARIES})
endif()

target_include_directories(structdb_infra
  PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_infra PUBLIC
  fmt::fmt
  spdlog::spdlog
)

if(MSVC)
  target_compile_options(structdb_infra PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_infra PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()
