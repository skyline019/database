cmake_minimum_required(VERSION 3.16)
project(newdb LANGUAGES CXX)

if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(NEWDB_ENABLE_COVERAGE "Compile/link with coverage (gcov/lcov; GCC or Clang)" OFF)

# Parent of this directory is the repository root (contains waterfall/).
get_filename_component(NEWDB_REPO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)

set(NEWDB_WATERFALL_CONFIG "${NEWDB_REPO_ROOT}/waterfall/config.h")

function(newdb_force_include_waterfall_config target)
    if(MSVC)
        target_compile_options(${target} PRIVATE "/FI${NEWDB_WATERFALL_CONFIG}")
    else()
        target_compile_options(${target} PRIVATE "-include${NEWDB_WATERFALL_CONFIG}")
    endif()
endfunction()

if(NEWDB_ENABLE_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(--coverage -Og -g)
    add_link_options(--coverage)
endif()

add_library(newdb_core STATIC
    src/schema.cpp
    src/schema_io.cpp
    src/heap_table.cpp
    src/heap_file_read_view.cpp
    src/tuple_codec.cpp
    src/heap_page_waterfall.cpp
    src/page_io.cpp
    src/session.cpp
    src/catalog.cpp
    src/wal_manager.cpp
    src/wal_codec.cpp
    src/mvcc.cpp
    src/session_history.cpp
    src/error_format.cpp
    src/crc32c_compat.cpp
    "${NEWDB_REPO_ROOT}/waterfall/storage/page.cc"
    "${NEWDB_REPO_ROOT}/waterfall/storage/record.cc"
    "${NEWDB_REPO_ROOT}/waterfall/storage/error.cc"
    "${NEWDB_REPO_ROOT}/waterfall/utils/arena.cc"
    "${NEWDB_REPO_ROOT}/waterfall/utils/error.cc"
    "${NEWDB_REPO_ROOT}/waterfall/utils/system_errors.cc"
)
set_target_properties(newdb_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(newdb_core PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${NEWDB_REPO_ROOT}"
)

newdb_force_include_waterfall_config(newdb_core)

option(NEWDB_BUILD_SHARED "Build shared library (C ABI) for cross-language usage" ON)
if(NEWDB_BUILD_SHARED)
    add_library(newdb_shared SHARED
        src/c_api.cpp
    )
    target_include_directories(newdb_shared PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/demo"
        "${NEWDB_REPO_ROOT}"
    )
    target_link_libraries(newdb_shared PRIVATE newdb_core)
    if(WIN32)
        target_compile_definitions(newdb_shared PRIVATE NEWDB_SHARED_BUILD)
    endif()
    set_target_properties(newdb_shared PROPERTIES OUTPUT_NAME "newdb")
    newdb_force_include_waterfall_config(newdb_shared)
endif()

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(CRC32C libcrc32c)
endif()

if(CRC32C_FOUND)
    target_include_directories(newdb_core PRIVATE ${CRC32C_INCLUDE_DIRS})
    target_link_libraries(newdb_core PUBLIC ${CRC32C_LIBRARIES})
endif()

add_executable(newdb_smoke tools/newdb_smoke.cpp)
target_link_libraries(newdb_smoke PRIVATE newdb_core)
newdb_force_include_waterfall_config(newdb_smoke)

add_executable(newdb_perf tools/newdb_perf.cpp)
newdb_force_include_waterfall_config(newdb_perf)

add_executable(newdb_concurrent_perf tools/newdb_concurrent_perf.cpp)
target_link_libraries(newdb_concurrent_perf PRIVATE newdb_core)
newdb_force_include_waterfall_config(newdb_concurrent_perf)

set(NEWDB_DEMO_LIB_SOURCES
    demo/demo_runner.cc
    demo/demo_diag.cc
    demo/demo_shell.cc
    demo/demo_export.cc
    demo/demo_commands.cc
    demo/demo_cli.cpp
    demo/table_view.cc
    demo/where.cc
    demo/schema_catalog.cc
    demo/condition.cc
    demo/utils.cc
    demo/logging.cc
    demo/import.cc
    demo/txn_manager.cc
    demo/bptree_index.cc
    demo/page_index_sidecar.cc
    demo/equality_index_sidecar.cc
    demo/eq_bloom.cc
    demo/covering_index_sidecar.cc
    demo/visibility_checkpoint_sidecar.cc
    demo/sidecar_wal_lsn.cpp
)
add_library(newdb_demo_lib STATIC ${NEWDB_DEMO_LIB_SOURCES})
set_target_properties(newdb_demo_lib PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(newdb_demo_lib PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/demo"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${NEWDB_REPO_ROOT}"
)
target_link_libraries(newdb_demo_lib PUBLIC newdb_core)
newdb_force_include_waterfall_config(newdb_demo_lib)

if(NEWDB_BUILD_SHARED)
    target_link_libraries(newdb_shared PRIVATE newdb_demo_lib)
endif()

add_executable(newdb_demo demo/demo_main.cc)
target_link_libraries(newdb_demo PRIVATE newdb_demo_lib)
target_compile_definitions(newdb_demo PRIVATE "NEWDB_DISPLAY_PROJECT=\"newdb\"")
newdb_force_include_waterfall_config(newdb_demo)

if(MINGW)
    # Reduce runtime dependency issues on target machines
    # (e.g. missing pthread_cond_timedwait64 in mismatched libwinpthread-1.dll).
    set(NEWDB_STATIC_WINPTHREAD_FLAG "-Wl,-Bstatic,-l:libwinpthread.a,-Bdynamic")
    target_link_options(newdb_demo PRIVATE
        -static-libgcc
        -static-libstdc++
        ${NEWDB_STATIC_WINPTHREAD_FLAG}
    )
    target_link_options(newdb_perf PRIVATE
        -static-libgcc
        -static-libstdc++
        ${NEWDB_STATIC_WINPTHREAD_FLAG}
    )
    if(NEWDB_BUILD_SHARED)
        target_link_options(newdb_shared PRIVATE
            -static-libgcc
            -static-libstdc++
            ${NEWDB_STATIC_WINPTHREAD_FLAG}
        )
    endif()
endif()

# ---- Qt GUI (optional; independent executable from newdb_demo CLI) ----
option(NEWDB_BUILD_GUI "Build newdb_gui (Qt Widgets; requires Qt5>=5.12 or Qt6)" OFF)
if(NEWDB_BUILD_GUI)
    set(CMAKE_AUTOMOC ON)
    find_package(Qt6 QUIET COMPONENTS Widgets)
    if(Qt6_FOUND)
        set(NEWDB_GUI_QT Qt6::Widgets)
    else()
        find_package(Qt5 5.12 REQUIRED COMPONENTS Widgets)
        set(NEWDB_GUI_QT Qt5::Widgets)
    endif()

    file(GLOB_RECURSE NEWDB_GUI_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/gui/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/gui/*.h"
    )
    add_executable(newdb_gui ${NEWDB_GUI_SOURCES})
    target_include_directories(newdb_gui PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/gui"
        "${CMAKE_CURRENT_SOURCE_DIR}/demo"
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${NEWDB_REPO_ROOT}"
    )
    target_link_libraries(newdb_gui PRIVATE newdb_demo_lib ${NEWDB_GUI_QT})
    target_compile_definitions(newdb_gui PRIVATE "NEWDB_DISPLAY_PROJECT=\"newdb\"")
    newdb_force_include_waterfall_config(newdb_gui)
endif()

option(NEWDB_BUILD_TESTS "Build GoogleTest suite (newdb_tests)" ON)

if(NEWDB_BUILD_TESTS)
    include(CTest)
    include(FetchContent)
    # Tarball avoids slow/blocked git clones in some environments.
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    add_executable(newdb_tests
        tests/test_status.cpp
        tests/test_schema.cpp
        tests/test_schema_io.cpp
        tests/test_heap_table.cpp
        tests/test_heap_page.cpp
        tests/test_tuple_codec.cpp
        tests/test_page_io.cpp
        tests/test_session.cpp
        tests/test_catalog.cpp
        tests/test_mvcc_visibility.cpp
        tests/test_demo_logging_history.cpp
        tests/test_demo_txn_wal.cpp
        tests/test_demo_where.cpp
        tests/test_demo_schema_catalog.cpp
        tests/test_demo_cli.cpp
        tests/test_demo_mdb_stop.cpp
        tests/test_demo_utils.cpp
        tests/test_c_api.cpp
        tests/test_error_format.cpp
        tests/test_wal_codec.cpp
        tests/test_wal_truncate.cpp
        tests/test_wal_segment.cpp
        tests/test_wal_concurrency.cpp
        tests/test_wal_recovery_indexed.cpp
        tests/test_txn_file_lock.cpp
        tests/test_sidecar_wal_lsn_stress.cpp
        tests/test_visibility_checkpoint_sidecar.cpp
        tests/test_covering_index_sidecar.cpp
        tests/test_covering_projection_sidecar.cpp
        tests/test_eq_sidecar_bucket.cpp
        tests/test_eq_bloom.cpp
        tests/test_where_concurrency.cpp
        tests/test_ci_microbench.cpp
        demo/sidecar_wal_lsn.cpp
        demo/demo_runner.cc
        demo/demo_diag.cc
        demo/demo_export.cc
        demo/demo_shell.cc
        demo/demo_commands.cc
        demo/import.cc
        demo/table_view.cc
        demo/bptree_index.cc
        demo/page_index_sidecar.cc
        demo/demo_cli.cpp
        demo/condition.cc
        demo/where.cc
        demo/equality_index_sidecar.cc
        demo/eq_bloom.cc
        demo/covering_index_sidecar.cc
        demo/visibility_checkpoint_sidecar.cc
        demo/schema_catalog.cc
        demo/utils.cc
        demo/logging.cc
        demo/txn_manager.cc
        src/c_api.cpp
    )
    target_include_directories(newdb_tests PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_SOURCE_DIR}/demo"
        "${NEWDB_REPO_ROOT}"
    )
    target_link_libraries(newdb_tests PRIVATE newdb_core GTest::gtest_main)
    newdb_force_include_waterfall_config(newdb_tests)

    include(GoogleTest)
    gtest_discover_tests(newdb_tests)
endif()
