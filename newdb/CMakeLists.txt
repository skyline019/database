cmake_minimum_required(VERSION 3.16)
project(newdb LANGUAGES CXX)

if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# MSVC: optional fully static CRT (/MT). Requires CMP0091 for MSVC_RUNTIME_LIBRARY on targets.
option(NEWDB_MSVC_STATIC_RUNTIME "MSVC only: link C runtime statically (/MT, MultiThreaded)" OFF)
if(MSVC)
    if(POLICY CMP0091)
        cmake_policy(SET CMP0091 NEW)
    endif()
    if(NEWDB_MSVC_STATIC_RUNTIME)
        set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
    endif()
    # Ensure UTF-8 sources/comments are parsed consistently on Windows.
    add_compile_options(/utf-8)
endif()

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
    engine/src/schema/schema.cpp
    engine/src/schema/schema_io.cpp
    engine/src/heap/heap_table.cpp
    engine/src/heap/heap_file_read_view.cpp
    engine/src/cache/page_cache.cpp
    engine/src/cache/memory_budget.cpp
    engine/src/memory/memory_registry.cpp
    engine/src/codec/tuple_codec.cpp
    engine/src/heap/heap_page_waterfall.cpp
    engine/src/io/page/page_io.cpp
    engine/src/lsm/lsm_lite.cpp
    engine/src/session/api/session.cpp
    engine/src/catalog/catalog.cpp
    engine/src/wal/writer/wal_manager.cpp
    engine/src/wal/recovery/wal_manager_recover_support.cpp
    engine/src/wal/recovery/wal_redo_planner.cpp
    engine/src/wal/recovery/wal_redo_applier.cpp
    engine/src/wal/recovery/wal_segment_scanner.cpp
    engine/src/wal/codec/wal_codec.cpp
    engine/src/mvcc/snapshot/mvcc.cpp
    engine/src/session/table_access/session_history.cpp
    engine/src/error/error_format.cpp
    engine/src/util/crc32c_compat.cpp
    "${NEWDB_REPO_ROOT}/waterfall/storage/page.cc"
    "${NEWDB_REPO_ROOT}/waterfall/storage/record.cc"
    "${NEWDB_REPO_ROOT}/waterfall/storage/error.cc"
    "${NEWDB_REPO_ROOT}/waterfall/utils/arena.cc"
    "${NEWDB_REPO_ROOT}/waterfall/utils/error.cc"
    "${NEWDB_REPO_ROOT}/waterfall/utils/system_errors.cc"
)
set_target_properties(newdb_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(newdb_core PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/engine/include"
    "${NEWDB_REPO_ROOT}"
)

newdb_force_include_waterfall_config(newdb_core)

option(NEWDB_BUILD_SHARED "Build shared library (C ABI) for cross-language usage" ON)
if(NEWDB_BUILD_SHARED)
    add_library(newdb_shared SHARED
        engine/src/api/c/c_api.cpp
    )
    target_include_directories(newdb_shared PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/engine/include"
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

add_executable(newdb_smoke tools/smoke/newdb_smoke.cpp)
target_link_libraries(newdb_smoke PRIVATE newdb_core)
newdb_force_include_waterfall_config(newdb_smoke)

add_executable(newdb_perf tools/perf/newdb_perf.cpp)
newdb_force_include_waterfall_config(newdb_perf)

add_executable(newdb_concurrent_perf tools/perf/newdb_concurrent_perf.cpp)
target_link_libraries(newdb_concurrent_perf PRIVATE newdb_core)
newdb_force_include_waterfall_config(newdb_concurrent_perf)

add_executable(newdb_runtime_report tools/report/newdb_runtime_report.cpp)
newdb_force_include_waterfall_config(newdb_runtime_report)

set(NEWDB_DEMO_LIB_SOURCES
    cli/shell/bootstrap/demo_runner.cc
    cli/shell/diag/demo_diag.cc
    cli/shell/repl/demo_shell.cc
    cli/modules/import_export/demo_export.cc
    cli/shell/dispatch/router/dispatch.cc
    cli/shell/dispatch/router/dispatch_routing.cc
    cli/shell/dispatch/handlers/session/session_handler.cc
    cli/shell/dispatch/handlers/txn/txn_handler.cc
    cli/shell/dispatch/handlers/ddl/ddl_handler.cc
    cli/shell/dispatch/handlers/dml/dml_handler.cc
    cli/shell/dispatch/handlers/query/query_handler.cc
    cli/shell/dispatch/handlers/io/io_handler.cc
    cli/shell/dispatch/handlers/workspace/workspace_handler.cc
    cli/shell/dispatch/support/args/args_impl.cc
    cli/shell/dispatch/support/index/fast_index_impl.cc
    cli/shell/dispatch/services/sidecar/sidecar_invalidate_service.cc
    cli/shell/dispatch/services/lsm/lsm_lite_service.cc
    cli/shell/bootstrap/demo_cli.cpp
    cli/modules/common/view/table_view.cc
    cli/modules/where/executor/match/match_impl.cc
    cli/modules/where/executor/cache/cache_impl.cc
    cli/modules/where/executor/policy/policy_service.cc
    cli/modules/where/executor/plan/plan_impl.cc
    cli/modules/where/executor/stats/table_stats.cc
    cli/modules/sidecar/common/index_catalog.cc
    cli/modules/storage/table_storage_health.cc
    cli/modules/catalog/schema_catalog.cc
    cli/modules/where/parser/condition.cc
    cli/modules/common/util/utils.cc
    cli/modules/common/logging/logging.cc
    cli/modules/import_export/import.cc
    cli/modules/txn/coordinator/core/core_impl.cc
    cli/modules/txn/coordinator/lock/lock_service.cc
    cli/modules/txn/coordinator/wal/wal_service.cc
    cli/modules/txn/coordinator/recovery/recovery_service.cc
    cli/modules/txn/coordinator/write_conflict/write_conflict_service.cc
    cli/modules/txn/coordinator/vacuum/vacuum_service.cc
    cli/modules/txn/coordinator/stats/stats_impl.cc
    cli/modules/sidecar/common/bptree_index.cc
    cli/modules/sidecar/page/page_index_sidecar.cc
    cli/modules/sidecar/eq/equality_index_sidecar.cc
    cli/modules/sidecar/eq/eq_bloom.cc
    cli/modules/sidecar/covering/covering_index_sidecar.cc
    cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.cc
    cli/modules/sidecar/common/sidecar_wal_lsn.cpp
)
add_library(newdb_demo_lib STATIC ${NEWDB_DEMO_LIB_SOURCES})
set_target_properties(newdb_demo_lib PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(newdb_demo_lib PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}/engine/include"
    "${NEWDB_REPO_ROOT}"
)
target_link_libraries(newdb_demo_lib PUBLIC newdb_core)
newdb_force_include_waterfall_config(newdb_demo_lib)

if(NEWDB_BUILD_SHARED)
    target_link_libraries(newdb_shared PRIVATE newdb_demo_lib)
endif()

add_executable(newdb_demo cli/app/demo_main.cc)
target_link_libraries(newdb_demo PRIVATE newdb_demo_lib)
target_compile_definitions(newdb_demo PRIVATE "NEWDB_DISPLAY_PROJECT=\"newdb\"")
newdb_force_include_waterfall_config(newdb_demo)

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
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/engine/include"
        "${NEWDB_REPO_ROOT}"
    )
    target_link_libraries(newdb_gui PRIVATE newdb_demo_lib ${NEWDB_GUI_QT})
    target_compile_definitions(newdb_gui PRIVATE "NEWDB_DISPLAY_PROJECT=\"newdb\"")
    newdb_force_include_waterfall_config(newdb_gui)
endif()

option(NEWDB_BUILD_TESTS "Build GoogleTest suite (newdb_tests)" ON)
option(NEWDB_GTEST_BUILD_SHARED_DLL "Build GoogleTest as shared DLLs for cross-language reuse" ON)

# /MT + gtest DLL mixes CRTs; default gtest to static CRT when using static MSVC runtime.
if(MSVC AND NEWDB_MSVC_STATIC_RUNTIME AND NEWDB_BUILD_TESTS AND NEWDB_GTEST_BUILD_SHARED_DLL)
    message(STATUS "NEWDB_MSVC_STATIC_RUNTIME: forcing NEWDB_GTEST_BUILD_SHARED_DLL=OFF (avoid /MT + MD gtest mix).")
    set(NEWDB_GTEST_BUILD_SHARED_DLL OFF CACHE BOOL "" FORCE)
endif()
# MinGW: -static-libgcc on exes/DLLs + libgtest.dll causes duplicate _Unwind_Resume (libgcc_eh vs gtest DLL).
if(MINGW AND NEWDB_BUILD_TESTS AND NEWDB_GTEST_BUILD_SHARED_DLL)
    message(STATUS "MinGW: forcing NEWDB_GTEST_BUILD_SHARED_DLL=OFF (avoid _Unwind_Resume clash with -static-libgcc).")
    set(NEWDB_GTEST_BUILD_SHARED_DLL OFF CACHE BOOL "" FORCE)
endif()

if(NEWDB_BUILD_TESTS)
    include(CTest)
    include(FetchContent)
    # Prefer a local tree when provided (-Dgoogletest_SOURCE_DIR=... or
    # -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=...) so offline / blocked-GitHub builds work.
    set(_newdb_googletest_src "")
    if(DEFINED FETCHCONTENT_SOURCE_DIR_GOOGLETEST AND FETCHCONTENT_SOURCE_DIR_GOOGLETEST
            AND EXISTS "${FETCHCONTENT_SOURCE_DIR_GOOGLETEST}/CMakeLists.txt")
        set(_newdb_googletest_src "${FETCHCONTENT_SOURCE_DIR_GOOGLETEST}")
    elseif(googletest_SOURCE_DIR AND EXISTS "${googletest_SOURCE_DIR}/CMakeLists.txt")
        set(_newdb_googletest_src "${googletest_SOURCE_DIR}")
    endif()
    if(_newdb_googletest_src)
        message(STATUS "googletest: using local SOURCE_DIR (${_newdb_googletest_src})")
        set(FETCHCONTENT_UPDATES_DISCONNECTED_GOOGLETEST ON CACHE BOOL "" FORCE)
        FetchContent_Declare(googletest SOURCE_DIR "${_newdb_googletest_src}")
    else()
        # Tarball avoids slow/blocked git clones in some environments.
        FetchContent_Declare(
            googletest
            URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
        )
    endif()
    if(MSVC AND NEWDB_MSVC_STATIC_RUNTIME)
        set(gtest_force_shared_crt OFF CACHE BOOL "" FORCE)
    else()
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    endif()
    if(NEWDB_GTEST_BUILD_SHARED_DLL)
        # Build gtest/gtest_main as DLLs so other language runtimes can reuse them.
        set(BUILD_SHARED_LIBS ON CACHE BOOL "" FORCE)
    else()
        # Stale ON from a prior configure would keep gtest as DLLs (e.g. after MinGW forces shared OFF).
        set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    endif()
    FetchContent_MakeAvailable(googletest)

    # googletest lives in a subdirectory and may not inherit CMAKE_MSVC_RUNTIME_LIBRARY from the parent.
    if(MSVC AND NEWDB_MSVC_STATIC_RUNTIME)
        set(_newdb_msvc_mt_rt MultiThreaded$<$<CONFIG:Debug>:Debug>)
        foreach(_gt IN ITEMS gtest gtest_main gmock gmock_main)
            if(TARGET ${_gt})
                set_property(TARGET ${_gt} PROPERTY MSVC_RUNTIME_LIBRARY "${_newdb_msvc_mt_rt}")
            endif()
        endforeach()
    endif()

    add_library(gtest_capi SHARED
        tests/gtest_c_api.cpp
    )
    target_include_directories(gtest_capi PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
    )
    # Link both gtest and gtest_main so flag symbols are consistently resolved
    # when gtest is built as shared libraries on MSVC.
    target_link_libraries(gtest_capi PRIVATE GTest::gtest GTest::gtest_main)
    if(NEWDB_GTEST_BUILD_SHARED_DLL)
        target_compile_definitions(gtest_capi PRIVATE GTEST_LINKED_AS_SHARED_LIBRARY=1)
    endif()
    if(WIN32)
        target_compile_definitions(gtest_capi PRIVATE GTEST_C_API_SHARED_BUILD)
    endif()
    set_target_properties(gtest_capi PROPERTIES OUTPUT_NAME "gtest_capi")

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
        tests/test_lsm_lite_engine.cpp
        tests/test_demo_logging_history.cpp
        tests/test_demo_txn_wal.cpp
        tests/test_demo_where.cpp
        tests/test_demo_schema_catalog.cpp
        tests/test_demo_cli.cpp
        tests/test_demo_mdb_stop.cpp
        tests/test_demo_utils.cpp
        tests/test_demo_page_keyset.cpp
        tests/test_dispatch_routing.cpp
        tests/test_demo_lsm_lite.cpp
        tests/test_c_api.cpp
        tests/test_error_format.cpp
        tests/test_wal_codec.cpp
        tests/test_wal_truncate.cpp
        tests/test_wal_segment.cpp
        tests/test_wal_concurrency.cpp
        tests/test_wal_recovery_indexed.cpp
        tests/test_txn_file_lock.cpp
        tests/test_txn_isolation_config.cpp
        tests/test_txn_isolation_visibility.cpp
        tests/test_txn_shell_multi_entry_snapshot.cpp
        tests/test_show_plan_table_stats_stale_shell.cpp
        tests/test_where_heap_scan_budget_binding.cpp
        tests/test_where_query_temp_cap.cpp
        tests/test_txn_autovacuum.cpp
        tests/test_txn_write_conflict.cpp
        tests/test_sidecar_wal_lsn_stress.cpp
        tests/test_visibility_checkpoint_sidecar.cpp
        tests/test_covering_index_sidecar.cpp
        tests/test_covering_projection_sidecar.cpp
        tests/test_eq_sidecar_bucket.cpp
        tests/test_eq_sidecar_admit.cpp
        tests/test_eq_bloom.cpp
        tests/test_where_concurrency.cpp
        tests/test_query_table_stats.cpp
        tests/test_index_catalog.cpp
        tests/test_wal_segment_scanner.cpp
        tests/test_table_storage_health.cpp
        tests/test_page_cache.cpp
        tests/test_memory_registry.cpp
        tests/test_storage_soak.cpp
        tests/test_ci_microbench.cpp
        cli/modules/sidecar/common/sidecar_wal_lsn.cpp
        cli/shell/bootstrap/demo_runner.cc
        cli/shell/diag/demo_diag.cc
        cli/modules/import_export/demo_export.cc
        cli/shell/repl/demo_shell.cc
        cli/shell/dispatch/router/dispatch.cc
        cli/shell/dispatch/router/dispatch_routing.cc
        cli/shell/dispatch/handlers/session/session_handler.cc
        cli/shell/dispatch/handlers/txn/txn_handler.cc
        cli/shell/dispatch/handlers/ddl/ddl_handler.cc
        cli/shell/dispatch/handlers/dml/dml_handler.cc
        cli/shell/dispatch/handlers/query/query_handler.cc
        cli/shell/dispatch/handlers/io/io_handler.cc
        cli/shell/dispatch/handlers/workspace/workspace_handler.cc
        cli/shell/dispatch/support/args/args_impl.cc
        cli/shell/dispatch/support/index/fast_index_impl.cc
        cli/shell/dispatch/services/sidecar/sidecar_invalidate_service.cc
        cli/shell/dispatch/services/lsm/lsm_lite_service.cc
        cli/modules/import_export/import.cc
        cli/modules/common/view/table_view.cc
        cli/modules/sidecar/common/bptree_index.cc
        cli/modules/sidecar/page/page_index_sidecar.cc
        cli/shell/bootstrap/demo_cli.cpp
        cli/modules/where/parser/condition.cc
        cli/modules/where/executor/match/match_impl.cc
        cli/modules/where/executor/cache/cache_impl.cc
        cli/modules/where/executor/policy/policy_service.cc
        cli/modules/where/executor/plan/plan_impl.cc
        cli/modules/where/executor/stats/table_stats.cc
        cli/modules/sidecar/common/index_catalog.cc
        cli/modules/storage/table_storage_health.cc
        cli/modules/sidecar/eq/equality_index_sidecar.cc
        cli/modules/sidecar/eq/eq_bloom.cc
        cli/modules/sidecar/covering/covering_index_sidecar.cc
        cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.cc
        cli/modules/catalog/schema_catalog.cc
        cli/modules/common/util/utils.cc
        cli/modules/common/logging/logging.cc
        cli/modules/txn/coordinator/core/core_impl.cc
        cli/modules/txn/coordinator/lock/lock_service.cc
        cli/modules/txn/coordinator/wal/wal_service.cc
        cli/modules/txn/coordinator/recovery/recovery_service.cc
        cli/modules/txn/coordinator/write_conflict/write_conflict_service.cc
        cli/modules/txn/coordinator/vacuum/vacuum_service.cc
        cli/modules/txn/coordinator/stats/stats_impl.cc
        engine/src/api/c/c_api.cpp
    )
    target_include_directories(newdb_tests PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}"
        "${CMAKE_CURRENT_SOURCE_DIR}/engine/include"
        "${NEWDB_REPO_ROOT}"
    )
    target_link_libraries(newdb_tests PRIVATE newdb_core GTest::gtest_main)
    # MSVC: multiple translation units in one target can contend on vc*.pdb without /FS.
    if(MSVC)
        target_compile_options(newdb_tests PRIVATE /FS)
    endif()
    newdb_force_include_waterfall_config(newdb_tests)
    # copy_if_different with $<TARGET_RUNTIME_DLLS:...> is invalid when the exe has no runtime DLLs
    # (e.g. MSVC /MT + static gtest); only used when gtest is built as DLLs.
    if(WIN32 AND NEWDB_GTEST_BUILD_SHARED_DLL)
        add_custom_command(TARGET newdb_tests POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_RUNTIME_DLLS:newdb_tests>
                $<TARGET_FILE_DIR:newdb_tests>
            COMMAND_EXPAND_LISTS
        )
    endif()

    include(GoogleTest)
    # PRE_TEST avoids build-time execution (which can fail on Windows when
    # dependent test DLLs are not yet on PATH during discovery step).
    # Default label `newdb` for filtering; targeted subsets use `ctest -R` (see TESTS_FAULT_INJECTION_AND_TXN_MATRIX.md).
    gtest_discover_tests(newdb_tests
        DISCOVERY_MODE PRE_TEST
        PROPERTIES LABELS "newdb"
    )
endif()

# ---- Shared bundle (self-contained runtime) ----
set(NEWDB_SHARED_BUNDLE_EXECUTABLE_TARGETS
    newdb_demo
    newdb_smoke
    newdb_perf
    newdb_concurrent_perf
    newdb_runtime_report
)
set(NEWDB_SHARED_BUNDLE_LIBRARY_TARGETS)
if(NEWDB_BUILD_SHARED)
    list(APPEND NEWDB_SHARED_BUNDLE_LIBRARY_TARGETS newdb_shared)
endif()
if(NEWDB_BUILD_GUI)
    list(APPEND NEWDB_SHARED_BUNDLE_EXECUTABLE_TARGETS newdb_gui)
endif()
if(NEWDB_BUILD_TESTS)
    list(APPEND NEWDB_SHARED_BUNDLE_EXECUTABLE_TARGETS newdb_tests)
    list(APPEND NEWDB_SHARED_BUNDLE_LIBRARY_TARGETS gtest_capi)
    if(TARGET gtest)
        list(APPEND NEWDB_SHARED_BUNDLE_LIBRARY_TARGETS gtest)
    endif()
    if(TARGET gtest_main)
        list(APPEND NEWDB_SHARED_BUNDLE_LIBRARY_TARGETS gtest_main)
    endif()
endif()
set(NEWDB_SHARED_BUNDLE_TARGETS
    ${NEWDB_SHARED_BUNDLE_EXECUTABLE_TARGETS}
    ${NEWDB_SHARED_BUNDLE_LIBRARY_TARGETS}
)

# MinGW: statically link libgcc/libstdc++ and pin winpthread for all shipped binaries (full tree).
if(MINGW)
    set(NEWDB_MINGW_STATIC_RT_FLAGS
        -static-libgcc
        -static-libstdc++
        "-Wl,-Bstatic,-l:libwinpthread.a,-Bdynamic"
    )
    set(_newdb_mingw_link_targets
        newdb_demo
        newdb_smoke
        newdb_perf
        newdb_concurrent_perf
        newdb_runtime_report
    )
    if(NEWDB_BUILD_SHARED)
        list(APPEND _newdb_mingw_link_targets newdb_shared)
    endif()
    if(NEWDB_BUILD_TESTS)
        list(APPEND _newdb_mingw_link_targets newdb_tests gtest_capi)
        # When gtest is built as DLLs, link them with the same static libgcc/libstdc++/winpthread as our exes.
        if(NEWDB_GTEST_BUILD_SHARED_DLL)
            foreach(_gt IN ITEMS gtest gtest_main gmock gmock_main)
                if(TARGET ${_gt})
                    get_target_property(_gt_type ${_gt} TYPE)
                    if(_gt_type STREQUAL "SHARED_LIBRARY")
                        list(APPEND _newdb_mingw_link_targets ${_gt})
                    endif()
                endif()
            endforeach()
        endif()
    endif()
    if(NEWDB_BUILD_GUI)
        list(APPEND _newdb_mingw_link_targets newdb_gui)
    endif()
    foreach(_mt IN LISTS _newdb_mingw_link_targets)
        if(TARGET ${_mt})
            target_link_options(${_mt} PRIVATE ${NEWDB_MINGW_STATIC_RT_FLAGS})
        endif()
    endforeach()
endif()

if(UNIX AND NOT APPLE)
    foreach(_rpath_target IN LISTS NEWDB_SHARED_BUNDLE_TARGETS)
        set_target_properties(${_rpath_target} PROPERTIES
            BUILD_RPATH "\$ORIGIN"
            INSTALL_RPATH "\$ORIGIN"
        )
    endforeach()
endif()

set(NEWDB_SHARED_BUNDLE_OUTPUT_DIR "${CMAKE_BINARY_DIR}/shared_bundle/$<CONFIG>")
set(_newdb_bundle_script_content "set(_bundle_dir \"${NEWDB_SHARED_BUNDLE_OUTPUT_DIR}\")\n")
string(APPEND _newdb_bundle_script_content "file(MAKE_DIRECTORY \"\${_bundle_dir}\")\n")
foreach(_bundle_target IN LISTS NEWDB_SHARED_BUNDLE_TARGETS)
    string(APPEND _newdb_bundle_script_content
        "set(_bin \"$<TARGET_FILE:${_bundle_target}>\")\n"
        "if(EXISTS \"\${_bin}\")\n"
        "  file(COPY \"\${_bin}\" DESTINATION \"\${_bundle_dir}\")\n"
        "endif()\n"
    )
    # TARGET_RUNTIME_DLLS is only valid for shared/module targets (not static gtest with /MT).
    if(WIN32 AND TARGET ${_bundle_target})
        get_target_property(_newdb_bundle_ttype ${_bundle_target} TYPE)
        if(_newdb_bundle_ttype STREQUAL "SHARED_LIBRARY" OR _newdb_bundle_ttype STREQUAL "MODULE_LIBRARY")
            string(APPEND _newdb_bundle_script_content
                "set(_dlls \"$<TARGET_RUNTIME_DLLS:${_bundle_target}>\")\n"
                "if(_dlls)\n"
                "  foreach(_dll IN LISTS _dlls)\n"
                "    if(EXISTS \"\${_dll}\")\n"
                "      file(COPY \"\${_dll}\" DESTINATION \"\${_bundle_dir}\")\n"
                "    endif()\n"
                "  endforeach()\n"
                "endif()\n"
            )
        endif()
    endif()
endforeach()

foreach(_exe_target IN LISTS NEWDB_SHARED_BUNDLE_EXECUTABLE_TARGETS)
    if(WIN32)
        string(APPEND _newdb_bundle_script_content
            "set(_launcher \"\${_bundle_dir}/run_$<TARGET_FILE_BASE_NAME:${_exe_target}>.cmd\")\n"
            "file(WRITE \"\${_launcher}\" \"@echo off\\nsetlocal\\nset \\\"PATH=%~dp0\\\"\\n\\\"%~dp0$<TARGET_FILE_NAME:${_exe_target}>\\\" %*\\n\")\n"
        )
    endif()
endforeach()

if(MINGW)
    get_filename_component(_newdb_mingw_bin_dir "${CMAKE_CXX_COMPILER}" DIRECTORY)
    string(APPEND _newdb_bundle_script_content
        "set(_mingw_bin \"${_newdb_mingw_bin_dir}\")\n"
        "if(EXISTS \"\${_mingw_bin}\")\n"
        "  set(_mingw_runtime_patterns\n"
        "    \"libstdc++-6.dll\"\n"
        "    \"libwinpthread-1.dll\"\n"
        "    \"libssp-0.dll\"\n"
        "    \"libgcc_s_*.dll\"\n"
        "  )\n"
        "  foreach(_pat IN LISTS _mingw_runtime_patterns)\n"
        "    file(GLOB _matches \"\${_mingw_bin}/\${_pat}\")\n"
        "    foreach(_dll IN LISTS _matches)\n"
        "      if(EXISTS \"\${_dll}\")\n"
        "        file(COPY \"\${_dll}\" DESTINATION \"\${_bundle_dir}\")\n"
        "      endif()\n"
        "    endforeach()\n"
        "  endforeach()\n"
        "endif()\n"
    )
endif()

file(GENERATE
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/cmake/newdb_shared_bundle_$<CONFIG>.cmake"
    CONTENT "${_newdb_bundle_script_content}"
)

add_custom_target(shared_bundle
    COMMAND ${CMAKE_COMMAND} -P "${CMAKE_CURRENT_BINARY_DIR}/cmake/newdb_shared_bundle_$<CONFIG>.cmake"
    COMMENT "Bundling self-contained shared bundle artifacts"
)
add_dependencies(shared_bundle ${NEWDB_SHARED_BUNDLE_TARGETS})

# Backward compatibility alias.
add_custom_target(newdb_bundle DEPENDS shared_bundle)


