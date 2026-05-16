cmake_minimum_required(VERSION 3.16)
project(gtest_capi_standalone LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(GTEST_CAPI_BUILD_SHARED "Build gtest_capi shared library" ON)
option(GTEST_CAPI_BUILD_SAMPLES "Build sample test-bearing DLL" ON)
option(GTEST_CAPI_MSVC_STATIC_RUNTIME "Use /MT /MTd with MSVC" ON)
option(GTEST_CAPI_INSTALL "Generate install() rules for gtest_capi (standalone/SDK)" ON)

if(MSVC)
  if(GTEST_CAPI_MSVC_STATIC_RUNTIME)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
    set(gtest_force_shared_crt OFF CACHE BOOL "" FORCE)
  else()
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  endif()
endif()

include(FetchContent)
# Vendored googletest under this tree (no network). Preferred layout: third_party/googletest/
set(GTEST_CAPI_LOCAL_GTEST_DIR "" CACHE PATH
    "Override path to googletest root (must contain top-level CMakeLists.txt). Empty = auto-detect.")
set(_gtest_capi_gtest_src "")
if(GTEST_CAPI_LOCAL_GTEST_DIR AND EXISTS "${GTEST_CAPI_LOCAL_GTEST_DIR}/CMakeLists.txt")
  set(_gtest_capi_gtest_src "${GTEST_CAPI_LOCAL_GTEST_DIR}")
else()
  foreach(_gtest_cand IN ITEMS
      "${CMAKE_CURRENT_SOURCE_DIR}/third_party/googletest"
      "${CMAKE_CURRENT_SOURCE_DIR}/googletest"
      "${CMAKE_CURRENT_SOURCE_DIR}/_deps/googletest-src")
    if(EXISTS "${_gtest_cand}/CMakeLists.txt")
      set(_gtest_capi_gtest_src "${_gtest_cand}")
      break()
    endif()
  endforeach()
endif()

# MSVC + static CRT (/MT): shared libgtest leaks heap boundaries with the embedding EXE; build
# GoogleTest as static in that configuration. Otherwise keep shared gtest for gtest_capi DLL reuse.
if(MSVC AND GTEST_CAPI_MSVC_STATIC_RUNTIME)
  set(_gtest_capi_googletest_shared OFF)
else()
  set(_gtest_capi_googletest_shared ON)
endif()

if(_gtest_capi_gtest_src)
  message(STATUS "gtest_capi: using local googletest (${_gtest_capi_gtest_src})")
  if(NOT MSVC)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  endif()
  set(_gtest_capi_saved_build_shared "${BUILD_SHARED_LIBS}")
  if(_gtest_capi_googletest_shared)
    set(BUILD_SHARED_LIBS ON)
  else()
    set(BUILD_SHARED_LIBS OFF)
  endif()
  add_subdirectory("${_gtest_capi_gtest_src}" "${CMAKE_CURRENT_BINARY_DIR}/_deps/googletest-build")
  set(BUILD_SHARED_LIBS "${_gtest_capi_saved_build_shared}")
else()
  message(STATUS "gtest_capi: local googletest not found; fetching v1.14.0 tarball")
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
  )
  if(NOT MSVC)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  endif()
  set(_gtest_capi_saved_build_shared "${BUILD_SHARED_LIBS}")
  if(_gtest_capi_googletest_shared)
    set(BUILD_SHARED_LIBS ON)
  else()
    set(BUILD_SHARED_LIBS OFF)
  endif()
  FetchContent_MakeAvailable(googletest)
  set(BUILD_SHARED_LIBS "${_gtest_capi_saved_build_shared}")
endif()

if(GTEST_CAPI_BUILD_SHARED)
  add_library(gtest_capi SHARED
    src/gtest_capi.cpp
  )
  set_target_properties(gtest_capi PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_include_directories(gtest_capi PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(gtest_capi PRIVATE GTest::gtest GTest::gtest_main)
  target_compile_definitions(gtest_capi PRIVATE
    GTEST_C_API_SHARED_BUILD
    GTEST_LINKED_AS_SHARED_LIBRARY=1
  )
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(gtest_capi PRIVATE
      -fvisibility=hidden
      -fvisibility-inlines-hidden
    )
  endif()
  set_target_properties(gtest_capi PROPERTIES OUTPUT_NAME "gtest_capi")
else()
  add_library(gtest_capi STATIC
    src/gtest_capi.cpp
  )
  target_include_directories(gtest_capi PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(gtest_capi PUBLIC GTest::gtest GTest::gtest_main)
endif()

if(GTEST_CAPI_BUILD_SAMPLES)
  add_library(gtest_capi_samples SHARED
    src/gtest_capi.cpp
    examples/cpp/sample_tests.cpp
  )
  set_target_properties(gtest_capi_samples PROPERTIES POSITION_INDEPENDENT_CODE ON)
  target_include_directories(gtest_capi_samples PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(gtest_capi_samples PRIVATE GTest::gtest GTest::gtest_main)
  target_compile_definitions(gtest_capi_samples PRIVATE
    GTEST_C_API_SHARED_BUILD
    GTEST_LINKED_AS_SHARED_LIBRARY=1
  )
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(gtest_capi_samples PRIVATE
      -fvisibility=hidden
      -fvisibility-inlines-hidden
    )
  endif()
  set_target_properties(gtest_capi_samples PROPERTIES OUTPUT_NAME "gtest_capi_samples")
endif()

if(GTEST_CAPI_INSTALL AND TARGET gtest_capi)
  include(GNUInstallDirs OPTIONAL)
  if(NOT DEFINED CMAKE_INSTALL_LIBDIR)
    set(CMAKE_INSTALL_LIBDIR "lib")
  endif()
  if(NOT DEFINED CMAKE_INSTALL_BINDIR)
    set(CMAKE_INSTALL_BINDIR "bin")
  endif()
  if(NOT DEFINED CMAKE_INSTALL_INCLUDEDIR)
    set(CMAKE_INSTALL_INCLUDEDIR "include")
  endif()
  install(TARGETS gtest_capi
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
  install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/include/gtest_capi.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
endif()
