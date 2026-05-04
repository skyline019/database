cmake_minimum_required(VERSION 3.16)
project(gtest_capi_standalone LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(GTEST_CAPI_BUILD_SHARED "Build gtest_capi shared library" ON)
option(GTEST_CAPI_BUILD_SAMPLES "Build sample test-bearing DLL" ON)
option(GTEST_CAPI_MSVC_STATIC_RUNTIME "Use /MT /MTd with MSVC" ON)

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
set(GTEST_CAPI_LOCAL_GTEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../newdb/build_mingw/_deps/googletest-src"
    CACHE PATH "Local googletest source directory (optional; avoids network fetch)")
if(EXISTS "${GTEST_CAPI_LOCAL_GTEST_DIR}/CMakeLists.txt")
  message(STATUS "Using local googletest source: ${GTEST_CAPI_LOCAL_GTEST_DIR}")
  if(NOT MSVC)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  endif()
  set(BUILD_SHARED_LIBS ON CACHE BOOL "" FORCE)
  add_subdirectory("${GTEST_CAPI_LOCAL_GTEST_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/_deps/googletest-build")
else()
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
  )
  if(NOT MSVC)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  endif()
  set(BUILD_SHARED_LIBS ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

if(GTEST_CAPI_BUILD_SHARED)
  add_library(gtest_capi SHARED
    src/gtest_capi.cpp
  )
  target_include_directories(gtest_capi PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(gtest_capi PRIVATE GTest::gtest GTest::gtest_main)
  if(WIN32)
    target_compile_definitions(gtest_capi PRIVATE GTEST_C_API_SHARED_BUILD GTEST_LINKED_AS_SHARED_LIBRARY=1)
  endif()
  set_target_properties(gtest_capi PROPERTIES OUTPUT_NAME "gtest_capi")
endif()

if(GTEST_CAPI_BUILD_SAMPLES)
  add_library(gtest_capi_samples SHARED
    src/gtest_capi.cpp
    examples/cpp/sample_tests.cpp
  )
  target_include_directories(gtest_capi_samples PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )
  target_link_libraries(gtest_capi_samples PRIVATE GTest::gtest GTest::gtest_main)
  if(WIN32)
    target_compile_definitions(gtest_capi_samples PRIVATE GTEST_C_API_SHARED_BUILD GTEST_LINKED_AS_SHARED_LIBRARY=1)
  endif()
  set_target_properties(gtest_capi_samples PROPERTIES OUTPUT_NAME "gtest_capi_samples")
endif()
