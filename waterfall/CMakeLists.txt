cmake_minimum_required(VERSION 3.16)
project(waterfall LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(WATERFALL_CONFIG "${CMAKE_CURRENT_SOURCE_DIR}/config.h")

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(WATERFALL_CRC32C libcrc32c)
endif()

set(WATERFALL_CRC32C_COMPAT_SOURCES "")
if(NOT WATERFALL_CRC32C_FOUND)
    list(APPEND WATERFALL_CRC32C_COMPAT_SOURCES utils/crc32c_compat.cc)
endif()

add_library(waterfall STATIC
    storage/page.cc
    storage/record.cc
    storage/error.cc
    utils/arena.cc
    utils/error.cc
    utils/system_errors.cc
    ${WATERFALL_CRC32C_COMPAT_SOURCES}
)
# Required when linking this archive into shared objects (libnewdb.so, libnewdb_cli_backend.so, etc.).
set_target_properties(waterfall PROPERTIES POSITION_INDEPENDENT_CODE ON)

# PUBLIC `${CMAKE_CURRENT_SOURCE_DIR}/include`: all `<waterfall/...>` includes resolve here (thin shims + headers).
# PRIVATE source tree: `"error.h"` / `"tuple.h"` in `storage/*.cc` resolve next to those TUs.
# The repo root is no longer a PUBLIC include path for `waterfall` consumers.
target_include_directories(waterfall PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
target_include_directories(waterfall PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
)

if(WATERFALL_CRC32C_FOUND)
    target_include_directories(waterfall PRIVATE ${WATERFALL_CRC32C_INCLUDE_DIRS})
    target_link_libraries(waterfall PUBLIC ${WATERFALL_CRC32C_LIBRARIES})
endif()

function(waterfall_force_include_config target)
    if(MSVC)
        target_compile_options(${target} PRIVATE "/FI${WATERFALL_CONFIG}")
    else()
        target_compile_options(${target} PRIVATE "-include${WATERFALL_CONFIG}")
    endif()
endfunction()

waterfall_force_include_config(waterfall)
