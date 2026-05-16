# Adapter CMake for Base/waterfall (historically Makefile-built).
# Include root is ${CMAKE_CURRENT_SOURCE_DIR} so <waterfall/...> resolves.

add_library(wf_waterfall STATIC
  waterfall/utils/error.cc
  waterfall/utils/arena.cc
  waterfall/utils/system_errors.cc
  waterfall/schema/scn.cc
  waterfall/storage/error.cc
  waterfall/storage/record.cc
  waterfall/storage/page.cc
  waterfall/storage/segment.cc
)

target_include_directories(wf_waterfall PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}"
)

target_compile_definitions(wf_waterfall PUBLIC
  $<$<BOOL:${MSVC}>:NOMINMAX>
  $<$<BOOL:${MSVC}>:WIN32_LEAN_AND_MEAN>
)

target_link_libraries(wf_waterfall PUBLIC
  crc32c
  roaring
)

if(MSVC)
  target_compile_options(wf_waterfall PRIVATE /W3 /utf-8)
else()
  target_compile_options(wf_waterfall PRIVATE -Wall -Wextra -Wno-unused-parameter)
endif()
