set(STRUCTDB_TEST_RUNS_PARENT "${CMAKE_BINARY_DIR}/test_runs")
configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/test_artifact_config.hpp.in"
  "${CMAKE_CURRENT_BINARY_DIR}/test_artifact_config.hpp"
  @ONLY
)

add_executable(structdb_tests
  structdb_tests.cpp
  mdb_tests.cpp
  capi_test.cpp
  test_artifact_env.cpp
)

target_include_directories(structdb_tests PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")

target_link_libraries(structdb_tests PRIVATE
  structdb_client_embed
  structdb_client_mdb
  structdb_capi
  gtest_capi
)

# gtest_capi is static when MSVC+/MT (see root CMake); do not set GTEST_C_API_SHARED_USE then.
get_target_property(_structdb_gtest_capi_type gtest_capi TYPE)
if(_structdb_gtest_capi_type STREQUAL "SHARED_LIBRARY")
  target_compile_definitions(structdb_tests PRIVATE GTEST_C_API_SHARED_USE)
  target_link_libraries(structdb_tests PRIVATE GTest::gtest)
endif()

if(WIN32)
  if(_structdb_gtest_capi_type STREQUAL "SHARED_LIBRARY")
    add_custom_command(TARGET structdb_tests POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:gtest>"
        "$<TARGET_FILE_DIR:structdb_tests>/"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:gtest_main>"
        "$<TARGET_FILE_DIR:structdb_tests>/"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:gtest_capi>"
        "$<TARGET_FILE_DIR:structdb_tests>/"
      VERBATIM)
  endif()
endif()

add_test(NAME structdb_tests COMMAND structdb_tests)

add_test(NAME mdb_tests COMMAND structdb_tests --gtest_filter=Mdb.*)
set_tests_properties(mdb_tests PROPERTIES LABELS mdb)

if(STRUCTDB_BUILD_CAPI_SHARED)
  add_executable(structdb_capi_shared_smoke
    capi_shared_smoke.cpp
    test_artifact_env.cpp
  )
  target_include_directories(structdb_capi_shared_smoke PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
  target_link_libraries(structdb_capi_shared_smoke PRIVATE structdb_capi_shared gtest_capi)
  add_test(NAME structdb_capi_shared_smoke COMMAND structdb_capi_shared_smoke)
  if(WIN32)
    add_custom_command(TARGET structdb_capi_shared_smoke POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "$<TARGET_FILE:structdb_capi_shared>"
        "$<TARGET_FILE_DIR:structdb_capi_shared_smoke>/"
      VERBATIM)
  endif()
endif()
