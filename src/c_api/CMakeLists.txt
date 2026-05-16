add_library(structdb_capi STATIC
  src/structdb_capi.cpp
)

target_include_directories(structdb_capi PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

target_link_libraries(structdb_capi PUBLIC
  structdb_client_mdb
  structdb_client_embed
  structdb_facade
  structdb_infra
)

if(MSVC)
  target_compile_options(structdb_capi PRIVATE /W4 /permissive-)
else()
  target_compile_options(structdb_capi PRIVATE -Wall -Wextra)
endif()

if(STRUCTDB_BUILD_CAPI_SHARED AND NOT MSVC)
  set_target_properties(structdb_capi PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

if(STRUCTDB_BUILD_CAPI_SHARED)
  add_library(structdb_capi_shared SHARED
    src/structdb_capi.cpp
  )

  target_include_directories(structdb_capi_shared PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
  )

  target_link_libraries(structdb_capi_shared PUBLIC
    structdb_client_mdb
    structdb_client_embed
    structdb_facade
    structdb_infra
  )

  target_compile_definitions(structdb_capi_shared PRIVATE STRUCTDB_CAPI_BUILDING=1)
  target_compile_definitions(structdb_capi_shared INTERFACE STRUCTDB_CAPI_SHARED=1)

  set_target_properties(structdb_capi_shared PROPERTIES
    OUTPUT_NAME structdb_capi_shared
    WINDOWS_EXPORT_ALL_SYMBOLS OFF
  )

  if(MSVC)
    target_compile_options(structdb_capi_shared PRIVATE /W4 /permissive-)
  else()
    target_compile_options(structdb_capi_shared PRIVATE -Wall -Wextra -fvisibility=hidden)
    set_target_properties(structdb_capi_shared PROPERTIES POSITION_INDEPENDENT_CODE ON)
  endif()

  install(TARGETS structdb_capi_shared
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib)
endif()

install(TARGETS structdb_capi
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib)
install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/include/structdb_capi.h"
  DESTINATION include)
