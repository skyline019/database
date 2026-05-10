Place the GoogleTest source tree here as:

  third_party/googletest/CMakeLists.txt
  third_party/googletest/googletest/
  third_party/googletest/googlemock/

Extract release v1.14.0 (or match newdb/FetchContent URL) from:
  https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz

Then configure gtest_capi or newdb tests without network FetchContent.

Cross-platform shared library output names (OUTPUT_NAME gtest_capi):
  Linux/macOS: libgtest_capi.so / libgtest_capi.dylib
  Windows MSVC: gtest_capi.dll
  Windows MinGW: libgtest_capi.dll

Host languages load this DLL/.so and call the C API in include/gtest_capi.h.
When compiling the host against the DLL on Windows, define GTEST_C_API_SHARED_USE.
