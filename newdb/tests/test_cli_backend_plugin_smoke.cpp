#include <gtest/gtest.h>

#include <cstdlib>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <dlfcn.h>
#endif

TEST(CliBackendPluginSmoke, DllExportsSessionCreate) {
    const char* path = std::getenv("NEWDB_CLI_BACKEND_PATH");
    if (path == nullptr || path[0] == '\0') {
        GTEST_SKIP() << "Set NEWDB_CLI_BACKEND_PATH to the built newdb_cli_backend shared library.";
    }
#if defined(_WIN32)
    HMODULE h = LoadLibraryA(path);
    ASSERT_NE(h, nullptr);
    auto* sym = reinterpret_cast<void* (*)(const char*, const char*, const char*)>(
        GetProcAddress(h, "newdb_cli_backend_session_create"));
    ASSERT_NE(sym, nullptr);
    FreeLibrary(h);
#else
    void* h = dlopen(path, RTLD_NOW);
    ASSERT_NE(h, nullptr);
    auto sym = reinterpret_cast<void* (*)(const char*, const char*, const char*)>(
        dlsym(h, "newdb_cli_backend_session_create"));
    ASSERT_NE(sym, nullptr);
    dlclose(h);
#endif
}
