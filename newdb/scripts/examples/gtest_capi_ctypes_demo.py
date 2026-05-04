#!/usr/bin/env python3
import ctypes
import os
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_dll() -> Path:
    bundle = _repo_root() / "build_mingw" / "gtest_capi_bundle" / "libgtest_capi.dll"
    if bundle.exists():
        return bundle
    return _repo_root() / "build_mingw" / "libgtest_capi.dll"


def _load_windows_bundle(dll_path: Path) -> ctypes.CDLL:
    # Force loading from our own bundle first, avoiding PATH priority surprises.
    load_with_altered_search_path = 0x00000008
    dep_names = [
        "libwinpthread-1.dll",
        "libgcc_s_seh-1.dll",
        "libstdc++-6.dll",
        "libgtest.dll",
    ]
    for dep in dep_names:
        dep_path = dll_path.parent / dep
        if dep_path.exists():
            ctypes.WinDLL(str(dep_path), winmode=load_with_altered_search_path)
    return ctypes.CDLL(str(dll_path), winmode=load_with_altered_search_path)


def main() -> int:
    dll_path = Path(os.environ.get("GTEST_CAPI_DLL", str(_default_dll())))
    if not dll_path.exists():
        print(f"[ERR] DLL not found: {dll_path}")
        print("Set GTEST_CAPI_DLL or build target: cmake --build newdb/build_mingw --target gtest_capi")
        return 2

    if os.name == "nt":
        dll = _load_windows_bundle(dll_path)
    else:
        dll = ctypes.CDLL(str(dll_path))

    dll.gtest_capi_set_filter.argtypes = [ctypes.c_char_p]
    dll.gtest_capi_set_filter.restype = ctypes.c_int
    dll.gtest_capi_run_all.argtypes = []
    dll.gtest_capi_run_all.restype = ctypes.c_int
    dll.gtest_capi_total_test_count.argtypes = []
    dll.gtest_capi_total_test_count.restype = ctypes.c_int
    dll.gtest_capi_test_to_run_count.argtypes = []
    dll.gtest_capi_test_to_run_count.restype = ctypes.c_int
    dll.gtest_capi_failed_test_count.argtypes = []
    dll.gtest_capi_failed_test_count.restype = ctypes.c_int

    @ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_void_p)
    def on_line(line, _user):
        print("  ", line.decode("utf-8", errors="replace"))

    dll.gtest_capi_list_tests.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    dll.gtest_capi_list_tests.restype = ctypes.c_int

    print(f"[INFO] Using: {dll_path}")
    dll.gtest_capi_set_filter(b"*")

    listed = dll.gtest_capi_list_tests(on_line, None)
    print(f"[INFO] listed={listed}")
    print(f"[INFO] total={dll.gtest_capi_total_test_count()} to_run={dll.gtest_capi_test_to_run_count()}")

    rc = dll.gtest_capi_run_all()
    print(f"[INFO] run_all rc={rc} failed={dll.gtest_capi_failed_test_count()}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
