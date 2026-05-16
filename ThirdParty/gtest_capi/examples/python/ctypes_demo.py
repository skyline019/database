#!/usr/bin/env python3
import ctypes
import os
from pathlib import Path


def _module_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _default_dll() -> Path:
    roots = [
        _module_root() / "build" / "bundle",
        _module_root() / "build" / "Debug",
        _module_root() / "build" / "Release",
        _module_root() / "build",
    ]
    names = ["libgtest_capi_samples.dll", "gtest_capi_samples.dll"]
    for r in roots:
        for n in names:
            p = r / n
            if p.exists():
                return p
    return _module_root() / "build" / "gtest_capi_samples.dll"


def _load_windows_bundle(dll_path: Path) -> ctypes.CDLL:
    flag = 0x00000008  # LOAD_WITH_ALTERED_SEARCH_PATH
    for dep in [
        "libwinpthread-1.dll",
        "libgcc_s_seh-1.dll",
        "libstdc++-6.dll",
        "libgtest.dll",
        "gtest.dll",
        "libgtest_main.dll",
        "gtest_main.dll",
    ]:
        dep_path = dll_path.parent / dep
        if dep_path.exists():
            ctypes.WinDLL(str(dep_path), winmode=flag)
    return ctypes.CDLL(str(dll_path), winmode=flag)


def main() -> int:
    dll_path = Path(os.environ.get("GTEST_CAPI_DLL", str(_default_dll())))
    if not dll_path.exists():
        print(f"[ERR] DLL not found: {dll_path}")
        print("Build first: cmake -S . -B build && cmake --build build --target gtest_capi_samples")
        return 2

    dll = _load_windows_bundle(dll_path) if os.name == "nt" else ctypes.CDLL(str(dll_path))
    dll.gtest_capi_set_filter.argtypes = [ctypes.c_char_p]
    dll.gtest_capi_set_filter.restype = ctypes.c_int
    dll.gtest_capi_run_all.argtypes = []
    dll.gtest_capi_run_all.restype = ctypes.c_int
    dll.gtest_capi_total_test_count.argtypes = []
    dll.gtest_capi_total_test_count.restype = ctypes.c_int
    dll.gtest_capi_failed_test_count.argtypes = []
    dll.gtest_capi_failed_test_count.restype = ctypes.c_int

    print(f"[INFO] Using: {dll_path}")
    dll.gtest_capi_set_filter(b"*")
    print(f"[INFO] total={dll.gtest_capi_total_test_count()}")
    rc = dll.gtest_capi_run_all()
    print(f"[INFO] rc={rc} failed={dll.gtest_capi_failed_test_count()}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
