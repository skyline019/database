# gtest_capi 复用说明（跨项目 / 跨语言）

本文说明如何在其他项目中复用 `gtest_capi` 动态库，并用 Python/Rust/C# 控制测试执行。

## 1. 可分发文件（Windows + MinGW）

建议把以下文件放在同一目录（例如 `gtest_capi_bundle/`）后整体拷贝到目标机器：

- `libgtest_capi.dll`
- `libgtest.dll`
- `libstdc++-6.dll`
- `libgcc_s_seh-1.dll`
- `libwinpthread-1.dll`

当前工程已生成目录：

- `newdb/build_mingw/gtest_capi_bundle/`

## 2. 头文件与 ABI

公共头文件：

- `newdb/include/gtest_capi.h`

这是 `extern "C"` ABI，可通过 FFI 从多语言调用。

## 3. 外部 C/C++ 项目如何链接

最简单方式是运行时动态加载（`LoadLibrary`/`GetProcAddress`），无需在编译期链接 import lib。

如果你想在 CMake 里声明一个 imported DLL 目标：

```cmake
add_library(gtest_capi SHARED IMPORTED GLOBAL)
set_target_properties(gtest_capi PROPERTIES
  IMPORTED_LOCATION "C:/third_party/gtest_capi_bundle/libgtest_capi.dll"
)
target_include_directories(your_test_controller PRIVATE "C:/path/to/newdb/include")
```

> 注意：纯 DLL 无 `.lib` 时，推荐运行时动态加载。

## 4. Python（ctypes）调用最小示例

```python
import ctypes, os
from pathlib import Path

bundle = Path(r"C:/third_party/gtest_capi_bundle")
# Windows: force self-bundled DLL resolution, avoid PATH priority issues.
if os.name == "nt":
    LOAD_WITH_ALTERED_SEARCH_PATH = 0x00000008
    for dep in [
        "libwinpthread-1.dll",
        "libgcc_s_seh-1.dll",
        "libstdc++-6.dll",
        "libgtest.dll",
    ]:
        p = bundle / dep
        if p.exists():
            ctypes.WinDLL(str(p), winmode=LOAD_WITH_ALTERED_SEARCH_PATH)
    dll = ctypes.CDLL(str(bundle / "libgtest_capi.dll"), winmode=LOAD_WITH_ALTERED_SEARCH_PATH)
else:
    dll = ctypes.CDLL(str(bundle / "libgtest_capi.dll"))

dll.gtest_capi_set_filter.argtypes = [ctypes.c_char_p]
dll.gtest_capi_set_filter.restype = ctypes.c_int
dll.gtest_capi_run_all.argtypes = []
dll.gtest_capi_run_all.restype = ctypes.c_int

dll.gtest_capi_set_filter(b"*")
rc = dll.gtest_capi_run_all()
print("run_all rc =", rc)
```

## 5. Rust（libloading）调用最小示例

```rust
use libloading::{Library, Symbol};

type SetFilter = unsafe extern "C" fn(*const i8) -> i32;
type RunAll = unsafe extern "C" fn() -> i32;

fn main() -> anyhow::Result<()> {
    let lib = unsafe { Library::new("libgtest_capi.dll")? };
    let set_filter: Symbol<SetFilter> = unsafe { lib.get(b"gtest_capi_set_filter")? };
    let run_all: Symbol<RunAll> = unsafe { lib.get(b"gtest_capi_run_all")? };
    let f = std::ffi::CString::new("*")?;
    unsafe {
        set_filter(f.as_ptr());
        println!("rc={}", run_all());
    }
    Ok(())
}
```

## 6. C#（P/Invoke）调用最小示例

```csharp
using System.Runtime.InteropServices;

internal static class GTestCapi {
    [DllImport("libgtest_capi.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern int gtest_capi_set_filter(string filter);

    [DllImport("libgtest_capi.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern int gtest_capi_run_all();
}

// usage
GTestCapi.gtest_capi_set_filter("*");
int rc = GTestCapi.gtest_capi_run_all();
Console.WriteLine($"rc={rc}");
```

## 7. 常见问题

- 运行时报 `could not find module ...`：通常是依赖 DLL 不在同目录，或未加入 DLL 搜索路径。
- 目标机器没有 MinGW runtime：请确保 `libstdc++-6.dll`、`libgcc_s_seh-1.dll`、`libwinpthread-1.dll` 一并分发。
- 跨编译器混用：建议保持同一工具链产物（MinGW 产物配 MinGW 生态）。

## 8. 绕过环境变量（强制使用自带 DLL）

在其他电脑上，为避免系统 PATH 中同名 DLL 抢占，推荐：

1. 把 `libgtest_capi.dll` 与依赖 DLL 放在同一 `bundle` 目录。
2. 使用绝对路径加载，不用裸文件名。
3. 先按绝对路径预加载依赖（`libstdc++-6.dll` 等），再加载主 DLL。
4. Windows 下优先使用 `LoadLibraryEx` 语义（`LOAD_WITH_ALTERED_SEARCH_PATH`）。

这样可以最大程度规避“环境变量导致的 DLL 链接优先级问题”。
