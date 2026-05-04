# gtest_capi (standalone)

独立于 `newdb/` 的 GoogleTest C API 封装模块，可单独拷贝和复用。

## 目录结构

- `include/gtest_capi.h`：稳定 C ABI 头文件
- `src/gtest_capi.cpp`：实现
- `examples/cpp/sample_tests.cpp`：示例测试
- `examples/python/ctypes_demo.py`：Python 调用示例
- `scripts/bundle_mingw_deps.ps1`：MinGW 依赖打包脚本

## 构建（相对路径）

在模块目录下：

```bash
cmake -S . -B build
cmake --build build --target gtest_capi gtest_capi_samples -j
```

## 双方案构建（Windows）

### A) MSVC + /MT 静态运行时

```bash
cmake -S . -B build_msvc_mt -G "Visual Studio 18 2026" -A x64 -DGTEST_CAPI_MSVC_STATIC_RUNTIME=ON
cmake --build build_msvc_mt --config Release --target gtest_capi gtest_capi_samples -j
```

产物示例：

- `build_msvc_mt/Release/gtest_capi.dll`
- `build_msvc_mt/Release/gtest_capi_samples.dll`

### B) MinGW + runtime 打包

```bash
cmake -S . -B build_mingw -G "MinGW Makefiles" ^
  -DCMAKE_C_COMPILER=E:/msys64/mingw64/bin/gcc.exe ^
  -DCMAKE_CXX_COMPILER=E:/msys64/mingw64/bin/g++.exe
cmake --build build_mingw --target gtest_capi gtest_capi_samples -j
powershell -ExecutionPolicy Bypass -File .\scripts\bundle_mingw_deps.ps1 -BuildDir ..\build_mingw -BundleDir ..\build_mingw\bundle
```

打包目录：

- `build_mingw/bundle/`

## 打包（Windows + MinGW）

在模块目录下执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bundle_mingw_deps.ps1
```

会生成：

- `build/bundle/`

其中包含主 DLL、gtest/gtest_main DLL 和运行时依赖，可直接拷贝到其他电脑。

## Python 调用示例

```bash
python .\examples\python\ctypes_demo.py
```

默认优先加载 `build/bundle/*gtest_capi_samples.dll`（若存在），并采用“绝对路径 + 先加载依赖 DLL”的策略，规避 PATH 优先级污染。

可通过环境变量指定 DLL：

```bash
set GTEST_CAPI_DLL=C:\path\to\libgtest_capi_samples.dll
python .\examples\python\ctypes_demo.py
```

## 外部项目复用建议

1. 复制整个 `gtest_capi/` 模块，或至少复制 `include/gtest_capi.h` + `build/bundle/` 到外部项目。
2. 通过 FFI（Python/Rust/C#/Go）按绝对路径加载 `gtest_capi*.dll`。
3. 先加载同目录依赖 DLL（`gtest*.dll` 与运行时），再加载主 DLL，避免系统 DLL 抢占。

## WSL/Linux 产物（.so）

```bash
wsl -e bash -lc "cd /mnt/e/db/DB/gtest_capi && \
  cmake -S . -B build_wsl -DGTEST_CAPI_LOCAL_GTEST_DIR=/mnt/e/db/DB/newdb/build_mingw/_deps/googletest-src && \
  cmake --build build_wsl --target gtest_capi gtest_capi_samples -j 6"
```

产物示例：

- `build_wsl/libgtest_capi.so`
- `build_wsl/libgtest_capi_samples.so`
