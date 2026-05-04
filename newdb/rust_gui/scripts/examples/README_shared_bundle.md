# newdb shared_bundle 跨语言调用说明

本文说明如何把 `shared_bundle` 作为独立运行时交付，并在 Python / C# / Node.js 中以“优先使用自带依赖”的方式调用。

## 1. shared_bundle 是什么

`shared_bundle` 是一个“自包含目录”，里面包括：

- 可执行文件（`newdb_demo`、`newdb_tests` 等）
- 动态库（`newdb.dll` / `libnewdb.so`，`gtest_capi` 等）
- 运行时依赖（MSVC/MinGW 依赖 DLL，或 Linux 下的 `.so`）
- 启动脚本（`run_*.cmd` / `run_*.sh`）

### 默认输出目录

- VS/MSVC: `build/newdb-vs2026-mt-tests/shared_bundle/Release`
- MinGW: `build/newdb-mingw/shared_bundle/Release`
- WSL/Linux: `build/newdb-wsl-tests/shared_bundle/Release`

---

## 2. 先构建 shared_bundle

### 2.1 Windows (VS)

```powershell
cmake --build build/newdb-vs2026-mt-tests --config Release --target shared_bundle --parallel
```

### 2.2 Windows (MinGW)

```powershell
cmake --build build/newdb-mingw --target shared_bundle --parallel
```

### 2.3 WSL/Linux

```bash
cmake --build build/newdb-wsl-tests --target shared_bundle -j8
```

---

## 3. 完整接口说明（newdb C API）

头文件：`newdb/engine/include/newdb/c_api.h`

### 3.1 版本与兼容性常量

- `NEWDB_C_API_VERSION_MAJOR` / `MINOR` / `PATCH`
  - 当前值：`0.5.0`
  - 用途：供调用方在运行时或启动阶段打印/检查版本。
- `NEWDB_C_API_ABI_VERSION`
  - 当前值：`1`
  - 用途：二进制 ABI 协商（不同语言运行时加载时优先检查）。

### 3.2 错误码（`newdb_error_code`）

- `NEWDB_OK = 0`
  - 操作成功。
- `NEWDB_ERR_INVALID_ARGUMENT = 1`
  - 参数无效（空指针、空字符串、缓冲区长度不合法等）。
- `NEWDB_ERR_INVALID_HANDLE = 2`
  - `newdb_session_handle` 无效或已释放。
- `NEWDB_ERR_EXECUTION_FAILED = 3`
  - 命令执行失败（业务错误、语法错误、约束冲突等）。
- `NEWDB_ERR_INTERNAL = 4`
  - 内部异常。
- `NEWDB_ERR_LOG_IO = 5`
  - 日志/文件读写异常。
- `NEWDB_ERR_SESSION_TERMINATED = 6`
  - 会话已终止（例如执行到退出语义命令）。

### 3.3 结构体与句柄

- `typedef void* newdb_session_handle`
  - 不透明句柄。必须由 `newdb_session_create` 创建，最终调用 `newdb_session_destroy` 释放。
- `typedef struct newdb_schema_check_result { int ok; char message[512]; }`
  - `ok=1` 表示 schema 校验通过，`ok=0` 表示失败。
  - `message` 始终以 `\0` 结尾（最多 511 字节有效内容）。

### 3.4 线程安全约定

- 全局 API（版本/ABI/错误码字符串）是线程安全的。
- 会话 API 约定“一线程一会话句柄”。
- 多线程共享同一个 `newdb_session_handle` 不保证线程安全。

### 3.5 函数逐项说明

#### A) 全局/元信息接口

- `const char* newdb_version_string(void);`
  - 返回：静态版本字符串（例如 `newdb-c-api/0.5.0`）。
  - 失败：无（返回非空常量字符串）。

- `int newdb_api_version_major(void);`
- `int newdb_api_version_minor(void);`
- `int newdb_api_version_patch(void);`
  - 返回：对应版本号整数。

- `int newdb_abi_version(void);`
  - 返回：当前 ABI 版本（当前为 `1`）。

- `int newdb_negotiate_abi(int requested_abi);`
  - 入参：`requested_abi` 调用方期望 ABI。
  - 返回：
    - `1`：兼容（可继续调用）
    - `0`：不兼容（应拒绝加载）

- `const char* newdb_error_code_string(int code);`
  - 入参：错误码整数（可传枚举值或未知值）。
  - 返回：对应错误码文本（未知值通常返回兜底字符串）。

- `int newdb_sum(int lhs, int rhs);`
  - 入参：两个整数。
  - 返回：两数之和（用于 FFI 烟测连通性）。

#### B) Schema 检查接口

- `newdb_schema_check_result newdb_check_schema_file(const char* attr_file_path);`
  - 入参：schema 文件路径（通常是 `.attr`）。
  - 返回：
    - `ok=1`：格式合法
    - `ok=0`：不合法，`message` 带错误信息

#### C) 会话生命周期接口

- `newdb_session_handle newdb_session_create(const char* data_dir, const char* table_name, const char* log_file_path);`
  - 入参：
    - `data_dir`：数据目录
    - `table_name`：初始表名
    - `log_file_path`：日志文件路径
  - 返回：
    - 成功：非空句柄
    - 失败：`NULL`
  - 说明：建议创建后立刻调用一次 `newdb_session_last_error` 检查初始化状态。

- `void newdb_session_destroy(newdb_session_handle handle);`
  - 入参：会话句柄（允许空句柄，空操作）。
  - 返回：无。
  - 说明：销毁后句柄不可复用。

- `int newdb_session_set_table(newdb_session_handle handle, const char* table_name);`
  - 入参：句柄 + 新表名。
  - 返回：`newdb_error_code`。
  - 常见失败：句柄无效、表名非法。

- `int newdb_session_last_error(newdb_session_handle handle);`
  - 入参：会话句柄。
  - 返回：当前会话最近一次错误码（`newdb_error_code`）。

#### D) 命令执行与统计接口

- `int newdb_session_execute(newdb_session_handle handle, const char* command_line, char* output_buf, size_t output_buf_size);`
  - 入参：
    - `handle`：会话句柄
    - `command_line`：命令行文本（与 CLI 命令兼容）
    - `output_buf`：输出缓冲区
    - `output_buf_size`：缓冲区大小（字节）
  - 返回：`newdb_error_code`
  - 输出约定：
    - 成功/失败文本都会写入 `output_buf`
    - 建议预留较大缓冲区（如 4KB~64KB）
    - 缓冲区不足时内容会截断，但应保持 `\0` 结尾

- `int newdb_session_runtime_stats(newdb_session_handle handle, char* output_buf, size_t output_buf_size);`
  - 入参：句柄 + 输出缓冲区。
  - 返回：`newdb_error_code`
  - 输出：JSON 文本，包含运行时统计计数（vacuum、wal、锁等待等）。

- `int newdb_session_append_runtime_snapshot(newdb_session_handle handle, const char* output_jsonl_path, const char* label);`
  - 入参：
    - `output_jsonl_path`：目标 jsonl 文件
    - `label`：可选标签（可为 `NULL` 或空）
  - 返回：`newdb_error_code`
  - 行为：将当前 runtime 统计追加为一行 JSON 到文件。

### 3.6 调用顺序建议

1. `newdb_negotiate_abi(NEWDB_C_API_ABI_VERSION)`  
2. `newdb_session_create(...)`  
3. 多次 `newdb_session_execute(...)` / `newdb_session_runtime_stats(...)`  
4. 出错时 `newdb_session_last_error(...)` + `newdb_error_code_string(...)`  
5. `newdb_session_destroy(...)`

### 3.7 最小错误处理范式

- 每次 API 调用检查返回码是否为 `NEWDB_OK`
- 对失败码调用 `newdb_error_code_string(code)` 做可读映射
- 对 `execute` 场景优先看 `output_buf` 详细错误文本

---

## 3A. 完整接口说明（gtest_capi）

头文件：`newdb/include/gtest_capi.h`

### 3A.1 回调类型

- `gtest_capi_line_callback(const char* line, void* user_data)`
  - 用于按行返回 `Suite.Test` 形式列表。

- `gtest_capi_test_callback(...)`
  - 参数：
    - `suite_name` / `test_name`
    - `should_run`：是否匹配当前 filter（1/0）
    - `is_disabled`：是否禁用（1/0）
    - `result_status`：`0=notrun,1=passed,2=failed,3=skipped`

### 3A.2 初始化与执行

- `int gtest_capi_init_from_argv(int argc, const char* const* argv);`
- `int gtest_capi_run_all(void);`
- `int gtest_capi_list_tests(gtest_capi_line_callback cb, void* ud);`
- `int gtest_capi_enumerate_tests(gtest_capi_test_callback cb, void* ud);`

返回约定：

- 非负值通常表示成功（例如计数）
- 负值表示错误（参数错误/未初始化等）

### 3A.3 选项接口（Set/Get 成对）

- Filter/Output：
  - `gtest_capi_set_filter` / `gtest_capi_get_filter`
  - `gtest_capi_set_output` / `gtest_capi_get_output`
- 执行控制：
  - `set_repeat/get_repeat`
  - `set_shuffle/get_shuffle`
  - `set_random_seed/get_random_seed`
  - `set_color/get_color`
  - `set_break_on_failure/get_break_on_failure`
  - `set_throw_on_failure/get_throw_on_failure`
  - `set_catch_exceptions/get_catch_exceptions`
  - `set_also_run_disabled_tests/get_also_run_disabled_tests`
  - `set_brief/get_brief`
  - `set_print_time/get_print_time`

### 3A.4 通用键值接口

- `int gtest_capi_set_option(const char* key, const char* value);`
- `const char* gtest_capi_get_option(const char* key);`

支持 key：

- `filter`
- `output`
- `repeat`
- `shuffle`
- `random_seed`
- `color`
- `break_on_failure`
- `throw_on_failure`
- `catch_exceptions`
- `also_run_disabled_tests`
- `brief`
- `print_time`

### 3A.5 统计接口

- `gtest_capi_total_test_suite_count`
- `gtest_capi_total_test_count`
- `gtest_capi_test_to_run_count`
- `gtest_capi_successful_test_count`
- `gtest_capi_failed_test_count`
- `gtest_capi_skipped_test_count`
- `gtest_capi_disabled_test_count`
- `gtest_capi_reportable_disabled_test_count`
- `gtest_capi_elapsed_time_ms`

这些接口可用于语言侧测试面板、统计上报、CI 汇总。

---

## 4. 依赖优先策略（避免环境变量抢库）

### 4.1 Windows

推荐规则：

1. 使用绝对路径加载 DLL。
2. 用 `LoadLibraryEx(..., LOAD_WITH_ALTERED_SEARCH_PATH)` 或等价机制。
3. 先预加载依赖 DLL（MinGW runtime、gtest 等），再加载主库。
4. 或直接使用 bundle 自带 `run_*.cmd`。

### 4.2 Linux/WSL

`shared_bundle` 目标已设置 `RPATH=$ORIGIN`，同目录 `.so` 优先。  
调用时仍建议用绝对路径加载主库，确保路径明确。

---

## 5. Python 可运行示例（ctypes）

文件名示例：`use_newdb_python.py`

```python
import ctypes
import os
from pathlib import Path


def preload_windows_dependencies(bundle: Path) -> None:
    if os.name != "nt":
        return
    # 强制按 DLL 所在目录解析其依赖，减少 PATH 污染影响
    load_with_altered_search_path = 0x00000008
    dep_candidates = [
        "libwinpthread-1.dll",
        "libgcc_s_seh-1.dll",
        "libstdc++-6.dll",
        "gtest.dll",
        "gtest_main.dll",
        "libgtest.dll",
        "libgtest_main.dll",
    ]
    for name in dep_candidates:
        p = bundle / name
        if p.exists():
            ctypes.WinDLL(str(p), winmode=load_with_altered_search_path)


def load_newdb(bundle: Path):
    preload_windows_dependencies(bundle)
    if os.name == "nt":
        load_with_altered_search_path = 0x00000008
        dll_path = bundle / "newdb.dll"
        return ctypes.CDLL(str(dll_path), winmode=load_with_altered_search_path)
    return ctypes.CDLL(str(bundle / "libnewdb.so"))


def main():
    bundle = Path(r"E:\db\DB\build\newdb-vs2026-mt-tests\shared_bundle\Release")
    # Linux 示例可改成:
    # bundle = Path("/mnt/e/db/DB/build/newdb-wsl-tests/shared_bundle/Release")

    lib = load_newdb(bundle)

    lib.newdb_version_string.argtypes = []
    lib.newdb_version_string.restype = ctypes.c_char_p
    lib.newdb_abi_version.argtypes = []
    lib.newdb_abi_version.restype = ctypes.c_int
    lib.newdb_sum.argtypes = [ctypes.c_int, ctypes.c_int]
    lib.newdb_sum.restype = ctypes.c_int

    print("version =", lib.newdb_version_string().decode("utf-8", errors="replace"))
    print("abi =", lib.newdb_abi_version())
    print("sum(7, 35) =", lib.newdb_sum(7, 35))


if __name__ == "__main__":
    main()
```

运行：

```powershell
python .\use_newdb_python.py
```

---

## 6. C# 可运行示例（.NET 8）

文件名示例：`Program.cs`

```csharp
using System;
using System.IO;
using System.Runtime.InteropServices;

internal static class Native
{
    [DllImport("newdb.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr newdb_version_string();

    [DllImport("newdb.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern int newdb_abi_version();

    [DllImport("newdb.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern int newdb_sum(int lhs, int rhs);
}

class Program
{
    static void Main()
    {
        // 让进程优先从 shared_bundle 当前目录解析 DLL
        var bundle = @"E:\db\DB\build\newdb-vs2026-mt-tests\shared_bundle\Release";
        Directory.SetCurrentDirectory(bundle);

        var verPtr = Native.newdb_version_string();
        var version = Marshal.PtrToStringAnsi(verPtr) ?? "<null>";

        Console.WriteLine($"version = {version}");
        Console.WriteLine($"abi = {Native.newdb_abi_version()}");
        Console.WriteLine($"sum(10, 20) = {Native.newdb_sum(10, 20)}");
    }
}
```

创建并运行（Windows）：

```powershell
dotnet new console -n NewdbCSharpDemo
cd .\NewdbCSharpDemo
# 用上面的 Program.cs 覆盖默认文件
dotnet run -c Release
```

> 如果你在 Linux 上跑 C#，把库名换为 `libnewdb.so`，并把 `bundle` 路径改成 Linux 路径。

---

## 7. Node.js 可运行示例（ffi-napi）

文件名示例：`use_newdb_node.js`

```javascript
const path = require("path");
const os = require("os");
const ffi = require("ffi-napi");

const isWin = os.platform() === "win32";

const bundle = isWin
  ? "E:/db/DB/build/newdb-vs2026-mt-tests/shared_bundle/Release"
  : "/mnt/e/db/DB/build/newdb-wsl-tests/shared_bundle/Release";

const libPath = isWin
  ? path.join(bundle, "newdb.dll")
  : path.join(bundle, "libnewdb.so");

const api = ffi.Library(libPath, {
  newdb_version_string: ["string", []],
  newdb_abi_version: ["int", []],
  newdb_sum: ["int", ["int", "int"]],
});

console.log("version =", api.newdb_version_string());
console.log("abi =", api.newdb_abi_version());
console.log("sum(3, 9) =", api.newdb_sum(3, 9));
```

安装并运行：

```bash
npm init -y
npm install ffi-napi ref-napi
node use_newdb_node.js
```

---

## 8. 分发建议（任意语言项目）

1. **整目录分发**：直接拷贝 `shared_bundle/Release`，不要只拷主库。
2. **固定平台**：Windows 产物给 Windows，Linux 产物给 Linux，不混用。
3. **固定加载路径**：调用方总是用绝对路径加载主库。
4. **优先脚本启动**：可执行程序优先用 `run_*.cmd` / `run_*.sh`。

---

## 9. 常见问题

- **加载失败：找不到模块**
  - 通常是主库存在，但依赖 DLL/SO 没有同目录或路径错。
- **Windows 上加载到系统同名 DLL**
  - 改用绝对路径 + 预加载依赖，或通过 `run_*.cmd` 启动。
- **WSL 构建时联网拉 gtest 失败**
  - 可配置 `-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=...` 指向本地镜像目录。

---

## 10. 接口矩阵（SDK 评审版）

本节用于对外评审/集成对齐，给出每个接口的调用级别、线程安全与常见错误。

### 10.1 newdb C API 矩阵

- `newdb_version_string`
  - 级别：可选（诊断）
  - 线程安全：是（全局）
  - 幂等性：是
  - 典型失败码：无
- `newdb_api_version_major/minor/patch`
  - 级别：可选（诊断）
  - 线程安全：是（全局）
  - 幂等性：是
  - 典型失败码：无
- `newdb_abi_version`
  - 级别：建议（加载前）
  - 线程安全：是（全局）
  - 幂等性：是
  - 典型失败码：无
- `newdb_negotiate_abi`
  - 级别：建议（加载前）
  - 线程安全：是（全局）
  - 幂等性：是
  - 返回：`1` 兼容，`0` 不兼容
- `newdb_error_code_string`
  - 级别：建议（错误处理）
  - 线程安全：是（全局）
  - 幂等性：是
  - 典型失败码：无（未知 code 返回兜底字符串）
- `newdb_sum`
  - 级别：可选（联调烟测）
  - 线程安全：是（全局）
  - 幂等性：是
  - 典型失败码：无
- `newdb_check_schema_file`
  - 级别：可选（导入前检查）
  - 线程安全：是（全局）
  - 幂等性：是（对同一文件）
  - 典型失败码：通过结构体 `ok/message` 表达
- `newdb_session_create`
  - 级别：必调（会话模式）
  - 线程安全：每线程独立 handle 安全
  - 幂等性：否（每次创建新会话）
  - 典型失败：返回 `NULL`
- `newdb_session_destroy`
  - 级别：必调（资源回收）
  - 线程安全：每线程独立 handle 安全
  - 幂等性：建议按“仅销毁一次”使用
  - 典型失败码：无（void）
- `newdb_session_set_table`
  - 级别：可选
  - 线程安全：同一 handle 并发不保证
  - 幂等性：同表名近似幂等
  - 典型失败码：`INVALID_ARGUMENT` / `INVALID_HANDLE`
- `newdb_session_last_error`
  - 级别：建议（错误诊断）
  - 线程安全：同一 handle 并发不保证
  - 幂等性：是（读取状态）
  - 典型失败码：`INVALID_HANDLE`
- `newdb_session_execute`
  - 级别：必调（核心）
  - 线程安全：同一 handle 并发不保证
  - 幂等性：取决于命令（读命令通常幂等，写命令通常非幂等）
  - 典型失败码：`INVALID_ARGUMENT` / `INVALID_HANDLE` / `EXECUTION_FAILED` / `SESSION_TERMINATED`
- `newdb_session_runtime_stats`
  - 级别：可选（观测）
  - 线程安全：同一 handle 并发不保证
  - 幂等性：近似幂等（统计会随时间变化）
  - 典型失败码：`INVALID_ARGUMENT` / `INVALID_HANDLE`
- `newdb_session_append_runtime_snapshot`
  - 级别：可选（审计/画像）
  - 线程安全：同一 handle 并发不保证
  - 幂等性：否（追加写入）
  - 典型失败码：`INVALID_ARGUMENT` / `INVALID_HANDLE` / `LOG_IO`

### 10.2 gtest_capi 接口矩阵

- `gtest_capi_init_from_argv`
  - 级别：建议（首次调用前）
  - 幂等性：可重复初始化（以后一次参数为准）
  - 失败返回：负值
- `gtest_capi_run_all`
  - 级别：核心
  - 幂等性：与当前过滤条件/随机种子相关
  - 返回：GoogleTest exit code（0 成功）
- `gtest_capi_list_tests`
  - 级别：可选
  - 幂等性：是（同配置下）
  - 返回：枚举数量或负值
- `gtest_capi_enumerate_tests`
  - 级别：可选（测试平台建议）
  - 幂等性：是（同配置下）
  - 返回：枚举数量或负值
- `gtest_capi_set_* / get_*`
  - 级别：可选（运行参数控制）
  - 幂等性：set 对同值幂等，get 幂等
  - 返回：`0` 成功，负值失败（按函数）
- `gtest_capi_set_option / get_option`
  - 级别：建议（多语言统一配置）
  - 幂等性：同上
  - 失败返回：负值/`NULL`
- `gtest_capi_*count / elapsed_time_ms`
  - 级别：可选（统计）
  - 幂等性：读取接口，结果随执行阶段变化

---

## 11. 推荐调用流程（生产集成）

### 11.1 newdb（业务调用）

1. 加载库文件（绝对路径）  
2. `newdb_negotiate_abi(NEWDB_C_API_ABI_VERSION)`  
3. `newdb_session_create(...)`  
4. 循环执行 `newdb_session_execute(...)`  
5. 出错时：
   - `newdb_session_last_error(handle)`
   - `newdb_error_code_string(code)`
   - 结合 `output_buf` 文本定位
6. 可选：
   - `newdb_session_runtime_stats(...)`
   - `newdb_session_append_runtime_snapshot(...)`
7. `newdb_session_destroy(handle)`

### 11.2 gtest_capi（测试平台调用）

1. 加载 `gtest_capi`（绝对路径）  
2. `gtest_capi_init_from_argv(...)`（可选但建议）  
3. 设置参数（`set_filter` / `set_option`）  
4. 执行 `gtest_capi_run_all()`  
5. 采集统计（`*_count` / `elapsed_time_ms`）  
6. 失败时读取配置并二次诊断（`get_option` + 枚举接口）

---

## 12. 结合项目功能的 DLL 外部调用说明（完整能力视角）

本节不是“函数签名级”说明，而是把 `newdb.dll` / `libnewdb.so` 当成一个嵌入式数据库引擎来说明外部如何使用其完整功能。

### 12.1 你真正拿到的能力边界

通过 `newdb` C API（核心是 `newdb_session_execute`）可覆盖以下能力：

- **表与模式管理（DDL）**
  - 建表、切表、模式加载/刷新、属性定义与校验。
- **数据读写（DML + Query）**
  - 插入、更新、删除、条件查询、分页、聚合、导入导出。
- **事务能力**
  - 事务开启/提交/回滚，冲突检测，锁等待策略。
- **WAL 与恢复**
  - 写前日志、恢复路径、段管理相关能力。
- **MVCC 可见性读**
  - 读视图与可见性控制由引擎内部处理。
- **Sidecar/索引加速能力**
  - page/equality/covering/visibility 相关侧车索引能力。
- **运行时观测**
  - `newdb_session_runtime_stats` + `newdb_session_append_runtime_snapshot` 提供结构化运行指标。

换句话说：外部系统无需直连底层存储结构，使用“会话 + 命令字符串”即可驱动全功能。

### 12.2 典型外部系统集成形态

- **桌面应用/GUI**
  - 每个窗口或标签页一个 `newdb_session_handle`，生命周期和页面一致。
- **服务端 API 网关**
  - 每个请求/每个租户维护独立会话句柄，避免多线程共享同句柄。
- **批处理/ETL**
  - 单会话串行执行命令，批次结束后写 runtime snapshot 归档。
- **测试平台**
  - 业务接口走 `newdb.dll`，测试控制走 `gtest_capi.dll`。

### 12.3 命令驱动模型（最重要）

`newdb` 外部调用本质上是：

1. 创建会话（绑定数据目录、表名、日志）
2. 向 `newdb_session_execute` 送入命令行字符串
3. 解析 `output_buf` 文本与返回码

因此你可以把 DLL 看成“可嵌入命令引擎”，上层语言只负责：

- 组装命令
- 调用执行
- 解析输出
- 做重试/补偿/告警

### 12.4 业务能力到命令调用的建议映射

建议在外部 SDK 层实现“命令工厂”，不要让业务方直接拼字符串：

- **表管理层**
  - `create_table(...)` -> 生成对应 DDL 命令后执行
- **写入层**
  - `insert_row(...)`, `update_row(...)`, `delete_row(...)`
- **查询层**
  - `find_by_id(...)`, `where(...)`, `page(...)`, `sum/avg/min/max(...)`
- **事务层**
  - `begin()`, `commit()`, `rollback()`, `savepoint()/rollback_to()/release()`
- **观测层**
  - `runtime_stats_json()`, `append_runtime_snapshot(path,label)`

这样可确保命令格式统一、日志统一、错误处理统一。

### 12.5 错误处理分层（建议落地）

建议分 3 层判断，不要只看一个字段：

1. **返回码层**（`newdb_error_code`）
2. **会话错误层**（`newdb_session_last_error`）
3. **文本诊断层**（`output_buf`）

推荐策略：

- `INVALID_ARGUMENT`：调用方 bug，立即失败并报警。
- `INVALID_HANDLE`：句柄生命周期错误，销毁并重建会话。
- `EXECUTION_FAILED`：业务失败，按业务规则决定重试或回滚。
- `LOG_IO`：环境问题（磁盘/权限），快速失败并上报基础设施。
- `SESSION_TERMINATED`：终止后不可继续，必须新建会话。

### 12.6 事务与并发实战建议

- 一线程一会话，避免共享 handle。
- 写操作放在显式事务中，失败路径统一回滚。
- 冲突高场景：
  - 读取 `runtime_stats` 的冲突与等待指标，
  - 上层做限流/重试退避（指数退避 + 抖动）。

### 12.7 运行时观测与运维接入

建议每个外部集成都接这两条：

- `newdb_session_runtime_stats`：用于实时面板（JSON）
- `newdb_session_append_runtime_snapshot`：用于时序归档（JSONL）

推荐采集指标：

- 事务提交延迟（p95/max）
- 冲突次数/等待超时次数
- WAL 恢复耗时/扫描记录数
- vacuum 触发与回收字节数

### 12.8 外部 SDK 最小封装模板（伪代码）

```text
load_library()
assert newdb_negotiate_abi(NEWDB_C_API_ABI_VERSION) == 1

h = newdb_session_create(data_dir, table, log_file)
if h == NULL: fail_fast()

for cmd in commands:
    rc, out = execute(h, cmd)
    if rc != NEWDB_OK:
        err = newdb_session_last_error(h)
        raise mapped_exception(err, out)

stats = runtime_stats(h)   # optional
append_snapshot(h, path, label)  # optional

newdb_session_destroy(h)
```

### 12.9 与 gtest_capi 的协同场景

如果外部平台既要“跑业务”又要“跑测试”，建议分两套运行时：

- `newdb.dll`：业务数据流
- `gtest_capi.dll`：测试执行流

不要在同一业务会话中混入测试控制逻辑。测试平台可并行跑：

- 一路调用 `newdb` 做业务回归
- 一路调用 `gtest_capi` 做白盒测试

### 12.10 交付给第三方项目的最终清单

必交付：

- `shared_bundle/Release` 整目录
- `newdb/engine/include/newdb/c_api.h`
- （可选）`newdb/include/gtest_capi.h`
- 本文档 `README_shared_bundle.md`

强约束：

- 禁止只拷单个 DLL/SO
- 禁止跨平台混用产物
- 调用方必须使用绝对路径加载库

---

## 13. 命令模板速查表（可直接传给 `newdb_session_execute`）

说明：

- 下列命令字符串均为 `command_line` 入参示例。
- 实际返回结果/错误信息请读取 `output_buf`。
- 命令大小写按实现支持大小写不敏感，但建议统一大写前缀。

### 13.1 DDL / 模式管理

- `CREATE TABLE(users)`
- `USE(users)`
- `SET PRIMARY KEY(uid)`
- `CREATE SCHEMA(finance_v1)`
- `DROP SCHEMA(finance_v1)`
- `ALTER TABLE users SET SCHEMA(finance_v1)`
- `ALTER TABLE users REMOVE SCHEMA`
- `RENAME TABLE(users_v2)`
- `DROP TABLE(users_v2)`

### 13.2 属性定义与文件导入导出

- `DEFATTR(name:string, balance:int, dept:string)`
- `IMPORTDIR(E:/data/bootstrap)`
- `EXPORT(E:/data/export/users.csv)`

### 13.3 DML（增删改）

- `INSERT(1, alice, 1000, rd)`
- `BULKINSERT(10000,500,ops)`
- `BULKINSERTFAST(20000,1000,ops)`
- `UPDATE(1, alice, 1200)`  
  或（配合 DEFATTR）  
  `UPDATE(1, alice, 1200, rd)`
- `DELETE(1)`
- `DELETEPK(alice)`
- `SETATTR(1, dept, infra)`
- `DELATTR(dept)`
- `RENATTR(balance, amount)`

### 13.4 查询与聚合

- `FIND(1)`
- `FINDPK(alice)`
- `QBAL(5000)`
- `WHERE(balance, >, 1000)`
- `WHERE(balance, >, 1000, AND, dept, =, rd)`
- `WHEREP(name, WHERE, dept, =, rd)`
- `COUNT`
- `COUNT(balance, >, 1000)`
- `PAGE(1, 20, id, desc)`
- `MIN(balance)`
- `MAX(balance)`
- `SUM(balance)`
- `AVG(balance)`
- 条件聚合示例：  
  `SUM(balance, WHERE, dept, =, rd, AND, balance, >, 500)`

### 13.5 事务 / 恢复 / 运行时控制

- `BEGIN`
- `COMMIT`
- `ROLLBACK`
- `SAVEPOINT(sp1)`
- `ROLLBACK TO(sp1)`
- `RELEASE SAVEPOINT(sp1)`
- `RECOVER TO LSN 100`
- `RECOVER TO TIME 2026-04-29 11:00:00`
- `TXNISOLATION snapshot`
- `TXNISOLATION read_committed`
- `WRITECONFLICT reject`
- `WRITECONFLICT wait 2000`

### 13.6 WAL / Vacuum / LSM 调优

- `AUTOVACUUM on 1000`
- `AUTOVACUUM threshold 500`
- `AUTOVACUUM interval 60`
- `AUTOVACUUM off`
- `WALSYNC full`
- `WALSYNC normal 200`
- `WALSYNC off`
- `WALADAPTIVE on`
- `GROUPCOMMIT 10 128`
- `HOTINDEX on`
- `SEGMENT 64`

### 13.7 推荐的外部封装签名（建议）

建议不要把上述字符串散落在业务代码中，统一封装：

- `exec_ddl_create_table(name)`
- `exec_dml_insert(row)`
- `exec_query_where(conds)`
- `exec_txn_begin/commit/rollback()`
- `exec_runtime_tune(option, value)`

这样可以做到参数校验、日志审计、失败重试策略统一。

---

## 14. 返回输出样例与解析规范（接入方必看）

`newdb_session_execute` 的最终判定建议始终基于三元组：

- 返回码（`rc`）
- 会话错误码（`newdb_session_last_error`）
- 输出文本（`output_buf`）

不要只依赖其中一个维度。

### 14.1 统一判定规则（推荐）

1. 先看 `rc`  
   - `NEWDB_OK`：进入成功路径
   - 非 `NEWDB_OK`：进入失败路径
2. 失败路径再读 `newdb_session_last_error(handle)` 做稳定分类
3. 使用 `output_buf` 作为人类可读诊断（日志/UI）

### 14.2 成功输出样例（示意）

> 说明：不同命令输出格式会有差异，以下是“结构特征示例”，用于指导解析策略。

- **`FIND(1)` 成功**
  - `rc = NEWDB_OK`
  - `last_error = NEWDB_OK`
  - `output_buf` 可能包含行级数据文本，例如：
    - 包含主键值
    - 包含属性键值对

- **`COUNT(balance, >, 1000)` 成功**
  - `rc = NEWDB_OK`
  - `last_error = NEWDB_OK`
  - `output_buf` 包含计数结果文本（通常带数字）

- **`SUM(balance)` 成功**
  - `rc = NEWDB_OK`
  - `last_error = NEWDB_OK`
  - `output_buf` 包含聚合值文本

- **`BEGIN` / `COMMIT` 成功**
  - `rc = NEWDB_OK`
  - `last_error = NEWDB_OK`
  - `output_buf` 常为状态提示文本

### 14.3 失败输出样例（示意）

- **参数错误（如空命令）**
  - `rc = NEWDB_ERR_INVALID_ARGUMENT`
  - `last_error = NEWDB_ERR_INVALID_ARGUMENT`
  - `output_buf` 常含 usage 或参数错误提示

- **句柄错误（已销毁后继续调用）**
  - `rc = NEWDB_ERR_INVALID_HANDLE`
  - `last_error = NEWDB_ERR_INVALID_HANDLE`
  - `output_buf` 可能为空或包含 handle 无效提示

- **业务执行失败（语法/约束/找不到对象）**
  - `rc = NEWDB_ERR_EXECUTION_FAILED`
  - `last_error = NEWDB_ERR_EXECUTION_FAILED`
  - `output_buf` 常含具体失败原因

- **会话终止后继续执行**
  - `rc = NEWDB_ERR_SESSION_TERMINATED`
  - `last_error = NEWDB_ERR_SESSION_TERMINATED`
  - `output_buf` 常含终止提示

### 14.4 输出缓冲区约束与建议

- `output_buf` 建议至少 `4096` 字节，复杂查询建议 `16384` 或更大。
- 当输出被截断时，不应依赖文本完整性做逻辑判断。
- 逻辑判断应依赖 `rc` 与 `last_error`，文本只用于展示和排障。

### 14.5 推荐解析器实现（语言无关）

建议统一封装结果结构：

```text
ExecuteResult {
  rc: int
  last_error: int
  error_name: string        // 由 newdb_error_code_string 得到
  output: string
  ok: bool                  // rc == NEWDB_OK
}
```

推荐伪代码：

```text
rc = newdb_session_execute(...)
last = newdb_session_last_error(handle)
name = newdb_error_code_string(last)
result = { rc, last, name, output, ok = (rc == NEWDB_OK) }
```

### 14.6 针对 gtest_capi 的结果判定

- `gtest_capi_run_all()`：
  - `0` = 全部通过
  - 非 `0` = 有失败用例或执行异常
- 推荐同时采集：
  - `gtest_capi_total_test_count`
  - `gtest_capi_failed_test_count`
  - `gtest_capi_elapsed_time_ms`

### 14.7 日志与可观测性建议

- 每次执行记录：
  - 命令摘要（脱敏）
  - `rc` / `last_error` / `error_name`
  - 输出摘要（前 N 字符）
  - 耗时
- 将 `newdb_session_runtime_stats` 周期性写入监控系统，建立异常基线。

---

## 15. 统一解析器实现（Python / C# / Node）

目标：三种语言都返回同一语义结构，方便上层系统统一处理。

统一结构（逻辑）：

```text
ExecuteResult {
  rc: int
  last_error: int
  error_name: string
  output: string
  ok: bool
}
```

### 15.1 Python 实现（ctypes）

```python
import ctypes
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ExecuteResult:
    rc: int
    last_error: int
    error_name: str
    output: str
    ok: bool


class NewdbClient:
    def __init__(self, bundle_dir: str):
        p = Path(bundle_dir)
        dll = p / "newdb.dll" if (p / "newdb.dll").exists() else p / "libnewdb.so"
        self.lib = ctypes.CDLL(str(dll))

        self.lib.newdb_error_code_string.argtypes = [ctypes.c_int]
        self.lib.newdb_error_code_string.restype = ctypes.c_char_p
        self.lib.newdb_session_create.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
        self.lib.newdb_session_create.restype = ctypes.c_void_p
        self.lib.newdb_session_destroy.argtypes = [ctypes.c_void_p]
        self.lib.newdb_session_destroy.restype = None
        self.lib.newdb_session_last_error.argtypes = [ctypes.c_void_p]
        self.lib.newdb_session_last_error.restype = ctypes.c_int
        self.lib.newdb_session_execute.argtypes = [
            ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t
        ]
        self.lib.newdb_session_execute.restype = ctypes.c_int

    def create_session(self, data_dir: str, table: str, log_file: str):
        h = self.lib.newdb_session_create(
            data_dir.encode(), table.encode(), log_file.encode()
        )
        if not h:
            raise RuntimeError("newdb_session_create failed")
        return h

    def destroy_session(self, handle):
        self.lib.newdb_session_destroy(handle)

    def execute(self, handle, command: str, out_size: int = 16384) -> ExecuteResult:
        buf = ctypes.create_string_buffer(out_size)
        rc = self.lib.newdb_session_execute(handle, command.encode(), buf, out_size)
        last = self.lib.newdb_session_last_error(handle)
        err = self.lib.newdb_error_code_string(last)
        err_name = err.decode("utf-8", errors="replace") if err else "UNKNOWN"
        out = buf.value.decode("utf-8", errors="replace")
        return ExecuteResult(rc=rc, last_error=last, error_name=err_name, output=out, ok=(rc == 0))


if __name__ == "__main__":
    client = NewdbClient(r"E:\db\DB\build\newdb-vs2026-mt-tests\shared_bundle\Release")
    h = client.create_session(r"E:\db\DB\data", "users", r"E:\db\DB\data\newdb.log")
    try:
        r = client.execute(h, "COUNT")
        print(r)
    finally:
        client.destroy_session(h)
```

### 15.2 C# 实现（.NET）

```csharp
using System;
using System.Runtime.InteropServices;
using System.Text;

public sealed record ExecuteResult(
    int Rc,
    int LastError,
    string ErrorName,
    string Output,
    bool Ok
);

internal static class Native
{
    [DllImport("newdb.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr newdb_session_create(string dataDir, string tableName, string logFilePath);

    [DllImport("newdb.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern void newdb_session_destroy(IntPtr handle);

    [DllImport("newdb.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern int newdb_session_last_error(IntPtr handle);

    [DllImport("newdb.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr newdb_error_code_string(int code);

    [DllImport("newdb.dll", CallingConvention = CallingConvention.Cdecl)]
    public static extern int newdb_session_execute(
        IntPtr handle,
        string commandLine,
        StringBuilder outputBuf,
        UIntPtr outputBufSize
    );
}

public static class NewdbAdapter
{
    public static ExecuteResult Execute(IntPtr handle, string command, int outSize = 16384)
    {
        var sb = new StringBuilder(outSize);
        int rc = Native.newdb_session_execute(handle, command, sb, (UIntPtr)outSize);
        int last = Native.newdb_session_last_error(handle);
        IntPtr pErr = Native.newdb_error_code_string(last);
        string err = Marshal.PtrToStringAnsi(pErr) ?? "UNKNOWN";
        string output = sb.ToString();
        return new ExecuteResult(rc, last, err, output, rc == 0);
    }
}

// usage:
// var h = Native.newdb_session_create(@"E:\db\DB\data", "users", @"E:\db\DB\data\newdb.log");
// var r = NewdbAdapter.Execute(h, "COUNT");
// Console.WriteLine(r);
// Native.newdb_session_destroy(h);
```

### 15.3 Node.js 实现（ffi-napi）

```javascript
const ffi = require("ffi-napi");
const ref = require("ref-napi");
const path = require("path");

function createClient(bundleDir) {
  const libPath = process.platform === "win32"
    ? path.join(bundleDir, "newdb.dll")
    : path.join(bundleDir, "libnewdb.so");

  const api = ffi.Library(libPath, {
    newdb_session_create: ["pointer", ["string", "string", "string"]],
    newdb_session_destroy: ["void", ["pointer"]],
    newdb_session_last_error: ["int", ["pointer"]],
    newdb_error_code_string: ["string", ["int"]],
    newdb_session_execute: ["int", ["pointer", "string", "char *", "size_t"]],
  });

  function execute(handle, command, outSize = 16384) {
    const buf = Buffer.alloc(outSize);
    const rc = api.newdb_session_execute(handle, command, buf, outSize);
    const last = api.newdb_session_last_error(handle);
    const errName = api.newdb_error_code_string(last) || "UNKNOWN";
    const zero = buf.indexOf(0);
    const output = (zero >= 0 ? buf.slice(0, zero) : buf).toString("utf8");
    return {
      rc,
      last_error: last,
      error_name: errName,
      output,
      ok: rc === 0,
    };
  }

  return { api, execute };
}

// usage:
// const { api, execute } = createClient("E:/db/DB/build/newdb-vs2026-mt-tests/shared_bundle/Release");
// const h = api.newdb_session_create("E:/db/DB/data", "users", "E:/db/DB/data/newdb.log");
// const r = execute(h, "COUNT");
// console.log(r);
// api.newdb_session_destroy(h);
```

### 15.4 解析器落地建议

- 每个语言项目都统一封装一个 `execute()`，不要在业务代码里直调 FFI。
- 上层业务仅消费 `ExecuteResult`，避免耦合底层缓冲区细节。
- 可在 `ExecuteResult` 增加：
  - `latency_ms`
  - `command_tag`（如 DDL/DML/QUERY/TXN）
  - `truncated`（输出是否截断）

