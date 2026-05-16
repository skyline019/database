# 二十七期：C API 完善（PHASE27）

## 目标

- 在 **不改动** 既有 `structdb_run_mdb_file` 符号与默认行为的前提下，补齐 **版本标识、稳定返回码、可扩展运行选项、可选日志出口** 与 **失败路径单测**。
- C 边界 **不抛出 C++ 异常**；未预料的 C++ 异常经捕获后写入 `err` 并映射为粗粒度返回码（与引擎启动类失败同码，见下表）。**`run_mdb_file` 路径**在 embed 已 `open` 后若抛异常，仍会关闭 embed 与引擎；**日志回调**抛异常时由实现吞掉，不穿出边界。

## 头文件与 ABI 约定

- 路径：[`c_api/include/structdb_capi.h`](../src/c_api/include/structdb_capi.h)。
- **版本**：`STRUCTDB_CAPI_VERSION_{MAJOR,MINOR,PATCH}`、`structdb_capi_version()`（`(major<<16)|(minor<<8)|patch`）与 `structdb_capi_version_string()`（语义版本仅描述本 C 头契约，与仓库 `project(StructDB VERSION …)` 独立）。
- **返回码** `enum structdb_capi_rc`（数值稳定，勿改已有含义）：

| 值 | 常量 | 典型含义 |
|----|------|----------|
| 0 | `STRUCTDB_CAPI_OK` | 成功 |
| 1 | `STRUCTDB_CAPI_ERR_NULL_ARG` | 空指针/空路径参数，或 `structdb_mdb_run_options.struct_size` 非 **`STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1`** / **`SIZE_V2`** |
| 2 | `STRUCTDB_CAPI_ERR_ENGINE_STARTUP` | 引擎 `startup` 失败，或未捕获 C++ 异常（实现侧统一映射到此码） |
| 3 | `STRUCTDB_CAPI_ERR_EMBED_OPEN` | 嵌入式会话 `open` 失败 |
| 4 | `STRUCTDB_CAPI_ERR_MDB_RUN` | MDB 脚本执行失败（`err` 含 `last_error` 或占位文案） |
| 5 | `STRUCTDB_CAPI_ERR_STORAGE_IO` | `wal_sync` / `flush_memtable` / `checkpoint` / `embed_save_checkpoint` 失败（`err` 含存储层信息） |

- **`structdb_mdb_run_options`**：首字段 **`struct_size`** 须为 **`STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1`**（48）或 **`STRUCTDB_MDB_RUN_OPTIONS_SIZE_V2`**（56，与 `sizeof(structdb_mdb_run_options)` 一致）；V1 前缀不含末尾 **`repl_exit_requested_out`** 指针，实现会规范化为 V2 默认。
- **`structdb_run_mdb_file_ex(..., opts)`**：`opts == NULL` 等价于历史默认（与单独调用 `structdb_run_mdb_file` 一致）；`structdb_run_mdb_file` 实现为薄转发到 `_ex(..., NULL)`。

## 运行选项与 C++ 映射

字段映射 [`MdbRunOptions`](../src/client/mdb/include/structdb/client/mdb_runner.hpp) 中已有成员：

- `fsync_each_batch`
- `fsync_each_session_txn_op`
- `fail_if_unclosed_txn`
- `allow_persist_while_txn_active_experimental`（默认建议为 true，与 C++ 默认一致）

## 日志

- **`log_file_path`**：非空时，每次运行以 **截断** 方式打开 UTF-8 路径，写入与 MDB `log_sink` 相同的文本行（每行末尾换行由实现写入）。
- **`log_line_cb` + `log_user_data`**：每行 NUL 结尾字符串；**仅在调用 `structdb_run_mdb_file*` 的同一线程**上同步调用。
- **二者可同时设置**：对每一行日志，**先**调用回调，**再**写入文件（若路径有效且文件成功打开）。二者皆空则不在此路径输出脚本行日志（致命错误仍经 `err` 报告）。逐行 REPL 路径下日志文件为 **追加**，见 [`PHASE28.md`](PHASE28.md)。

## 构建形态

- [`c_api/CMakeLists.txt`](../src/c_api/CMakeLists.txt) 默认产出 **静态库** **`structdb_capi`**；**`cmake --install`** 安装静态库归档与 **`structdb_capi.h`**。
- 可选 **`STRUCTDB_BUILD_CAPI_SHARED=ON`** 额外产出 **`structdb_capi_shared`**（导出宏、分发与单测见 [`PHASE28.md`](PHASE28.md)）。

## 回归与单测

- `tests/capi_test.cpp`：`Capi.*` 覆盖版本串、`_ex(NULL)` 与默认路径、空参数、错误 `struct_size`、**V1 `struct_size`**、**`repl_exit_requested_out`（脚本/REPL `EXIT`）**、**`structdb_mdb_execute_line`** 与 **`_ex(..., NULL)`** 等价、**`structdb_embed_get_session_dir_utf8`**、**`structdb_embed_get_session_log_path_utf8`**、**`wal_sync` / `flush_memtable` / `checkpoint` / embed 检查点与序号**、脚本失败、日志文件/回调、以及将 **文件** 当作 `data_dir` 时的启动失败（平台若接受则 `GTEST_SKIP`）。**`tests/structdb_tests.cpp`**：**`EmbedClient.SessionLogOpenCloseAndRotation`**（`session_log.txt` 分隔与轮转）。

## 后续（非本期）

- 仅解析、不执行的 C 绑定（`mdb_parse_command_line`）等见 [`PHASE28.md`](PHASE28.md)「后续」。
