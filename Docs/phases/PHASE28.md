# 二十八期：C API 共享库与 FFI 会话（PHASE28）

## 目标

- 默认 **静态** `structdb_capi` 不变；根 CMake 默认另建 **`structdb_capi_shared`**（`STRUCTDB_BUILD_CAPI_SHARED` 默认 **ON**，可设 **OFF** 跳过），产物名 **`structdb_capi_shared`**（Windows：`structdb_capi_shared.dll` + import `.lib`；Unix：`libstructdb_capi_shared.so`），避免与静态 `structdb_capi.lib` / `libstructdb_capi.a` 同名冲突。
- 头文件 **`STRUCTDB_CAPI_EXPORT`**：编译共享库时定义 **`STRUCTDB_CAPI_BUILDING`**；链接共享库的目标通过 **`structdb_capi_shared` 的 `INTERFACE`** 自动带上 **`STRUCTDB_CAPI_SHARED`**（Windows `dllimport`）。仅静态链接时不定义二者即可。
- **会话型纯 C API**（不透明句柄）：`structdb_engine_open` / `shutdown`、`structdb_embed_open` / **`structdb_embed_get_session_dir_utf8`** / `close`、`structdb_mdb_session_create` / `destroy` / `reset`、`structdb_mdb_execute_line_ex`，以及薄包装 **`structdb_mdb_execute_line`**（等价于 `_ex(..., NULL)`，与 `structdb_run_mdb_file` / `_ex` 对称）；语义对齐 C++ [`mdb_repl_execute_line`](../src/client/mdb/include/structdb/client/mdb_runner.hpp)，与 `run_mdb_script` 同源 MDB 指令面。**持久化对话 / 未提交事务** 约定与 **`run_mdb_file*` 删除 `session.txn`** 见头文件 **`structdb_capi.h`** 内 **「持久化对话与持久化事务」**。
- **日志**：`structdb_mdb_execute_line_ex` 使用与 `structdb_mdb_run_options` 相同的回调/文件字段；**文件路径为追加**（每行调用一次）；`structdb_run_mdb_file_ex` 仍为 **截断**（每 run 一次）。`fail_if_unclosed_txn` 在逐行 API 中忽略。

## 健壮性与 ABI 注意

- **`run_mdb_script` / 日志路径抛异常**：实现保证在 `EmbedClient::open` 成功后，若后续抛异常，仍会 **`close`** embed 并 **`shutdown`** 引擎，避免句柄泄漏。
- **`emit_logs`**：用户 **`log_line_cb`** 或文件写入抛异常时 **吞掉**，不穿透 C 边界（避免部分日志后进程异常状态）。
- **`structdb_mdb_execute_line_ex`**：`client` 必须与 **`engine->active_embed`** 一致（当前唯一合法打开的 embed），否则返回 **`STRUCTDB_CAPI_ERR_EMBED_OPEN`**。
- **版本字符串**：`structdb_capi_version_string()` 与 **`STRUCTDB_CAPI_VERSION_*`** 由同一组宏在 `.cpp` 展开生成，避免手改漂移（当前契约 **1.7.0**：**`NULL`/`""` 的 `data_dir`/`session_dir`** → **`cwd/_data`**、**`{data_dir}/embed_session`**；**`structdb_embed_get_session_dir_utf8`** / **`structdb_embed_get_session_log_path_utf8`** 便于 **`.so` / `.dll`** FFI 固定持久化路径；**`session_log.txt`**（**`SESSION_OPEN`/`SESSION_CLOSE`**，轮转 **`session_log.arch.*`**）；头文件内 **持久化对话与持久化事务** 与 **`run_mdb_file*` 清 `session.txn`** 说明；Windows 上 Facade **`Engine::startup`** 对 **`snap.data_dir`** 使用 **`u8path`**，与 C API UTF-8 一致）。

## 线程与生命周期

- **`structdb_engine_shutdown`**：若仍有打开的 embed，会先 **`structdb_embed_close`** 再 `Engine::shutdown`（与「须先 close」等价，避免泄漏）。
- 同一 **`structdb_engine*`** 上 **至多一个** 打开的 embed；第二次 **`structdb_embed_open`** 返回 `NULL` 并写入 `err`。
- `structdb_embed_client` 必须与创建它的 **`structdb_engine*`** 配对使用；交叉传入返回 **`STRUCTDB_CAPI_ERR_NULL_ARG`**。

## MSVC 与运行库

- 根选项 **`STRUCTDB_STATIC_MSVC_RUNTIME`**（默认 ON，`/MT`）下，宿主若动态加载 **另一 CRT** 的 DLL 可能出现堆不一致。对外分发 FFI 用共享库时，建议单独用 **`/MD`** 配置构建矩阵，并在发行说明中写明。

## 构建与安装

```text
cmake -B build
cmake --build build --config Release
ctest --test-dir build -C Release
```

（默认已含共享库与 **`structdb_capi_shared_smoke`**；若只要静态 C API，加 **`-DSTRUCTDB_BUILD_CAPI_SHARED=OFF`**。）

**`cmake --install`**：始终安装 **静态** **`structdb_capi`** 与 **`structdb_capi.h`**；若开启共享目标，另安装 **`structdb_capi_shared`**（见 [`c_api/CMakeLists.txt`](../src/c_api/CMakeLists.txt)）。非 MSVC 下对 **`structdb_capi`**（静态）与 **`structdb_capi_shared`** 启用 **`POSITION_INDEPENDENT_CODE`**，便于将静态对象链入 `.so`。

## 回归

- `structdb_tests`：`Capi.*`（含会话多行、embed/engine 校验、**重复 embed open**、**shutdown 自动关 embed**、**V1 选项**、**`repl_exit_requested_out`**、**`structdb_mdb_execute_line`** 与 **`_ex(NULL)`** 等价、**`structdb_embed_get_session_dir_utf8`** / **`structdb_embed_get_session_log_path_utf8`**、**`wal_sync` / `flush_memtable` / `checkpoint` / embed `save_checkpoint` 与 seq 查询）、**`EmbedClient.SessionLogOpenCloseAndRotation`**。
- 默认 **`STRUCTDB_BUILD_CAPI_SHARED=ON`** 时 **`ctest`** 另注册 **`structdb_capi_shared_smoke`**（链 `structdb_capi_shared`，Windows 将 DLL 复制到测试可执行目录）。

## 后续

- 仅解析、不执行的 C 绑定（`mdb_parse_command_line`）可作为 **28B**。
- 更细的会话状态查询、错误码拆分等按需迭代。
