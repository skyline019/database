#ifndef STRUCTDB_CAPI_H
#define STRUCTDB_CAPI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Shared library (FFI): building the DLL defines STRUCTDB_CAPI_BUILDING; consumers link import lib + define via CMake INTERFACE STRUCTDB_CAPI_SHARED. Static library: leave both unset. */
#if defined(_WIN32) || defined(__CYGWIN__)
#ifdef STRUCTDB_CAPI_BUILDING
#define STRUCTDB_CAPI_EXPORT __declspec(dllexport)
#elif defined(STRUCTDB_CAPI_SHARED)
#define STRUCTDB_CAPI_EXPORT __declspec(dllimport)
#else
#define STRUCTDB_CAPI_EXPORT
#endif
#else
#if defined(STRUCTDB_CAPI_BUILDING) && (defined(__GNUC__) || defined(__clang__))
#define STRUCTDB_CAPI_EXPORT __attribute__((visibility("default")))
#else
#define STRUCTDB_CAPI_EXPORT
#endif
#endif

/// C API 语义版本（与仓库 `project(StructDB VERSION …)` 独立，仅表示本头文件契约）。
/// 更新此处时须同步 GUI 版本：`gui/rust_gui` 下 `npm run sync-version-from-capi`（`predev`/`prebuild` 会自动执行）。
#define STRUCTDB_CAPI_VERSION_MAJOR 1
#define STRUCTDB_CAPI_VERSION_MINOR 8
#define STRUCTDB_CAPI_VERSION_PATCH 0

/// 当 **`structdb_engine_open`** / **`structdb_run_mdb_file*`** 的 **`data_dir`** 为 **`NULL` 或 `""`** 时，引擎数据目录为
/// **`absolute(current_path() / STRUCTDB_CAPI_DEFAULT_STORE_DIR / "_data")`**，避免 WAL / manifest / SST 等直接落在进程 **cwd** 根下。
/// 与 **`structdb_app`**（仍可使用其自身的 **`_data`** 布局）不同；显式传入非空 **`data_dir`** 时本宏不参与解析。
#define STRUCTDB_CAPI_DEFAULT_STORE_DIR "_structdb_capi"

/// `structdb_mdb_run_options` 历史布局大小（末尾无 `repl_exit_requested_out`）；与 **`STRUCTDB_MDB_RUN_OPTIONS_SIZE_V2`** 均被实现接受。
#define STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1 48
#define STRUCTDB_MDB_RUN_OPTIONS_SIZE_V2 56
/// V3 在 V2 后追加 **`long_task_control`**（`structdb_long_task_control*`，可为 `NULL`）。
#define STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3 64

/// 打包版本号：`(major << 16) | (minor << 8) | patch`，与三宏一致，便于 FFI 数值比较。
STRUCTDB_CAPI_EXPORT uint32_t structdb_capi_version(void);

/// 形如 `"1.8.0"` 的版本字符串（静态存储期，勿释放）；与 `STRUCTDB_CAPI_VERSION_*` 同步。
STRUCTDB_CAPI_EXPORT const char* structdb_capi_version_string(void);

/// `structdb_run_mdb_file` / `structdb_run_mdb_file_ex` / 会话 API 返回值（稳定数值，勿改已有含义）。
/// 所有入口 **非线程安全**：同一 `structdb_engine*` / 会话不得跨线程并发调用（与 Facade/Embed 单写者模型一致）。
enum structdb_capi_rc {
  STRUCTDB_CAPI_OK = 0,
  STRUCTDB_CAPI_ERR_NULL_ARG = 1,
  STRUCTDB_CAPI_ERR_ENGINE_STARTUP = 2,
  STRUCTDB_CAPI_ERR_EMBED_OPEN = 3,
  STRUCTDB_CAPI_ERR_MDB_RUN = 4,
  STRUCTDB_CAPI_ERR_STORAGE_IO = 5, /**< `wal_sync` / `flush_memtable` / `checkpoint` / `embed_save_checkpoint` 失败 */
  STRUCTDB_CAPI_ERR_CANCELLED = 6,  /**< 长任务协作取消（`structdb_long_task_control` / 脚本批处理） */
};

/// ### 持久化对话与持久化事务（DLL / FFI）
///
/// - **对话（嵌入会话）**：固定 **`data_dir`**（`structdb_engine_open`）与 **`session_dir`**（`structdb_embed_open`）。`EmbedClient` 在 **`session_dir/_structdb_embed/`** 下维护 **`session.journal`**、**`session.ckpt`**、**`session_log.txt`**、**`session.txn`** 等，避免与会话根目录下宿主自有文件混杂；若曾使用旧布局（上述文件直接在 **`session_dir`** 根下），首次 **`open`** 会将它们迁入 **`_structdb_embed/`**。进程退出后再次 **`structdb_embed_open(…, 同一 session_dir, …)`** 即恢复同一会话目录。进程内 **`structdb_mdb_session_create`** 每次新建即可；**`structdb_mdb_session_reset`** 仅清内存态，不代替磁盘布局。**`session_log.txt`**：每次成功 **`open`** 写入带 UTC 的 **`SESSION_OPEN`** 分隔记录，**`close`** 写 **`SESSION_CLOSE`**；超过大小阈值轮转 **`session_log.arch.*`** 并修剪归档数量以防膨胀（与 **`session.journal`** 分离）。
/// - **未提交 MDB 事务（`session.txn`）**：须通过 **`structdb_mdb_execute_line`** / **`structdb_mdb_execute_line_ex`**（REPL 路径）。**首条**成功进入执行路径的命令会尝试从 **`session_dir/_structdb_embed/session.txn`** 恢复；`opts->fsync_each_session_txn_op` 非 0 时事务日志追加可更强落盘。引擎侧 **`BEGIN` 内存储写** 等见仓库 **`POLICY`** / **`mdb_persist_in_begin`**。
/// - **`structdb_run_mdb_file` / `structdb_run_mdb_file_ex`**：内部为 **`run_mdb_script`**，**每次运行开始会删除 `session_dir/_structdb_embed/session.txn`**（并兼容删除根目录遗留 **`session.txn`**），**不**用于「跨运行续未提交 REPL 事务」；仅适合批处理脚本。
/// - **查询已打开会话路径**：**`structdb_embed_get_session_dir_utf8`** 返回 **`structdb_embed_open`** 实际使用的目录（UTF-8），便于宿主持久化配置。**`structdb_embed_get_session_log_path_utf8`** 返回 **`session_dir/_structdb_embed/session_log.txt`** 的绝对路径（UTF-8）。
///
/// ### 崩溃加固、响应与确认（宿主显式调用）
///
/// - **MDB 行级**：**`structdb_mdb_run_options`** 中 **`fsync_each_batch`**（经 `persist_table` → 批次 WAL **`wal_sync`** 边界）、**`fsync_each_session_txn_op`**（**`session.txn`** v2 追加后 fsync）与引擎配置 **`fsync_every_write`** / **`mdb_persist_in_begin`** 等组合使用；详见仓库 **`Docs/TXN_INNODB_MAP.md`**、**`POLICY`**。
/// - **引擎存储**：**`structdb_engine_wal_sync`** 仅刷 WAL；**`structdb_engine_flush_memtable`** 写 SST + 检查点（重操作）；**`structdb_engine_checkpoint`** 仅检查点路径（与运行时算子同名语义）。失败返回 **`STRUCTDB_CAPI_ERR_STORAGE_IO`**。
/// - **嵌入会话确认**：**`structdb_embed_save_checkpoint`** 将会话 journal 的 **`last_ack_seq`** 与引擎检查点指针落盘（**`EmbedClient::save_checkpoint`**）。**`structdb_embed_last_ack_seq`** / **`structdb_embed_next_seq`** / **`structdb_embed_read_snapshot_seq`** 供宿主在关键业务点后读取水位，自行持久化以实现端到端「已确认」语义；**`structdb_engine_latest_commit_seq`** 为引擎 KV 提交序号。

/// 每行脚本日志回调（与 `run_mdb_script` / REPL 的 `log_sink` 一致：含 `[COUNT]` 等）。`line` 以 NUL 结尾；**仅在调用本库 MDB 入口的线程上**同步调用；**不得在回调内再次调用**这些入口。
typedef void (*structdb_mdb_log_line_fn)(void* user_data, const char* line);

typedef struct structdb_long_task_control structdb_long_task_control;

/// 与 C++ `LongTaskProgressSnapshot` / GUI `long-task-progress` 对齐（字符串指针仅在回调期间有效）。
typedef struct structdb_long_task_progress {
  const char* kind;    /**< camelCase，如 `mdbScript` / `compactionMerge` */
  const char* status;  /**< `running` / `cancelled` / … */
  uint64_t units_done;
  uint64_t units_total;
  uint64_t bytes_done;
  uint64_t bytes_total;
  const char* detail;
  const char* task_id;
} structdb_long_task_progress;

typedef void (*structdb_long_task_progress_fn)(void* user_data, const structdb_long_task_progress* progress);

/// 创建协作取消 + 进度上报句柄（线程不安全：同一控制块勿跨线程并发使用）。
STRUCTDB_CAPI_EXPORT structdb_long_task_control* structdb_long_task_control_create(void);
STRUCTDB_CAPI_EXPORT void structdb_long_task_control_destroy(structdb_long_task_control* ctrl);
/// 清除取消标志与 reporter 状态（不销毁回调注册）。
STRUCTDB_CAPI_EXPORT void structdb_long_task_control_reset(structdb_long_task_control* ctrl);
STRUCTDB_CAPI_EXPORT void structdb_long_task_control_request_cancel(structdb_long_task_control* ctrl);
STRUCTDB_CAPI_EXPORT int structdb_long_task_control_cancel_requested(const structdb_long_task_control* ctrl);
STRUCTDB_CAPI_EXPORT void structdb_long_task_control_set_progress_callback(structdb_long_task_control* ctrl,
                                                                           structdb_long_task_progress_fn cb,
                                                                           void* user_data);

/// 可选运行参数。`struct_size` 须为 **`STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1`**（48）、**`SIZE_V2`**（56）或 **`SIZE_V3`**（64）；**V2** 在 V1 字段后追加 **`repl_exit_requested_out`**；**V3** 在 V2 后追加 **`long_task_control`**。实现将规范化为 V3 默认填充。
///
/// **日志**：`log_line_cb` 与 `log_file_path` 可同时非空：每行先调回调，再写入文件（路径为 **UTF-8**）。`structdb_run_mdb_file_ex` 打开日志文件时 **截断**（每 run 一次）。`structdb_mdb_execute_line_ex` 对同一 `log_file_path` 为 **追加**（每行调用一次）。
///
/// **`repl_exit_requested_out`**：若非 `NULL`，在 **`structdb_run_mdb_file_ex`** 成功返回前写入 **`run_mdb_script` 结束时的 `repl_exit_requested`**（0/1）；在 **`structdb_mdb_execute_line_ex`** 每行成功后写入 **该行** 的 `repl_exit_requested`。
typedef struct structdb_mdb_run_options {
  uint32_t struct_size;
  uint32_t reserved_flags; /**< reserved, set to 0 */

  int fsync_each_batch; /**< 非 0 为 true */
  int fsync_each_session_txn_op; /**< 非 0：REPL 下 `session.txn` v2 追加后可选 fsync，利于跨进程恢复未提交事务 */
  int fail_if_unclosed_txn; /**< `structdb_mdb_execute_line_ex` 忽略本字段 */
  int allow_persist_while_txn_active_experimental; /**< 非 0 为 true；默认建议 1，与 C++ `MdbRunOptions` 默认一致 */

  const char* log_file_path; /**< UTF-8 NUL 结尾，可选 */

  structdb_mdb_log_line_fn log_line_cb;
  void* log_user_data;

  int* repl_exit_requested_out; /**< V2：可选；每行/每脚本结束后写入 0 或 1 */
  structdb_long_task_control* long_task_control; /**< V3：可选；脚本/存储长任务共享取消与进度 */
} structdb_mdb_run_options;

typedef struct structdb_engine structdb_engine;
typedef struct structdb_embed_client structdb_embed_client;
typedef struct structdb_mdb_session structdb_mdb_session;

/// 将 `ctrl` 的 reporter 绑定到引擎存储（compaction merge / worker 进度）。`ctrl == NULL` 清除绑定。
STRUCTDB_CAPI_EXPORT void structdb_engine_bind_long_task(structdb_engine* engine, structdb_long_task_control* ctrl);

/// REPL / GUI 多行脚本批处理：在 `structdb_mdb_execute_line_ex` 循环前后调用；`executable_line_total` 为可执行行数。
STRUCTDB_CAPI_EXPORT void structdb_engine_begin_mdb_script_batch(structdb_engine* engine,
                                                                 structdb_long_task_control* ctrl,
                                                                 uint64_t executable_line_total);
STRUCTDB_CAPI_EXPORT void structdb_engine_end_mdb_script_batch(structdb_engine* engine);

/// `structdb_engine_open_ex`：`open_flags & STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK` 非零时，在 `data_dir` 上持有建议文件锁（见 `Docs/phases/PHASE35.md`）。
#define STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK 1u

/// 创建引擎并完成 `startup`。失败返回 `NULL` 并写入 `err`（若 `err_len>0`）。
/// **`data_dir`**：非空时为 **UTF-8** 目录（Windows 下经 `u8path` 打开，与 `EngineConfigSnapshot` 一致）。**`NULL` 或 `""`** 时自动使用 **`absolute(current_path() / STRUCTDB_CAPI_DEFAULT_STORE_DIR / "_data")`**（见 **`STRUCTDB_CAPI_DEFAULT_STORE_DIR`**；宿主进程 cwd 决定落点）。
STRUCTDB_CAPI_EXPORT structdb_engine* structdb_engine_open_ex(const char* data_dir, uint32_t open_flags, char* err,
                                                                size_t err_len);

/// 等价于 **`structdb_engine_open_ex(data_dir, 0, err, err_len)`**。
STRUCTDB_CAPI_EXPORT structdb_engine* structdb_engine_open(const char* data_dir, char* err, size_t err_len);

/// 写入引擎打开时解析后的 **`data_dir`**（UTF-8）。`out` 可为 `NULL` 仅查询长度。成功返回应写入的字节数（**不含** NUL）；若 `out != NULL` 且 `out_cap` 不足则写入截断的 NUL 结尾前缀并返回完整长度。`engine == NULL` 时返回 0。
STRUCTDB_CAPI_EXPORT size_t structdb_engine_get_data_dir_utf8(structdb_engine* engine, char* out, size_t out_cap);

/// 将 **`StorageEngine::wal_sync`** 刷盘（WAL 边界）。`engine` 未启动或 **`storage()`** 不可用时返回 **`STRUCTDB_CAPI_ERR_NULL_ARG`** 或 **`STRUCTDB_CAPI_ERR_STORAGE_IO`**。
STRUCTDB_CAPI_EXPORT int structdb_engine_wal_sync(structdb_engine* engine, char* err, size_t err_len);

/// **`flush_memtable`**：MemTable → SST + 检查点（可能触发 L0 合并等，成本高）。失败 **`STRUCTDB_CAPI_ERR_STORAGE_IO`**。
STRUCTDB_CAPI_EXPORT int structdb_engine_flush_memtable(structdb_engine* engine, char* err, size_t err_len);

/// 仅 **`StorageEngine::checkpoint`**（不写新 SST）。失败 **`STRUCTDB_CAPI_ERR_STORAGE_IO`**。
STRUCTDB_CAPI_EXPORT int structdb_engine_checkpoint(structdb_engine* engine, char* err, size_t err_len);

/// 引擎当前 **`latest_commit_seq()`**；`engine == NULL` 或未 **`startup`** 时返回 **0**。
STRUCTDB_CAPI_EXPORT uint64_t structdb_engine_latest_commit_seq(structdb_engine* engine);

/// 解析默认 **workspace / `absolute(workspace/STRUCTDB_CAPI_DEFAULT_STORE_DIR/_data)` / `{data_dir}/embed_session`** 布局（UTF-8 字符串；与 C API 省略 **`data_dir`** / **`session_dir`** 时的解析一致）。
/// - **`workspace_utf8`**：`NULL` 或 `""` 表示 **`current_path()`**；否则为 UTF-8 目录。
/// - **`workspace_out` / `data_dir_utf8_out` / `session_dir_utf8_out`**：可为 `NULL` 跳过该项；至少一项非 `NULL`。
/// 供宿主在链 **`.so` / `.dll`** 的 FFI 中预先取得持久化路径，使跨进程/跨版本重启仍能打开同一库与同一会话目录。
STRUCTDB_CAPI_EXPORT int structdb_capi_get_default_paths(const char* workspace_utf8, char* workspace_out, size_t workspace_cap,
                                                         char* data_dir_utf8_out, size_t data_dir_cap,
                                                         char* session_dir_utf8_out, size_t session_dir_cap, char* err,
                                                         size_t err_len);

/// `shutdown` 并释放。`NULL` 安全。若仍有打开的 embed，会先 **`structdb_embed_close`** 再 `Engine::shutdown`。
STRUCTDB_CAPI_EXPORT void structdb_engine_shutdown(structdb_engine* engine);

/// 在已启动的 `engine` 上打开嵌入式会话目录。失败返回 `NULL`。同一 `engine` 上 **同时只能有一个** 打开的 embed；重复 `open` 返回 `NULL` 且 `err` 提示。
/// **`session_dir`**：非空 UTF-8。**`NULL` 或 `""`** 时使用 **`{engine_data_dir}/embed_session`**（`engine_data_dir` 为 `structdb_engine_open` 解析后的路径，见 **`structdb_engine_get_data_dir_utf8`**）。
STRUCTDB_CAPI_EXPORT structdb_embed_client* structdb_embed_open(structdb_engine* engine, const char* session_dir,
                                                                char* err, size_t err_len);

/// 写入 **`structdb_embed_open`** 成功后实际使用的 **`session_dir`**（UTF-8，绝对路径语义与实现一致）。`out` 可为 `NULL` 仅查询长度。规则同 **`structdb_engine_get_data_dir_utf8`**。`client == NULL`、尚未 `open` 成功或已 `close` 的句柄返回 **0**。
STRUCTDB_CAPI_EXPORT size_t structdb_embed_get_session_dir_utf8(structdb_embed_client* client, char* out, size_t out_cap);

/// **`session_dir/_structdb_embed/session_log.txt`**（会话活动日志，UTF-8 路径）。`out` 可为 `NULL` 仅查询长度。规则同上。未成功 `open` 时返回 **0**。
STRUCTDB_CAPI_EXPORT size_t structdb_embed_get_session_log_path_utf8(structdb_embed_client* client, char* out, size_t out_cap);

/// **`EmbedClient::save_checkpoint`**：持久化 embed journal 已确认序号与引擎检查点指针。失败 **`STRUCTDB_CAPI_ERR_STORAGE_IO`**。
STRUCTDB_CAPI_EXPORT int structdb_embed_save_checkpoint(structdb_embed_client* client, char* err, size_t err_len);

/// 嵌入 journal 已确认的最大序号（无已打开 embed 时返回 **0**）。
STRUCTDB_CAPI_EXPORT uint64_t structdb_embed_last_ack_seq(structdb_embed_client* client);
/// 下一条 journal 序号（**≥ 1**；未打开时 **0**）。
STRUCTDB_CAPI_EXPORT uint64_t structdb_embed_next_seq(structdb_embed_client* client);
/// 当前 MVCC 读快照序号（未打开或未刷新时可能为 **UINT64_MAX** 语义，与 C++ 一致）。
STRUCTDB_CAPI_EXPORT uint64_t structdb_embed_read_snapshot_seq(structdb_embed_client* client);

STRUCTDB_CAPI_EXPORT void structdb_embed_close(structdb_embed_client* client);

STRUCTDB_CAPI_EXPORT structdb_mdb_session* structdb_mdb_session_create(void);
STRUCTDB_CAPI_EXPORT void structdb_mdb_session_destroy(structdb_mdb_session* session);
/// 仅重置进程内 REPL 计数/恢复标志等；**不**删除磁盘上 **`session_dir`** 内文件；跨重启续对话/未提交事务仍靠固定 **`session_dir`** + **`structdb_mdb_execute_line*`**。
STRUCTDB_CAPI_EXPORT void structdb_mdb_session_reset(structdb_mdb_session* session);

/// 执行一行 MDB（`mdb_repl_execute_line`）。**持久化对话 / 未提交事务** 须用本组 API + 固定 **`session_dir`**，见上文 **「持久化对话与持久化事务」**。
/// `opts` 为 `NULL` 时等价于默认 REPL 标志与 `allow_persist_while_txn_active_experimental=true`。
/// `EXIT`/`QUIT` 成功时仍返回 **`STRUCTDB_CAPI_OK`**；若 `opts->repl_exit_requested_out` 非 `NULL`（V2 选项），则同时写入 **1**。
STRUCTDB_CAPI_EXPORT int structdb_mdb_execute_line_ex(structdb_engine* engine, structdb_embed_client* client,
                                                        structdb_mdb_session* session, const char* line, char* err,
                                                        size_t err_len, const structdb_mdb_run_options* opts);

/// 等价于 **`structdb_mdb_execute_line_ex(..., err, err_len, NULL)`**；与 **`structdb_run_mdb_file`** 相对 **`_ex`** 的薄包装对称，便于纯 C/FFI 调用方省略选项指针。
STRUCTDB_CAPI_EXPORT int structdb_mdb_execute_line(structdb_engine* engine, structdb_embed_client* client,
                                                   structdb_mdb_session* session, const char* line, char* err,
                                                   size_t err_len);

/// **`mdb_path`** 须为非空 UTF-8 路径（脚本文件）。`opts` 为 `NULL` 时等价于历史默认。**`err`** 可为 `NULL`（`err_len==0` 时不写）。
/// **`data_dir`**：`NULL` 或 `""` → **`absolute(cwd / STRUCTDB_CAPI_DEFAULT_STORE_DIR / "_data")`**。**`session_dir`**：`NULL` 或 `""` → **`{resolved_data_dir}/embed_session`**。
/// **注意**：脚本路径 **每次运行会删除 `session_dir/_structdb_embed/session.txn`**（及兼容删除根目录遗留 **`session.txn`**），与 **持久化未提交 REPL 事务** 互斥；续事务请用 **`structdb_mdb_execute_line*`**。
STRUCTDB_CAPI_EXPORT int structdb_run_mdb_file_ex(const char* data_dir, const char* session_dir, const char* mdb_path, char* err,
                                                  size_t err_len, const structdb_mdb_run_options* opts);

/// 使用嵌入式会话对给定 **`data_dir`** 运行 **`.mdb`** 脚本；等价于 **`structdb_run_mdb_file_ex(..., NULL)`**。
/// 成功返回 **0**，否则为非零 **`enum structdb_capi_rc`**（**`err` / `err_len`** 规则同 **`structdb_run_mdb_file_ex`**）。
/// **`data_dir` / `session_dir`**：`NULL` 或 `""` 时与 **`structdb_run_mdb_file_ex`** 相同默认解析。
STRUCTDB_CAPI_EXPORT int structdb_run_mdb_file(const char* data_dir, const char* session_dir, const char* mdb_path, char* err,
                                                 size_t err_len);

/// `structdb_mdb_session_create` 在极端 OOM 时可能返回 `NULL`；其余入口对 `NULL` 句柄为 no-op 或返回 `STRUCTDB_CAPI_ERR_NULL_ARG`。

#ifdef __cplusplus
}
#endif

#endif
