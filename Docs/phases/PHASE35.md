# 三十五期（PHASE35）：多会话并发写、Compaction 与前台写、GUI/CLI/embed 互斥

## 目标与非目标

**目标**

1. **L0 compaction 与前台写**：将「读 SST、合并、写临时 SST」移出 `StorageEngine::mu_` 临界区，仅在提交 MANIFEST / checkpoint 时短持锁；失败时丢弃临时文件（可重试）。锁序与 **[`POLICY.md`](../POLICY.md) §4.2** 一致：worker 线程仍通过 `drain_pending_l0_compactions` 等 API 持锁提交，**不在**无锁状态下变异 MemTable / WAL。
2. **同进程多会话 / 多线程**：`EmbedClient::submit` 对存储 + journal + 序号整段加互斥，与 **`idem_mu_`** 固定为 **先 `submit_mu_` 再 `idem_mu_`**，避免多线程交错提交；**[`PHASE24.md`](PHASE24.md)** / **§4.3** 的链式事务与「旁路写」约束不变。
3. **跨进程**：可选 **`data_dir/.structdb_exclusive.lock`** 建议锁（`flock` / `LockFileEx`），由 **`EngineConfigSnapshot::exclusive_data_dir_lock`** 与 **`structdb_engine_open_ex`** 开启；第二进程打开失败并返回可读错误（崩溃后锁随进程释放）。
4. **GUI / CLI / embed**：部署规则见根目录 **[`README.md`](../../README.md)** 与 **[`gui/rust_gui/README.md`](../../gui/rust_gui/README.md)**（同一 `data_dir` 下单 WAL 拥有者、与 Tauri 内嵌 REPL 的关系）。

**非目标**（与 **[`PHASE34.md`](PHASE34.md)** 一致方向）

- 不改变 WAL 重放顺序、`commit_seq`、checkpoint 与 MANIFEST 的持久化语义。
- 不引入第二路「随意写」MemTable 线程；不实现跨进程多写者事务。
- 不在三十五期将 L1+ merge 全部改为锁外 I/O（**三十六期**已按与 L0 相同的两阶段模式推广，见 [`PHASE36.md`](PHASE36.md)）。

---

## 权威：L0 两阶段合并（三十五期）

以下相对于 [`src/engine/storage/include/structdb/storage/storage_engine.hpp`](../../src/engine/storage/include/structdb/storage/storage_engine.hpp)。

| 阶段 | 持锁 | 行为 |
|------|------|------|
| Capture | `mu_` | 拷贝 `manifest_.version()`、前两条 L0 的 `relative_path`、`l1_compact_output_from_l0_merge_` 及输出 basename。 |
| Materialize | **无 `mu_`** | 读两个输入 SST、合并、写入 **`_tmp_l0_compact_<nonce>.sst`**。 |
| Commit | `mu_` | 校验 manifest 版本与 L0 前缀未变；`rename` 临时文件为最终 `L0-`/`L1-` 名；写 MANIFEST、删旧 SST、checkpoint、段元数据（与 **[`COMPACTION.md`](../COMPACTION.md)** §1 顺序一致）。校验失败则删临时文件并返回错误（调用方可重试）。 |

---

## C API

- **`structdb_engine_open_ex(data_dir, flags, err, err_len)`**：`flags & STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK` 非零时启用目录建议锁。
- **`structdb_engine_open`**：等价于 `structdb_engine_open_ex(data_dir, 0, err, err_len)`。

---

## 验收命令

```powershell
cmake --build <build_dir> --target structdb_tests
.\build\tests\Release\structdb_tests.exe --gtest_filter="StorageEngine.*:Engine.*:*Phase35*"
```

---

## 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-14 | 初稿：L0 两阶段合并、`EmbedClient::submit` 互斥、可选目录锁、文档与 README 交叉引用 |
