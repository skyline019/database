# 三十六期（PHASE36）：完善多并发

## 与三十五期的关系

[PHASE35.md](PHASE35.md) 已落地 **L0** 两阶段合并（快照 → **`mu_` 外**物化 `_tmp_l0_compact_*.sst` → 锁内校验与提交）、`EmbedClient::submit_mu_`、可选 **`data_dir/.structdb_exclusive.lock`** 与 **`structdb_engine_open_ex`**。PHASE36 将 **L1→L2、L2→L3、L3→L4** 对齐为同一形状，并补充观测、可选 Facade 写队列与 GUI 独占锁开关。

## 存储：L1+ 两阶段合并

- **捕获（持 `mu_`）**：记录 `base_manifest_version`、参与合并的两条 **相对路径** 与在 `sst_entries()` 中的 **`first_idx`**（与既有「层块最前连续两条」规则一致）。
- **物化（不持 `mu_`）**：读双 SST、合并、写 **`_tmp_tier_compact_{src_level}_{ver}_{rand}.sst`**（磁盘前缀以代码为准；早期计划稿曾写 `_tmp_lN_compact_*`，已废弃）。
- **提交（再持 `mu_`）**：校验 manifest 版本与对应两条路径未变；否则删临时文件并返回 **`compact: … (retry)`** 类错误。成功路径：`rename` → `MANIFEST` → 删旧 SST → `lsm_.sync_from_manifest` → checkpoint / 段元数据（顺序见 [COMPACTION.md](../COMPACTION.md) §1）。
- **公开 API**：`compact_merge_two_oldest_l1_to_l2` / `l2_to_l3` / `l3_to_l4` 使用 **`std::unique_lock(mu_)`** 调用内部 **`*_with_relock_`**（与 L0 的 `compact_merge_two_oldest_l0_with_relock_` 对称）。

## 读并发（本期结论：维持 `std::mutex`，`shared_mutex` 延期）

- **现状**：`get` / `visit_prefix` 与写路径共用 **`StorageEngine::mu_`**；**`MemTable`** 另有内部互斥，但 **manifest / LSM 视图 / WAL 段元数据** 的更新均在持 **`mu_`** 的写路径下完成。
- **预研**：若将 **`mu_`** 换为 **`std::shared_mutex`**，须在持共享锁期间保证 **仅读取** 的 manifest 视图与 SST 列表不被写路径并发替换；且 **`visit_prefix`** 对 memtable 的排序遍历与 SST 扫描须与 flush/compaction 的 manifest 更新建立明确 happens-before。**本期不切换**，避免在未单独评审的不变式下引入数据竞争；后续若启用，须同步 [POLICY.md](../POLICY.md) §4.2 与回归用例。

## Facade：可选有界 `kv_put` 队列（默认关）

- **`EngineConfigSnapshot::kv_put_async_queue_depth`**：`0`（默认）表示 **`kv_put` 直接调用 `storage_->put`**；**`>0`** 时由 **单后台线程**顺序执行 `put`，队列中 **等待**任务数达到该上限时 **`kv_put` 返回 `false`**（仍保持单写者语义，不并行写 MemTable）。
- **观测**：`StoragePressureSnapshot::{facade_kv_put_queue_depth,facade_kv_put_queue_cap}` 由 `Engine::storage_pressure_snapshot` 在读取存储压力后填充（与 [PHASE21.md](PHASE21.md) 背压叙事对齐的可观测字段）。

## GUI / C API：环境变量独占锁

- Tauri 宿主在设置 **`STRUCTDB_GUI_EXCLUSIVE_DIR_LOCK=1` / `true`** 时，以 **`structdb_engine_open_ex(..., STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK)`** 打开引擎；默认 **关闭**，避免本机双窗调试时第二实例立刻失败。详见 [gui/rust_gui/README.md](../../gui/rust_gui/README.md)。

## 锁序与交叉引用

| 区域 | 锁 / 顺序 |
|------|-----------|
| L0 / L1+ compaction | `unique_lock(mu_)` → 快照 → `unlock` → I/O → `lock` → 校验 → MANIFEST / checkpoint |
| `EmbedClient::submit` | `idem_mu_` → `submit_mu_`（见 PHASE35） |
| 可选目录锁 | `open`..`close` 持有 `.structdb_exclusive.lock`（PHASE35） |

- **方针**：[POLICY.md](../POLICY.md) §4.2–§4.3、[COMPACTION.md](../COMPACTION.md)。

## 验收

```text
structdb_tests --gtest_filter=*Phase36*:StorageEngine.*:Engine.*
```

或全量 `structdb_tests` / `capi_test`（见根 `README` 与 `POLICY` §6）。
