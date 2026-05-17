# PHASE43：RECOVER TO CHECKPOINT_SEQ（Wave 3）

在 PHASE20 多段 WAL 与双槽 checkpoint 基础上，追加 **`checkpoint.chain`** 侧车链，支持 **按 checkpoint 序号** 的破坏性 PITR（非墙钟时间）。

## 1. 非目标

- `RECOVER TO TIME` / `RECOVER TO LSN`（仍 `[NOT_SUPPORTED]`，见 PHASE25 §25G）
- 在线热回滚、跨 `data_dir` 合并
- 自动回滚已 flush 进 SST 的数据（仅截断 **WAL 尾**；SST 不变）

## 2. `checkpoint.chain`

每次 `CheckpointWriter::write_rotating` 成功后 append 一行：

```text
seq wal_offset redo_offset manifest_version mdb_catalog_epoch undo_prefix written_unix_ns
```

文件：`data_dir/checkpoint.chain`。

## 3. `RECOVER TO CHECKPOINT_SEQ n`

1. **必须**先 `Engine::shutdown()`（MDB 动词会 `client.close()` 后调用 `Engine::recover_to_checkpoint_seq`）。
2. 在 chain 中查找 `n`；`n` 不得大于当前 `read_latest` 的 seq。
3. `resize_file(wal.log, wal_offset)`；`write_recovery_checkpoint` 写双槽 + legacy。
4. 截断 chain 中 `seq > n` 的行。
5. 重新 `startup` / `open` 后按 WAL 重放。

**MDB**：`RECOVER TO CHECKPOINT_SEQ n` → 提示重启会话。  
**API**：`structdb::facade::Engine::recover_to_checkpoint_seq(n)`（storage 已释放时）。

## 4. `SHOW CHECKPOINTS`

列出 chain 中各 `[CHECKPOINT] seq=… wal_offset=…`。

## 5. 与 undo / trim 互斥

- 恢复后 `undo.log` **不**自动截断；见 [`CHECKPOINT_UNDO_PREFIX.md`](CHECKPOINT_UNDO_PREFIX.md)。
- 操作前建议 [`BACKUP_RESTORE_RUNBOOK.md`](../BACKUP_RESTORE_RUNBOOK.md) 冷备。

## 6. 回归

```text
structdb_tests --gtest_filter=StorageEngine.Phase43*:Mdb.Phase43*
```

| 测试 | 覆盖 |
|------|------|
| `StorageEngine.Phase43CheckpointChainAppendOnRotate` | chain 追加 |
| `StorageEngine.Phase43RecoverWalOnlyRowVisible` | WAL 仅第二行 + recover |
| `Mdb.Phase43RecoverCheckpointSeqSmoke` | SHOW CHECKPOINTS + recover API |

## 7. 运维清单（Wave 4）

1. 冷备：`scripts/backup_bundle.ps1` 或 `structdb_app --backup-bundle`（含 `checkpoint.chain`）。
2. 停写后：`structdb_app --data-dir <dir> --recover-to-checkpoint-seq <n>` 或 `scripts/recover_to_checkpoint.ps1`。
3. 重启引擎；`COUNT` / `SHOW SNAPSHOT` 验收。
4. 启动可选严格链校验：`checkpoint_chain_strict=true`（配置快照）。

## 8. 回归（Wave 4 追加）

| 测试 | 覆盖 |
|------|------|
| `StorageEngine.Phase43ChainValidateMismatchStrict` | 严格校验失败 |
| `Capi.Phase45RecoverToCheckpointSeq` | `structdb_recover_data_dir_to_checkpoint_seq` |

## 9. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-16 | 初稿：chain、recover、SHOW CHECKPOINTS |
| 2026-05-16 | Wave 4：chain validate、app 恢复 CLI、Runbook 清单 |
