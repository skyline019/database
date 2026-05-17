# StructDB 冷备份与恢复 Runbook

本文档实现 **[`COMPETITIVE_IMPROVEMENT_PLAN.md`](COMPETITIVE_IMPROVEMENT_PLAN.md)** I-OPS：在 **停写** 前提下复制 [`POLICY.md`](POLICY.md) §4.0 保底目录，恢复后验证 WAL/journal 重放与行数。

**不承诺** SQLite 式单文件 `.db`；备份产物为 **`data_dir` + `session_dir` 目录树**。

---

## 1. 前置条件

- **停止一切** 指向该 `data_dir` 的进程（`structdb_app`、GUI、测试残留）。
- 推荐开启独占锁（见 §5）：`STRUCTDB_GUI_EXCLUSIVE_DIR_LOCK=1` 或 `structdb_engine_open_ex(..., EXCLUSIVE_DIR_LOCK)`，避免备份期间第二写者。
- 确认无未完成的 OS 缓存写入（备份前正常 `EXIT` / 进程退出即可）。

---

## 2. 冷备份顺序

### 2.1 识别路径

| 根 | 默认路径 | 内容 |
|----|----------|------|
| **引擎 `data_dir`** | `_data` 或 `--data-dir` | `wal.log`、`checkpoint*`、MANIFEST、SST、`undo.log`、可选 `wal.segments` / `undo.segments` 与 `*/archive/*` |
| **会话 `session_dir`** | `{data_dir}/embed_session` 或 `--session-dir` | `session.journal`、`session.ckpt`、可选 `session.txn`、`session_log.txt` |

完整清单见 **[`POLICY.md` §4.0.2](POLICY.md)**。

### 2.2 执行备份

**方式 A — PowerShell（推荐）**

```powershell
# 停写后
.\scripts\backup_bundle.ps1 -DataDir E:\path\to\_data -SessionDir E:\path\to\embed_session -OutDir E:\backups\structdb_20260516
```

**方式 B — `structdb_app`（不启动引擎）**

```powershell
.\build\src\app\Release\structdb_app.exe --backup-bundle E:\backups\structdb_20260516 --data-dir E:\path\to\_data --session-dir E:\path\to\embed_session
```

输出目录结构：

```text
OutDir/
  data_dir/    # 引擎树副本
  session_dir/ # 会话树副本
```

---

## 3. 恢复步骤

1. **删除或移走** 目标机上同名 `data_dir` / `session_dir`（若需覆盖恢复）。
2. 将备份中 `data_dir`、`session_dir` **原样复制** 到目标路径。
3. 以 **相同相对布局** 启动：
   ```powershell
   structdb_app.exe --data-dir <data_dir> --session-dir <session_dir> --repl
   ```
4. **Journal 重放检查**（embed）：
   - 打开无崩溃；`SHOW TXN` → `active=no`（或预期事务态）。
   - 对业务表 `USE(t)` → `COUNT` 与备份前一致。
5. **WAL 重放**：若异常，查 `wal.log` 尾帧、`checkpoint.active`；见 [`phases/WAL_REPLAY.md`](phases/WAL_REPLAY.md)。

---

## 4. 验收清单

- [ ] 冷备前 `COUNT` = N；恢复后 `COUNT` = N
- [ ] `Mdb.IntegrateTxnRecoverRollbackRestartChain` 类场景在 CI 仍绿（回归锚点）
- [ ] 多段 WAL / undo 封存目录若存在，已随 `data_dir` 一并复制

---

## 5. 独占目录锁（GUI / 多进程）

| 场景 | 建议 |
|------|------|
| 生产：GUI + CLI 可能共用目录 | 设置 **`STRUCTDB_GUI_EXCLUSIVE_DIR_LOCK=1`**（见 [`gui/rust_gui/README.md`](../gui/rust_gui/README.md)） |
| 开发：多窗口调试 | 默认 **关闭** 独占锁 |
| 备份窗口 | **必须** 单写者；备份脚本不替代锁，须先停写 |

第二进程打开同一 `data_dir` 时，启用独占锁应 **打开失败** 而非静默损坏（三十五期）。

---

## 6. checkpoint_seq 与 PITR（Wave 3）

- 冷备时记录 **`SHOW CHECKPOINTS`** 或 `EmbedClient::last_engine_checkpoint_seq()` 输出的 **最大 seq**。
- 将 `checkpoint.chain` 与 `checkpoint.a`/`checkpoint.b` 一并纳入备份 bundle（`scripts/backup_bundle.ps1` 扩展时优先包含 `data_dir` 根下 chain 文件）。
- **分段恢复**：`scripts/recover_to_checkpoint.ps1 -DataDir … -CheckpointSeq n`（调用 `structdb_app --recover-to-checkpoint-seq`）；或 C API `structdb_recover_data_dir_to_checkpoint_seq`；详见 [`phases/PHASE43.md`](phases/PHASE43.md)。
- **备份清单**：`backup_bundle` / `structdb_backup_bundle` 在存在 `checkpoint.chain` 时写入 `backup_manifest.json`（`last_checkpoint_seq`）。

## 7. 相关文档

- [`POLICY.md` §4.0](POLICY.md) — 保底文件与恢复权威顺序  
- [`phases/PHASE43.md`](phases/PHASE43.md) — `RECOVER TO CHECKPOINT_SEQ`  
- [`COMPETITIVE_MATRIX.md`](COMPETITIVE_MATRIX.md) §6.2 — `RENAME`/`DROP` 原子性（Wave 2）  
- [`scripts/results/README.md`](../scripts/results/README.md) — 性能归档（与备份独立）
