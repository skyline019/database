# 十二期：MANIFEST 分层（L0/L1）与「双最旧 L0 合并 → L1」MVP

本文描述 **十二期** 已实现内容：在 **单写者、同步 compaction** 前提下，为 `MANIFEST` 引入 **可持久化的 level 元数据**（**L0** 与 **L1**），读路径按 **L0 新→旧，再 L1 新→旧** 扫描；在配置开启时，`compact_merge_two_oldest_l0` 将 **最旧两个 L0 SST** 的合并结果写入 **`L1-{gen}.sst`** 并置于 MANIFEST **末尾**（L1 区），否则保持九期语义（输出仍为 **`L0-{gen}.sst`** 并置于列表 **前端**）。

## 1. MANIFEST 磁盘格式

- **旧格式（九～十一期）**  
  - 第 1 行：`manifest_version`（`uint64`）  
  - 第 2 行：`n`（SST 个数）  
  - 随后 `n` 行：每行一个相对路径；加载时 **全部视为 level 0**。

- **新格式（十二期，`FORMAT2`）**  
  - 第 1 行：`manifest_version`  
  - 第 2 行：固定字面量 **`FORMAT2`**  
  - 第 3 行：`n`  
  - 随后 `n` 行：每行 **`{level} {relative_path}`**（`level` 为十进制 `0` 或 `1`，与路径之间 **一个空格**；路径内不应含未转义换行）。  
  - **保存**：当前引擎在成功写入 MANIFEST 时 **一律写出 `FORMAT2`**（从旧目录打开后会 **升级** 格式）。

## 2. 内存不变式

- MANIFEST 中条目顺序为：**一段连续的 L0（由旧到新）**，后接 **零个或多个 L1（由旧到新）**。  
- `flush_memtable` 产生的新 SST 恒为 **L0**，插入在 **最后一个 L0 之后、第一个 L1之前**（若尚无 L1 则追加在末尾）。

## 3. 读路径顺序（`get` / `visit_prefix` / `lookup_versioned_raw_for_undo`）

与九期「单列表、自尾向首即新→旧」一致，推广为：

1. **MemTable**（若有则优先）。  
2. **所有 L0 文件**：按 MANIFEST 中 L0 段的 **新 → 旧**（自 L0 段最后一项向前扫到第一项）。  
3. **所有 L1 文件**：按 L1 段的 **新 → 旧**。

同一键在较新 SST 中的值覆盖较旧；故 **任意 L0 优先于任意 L1**。

## 4. Compaction 与十一期阈值

- **`compact_merge_two_oldest_l0`**：要求 MANIFEST **前两项均为 level 0**；否则失败。  
- **默认（`l1_compact_output_from_l0_merge == false`）**：与九期相同 —— 合并结果为 **`L0-{gen}.sst`**，替换后列表为 **`[新 L0] + 原列表自第三项起]`**（均保留原 level）。  
- **开启 L1 输出**：合并结果为 **`L1-{gen}.sst`**，level **1**；在去掉被合并的两项后，将新文件 **追加在 MANIFEST 末尾**（L1 区）。  
- **顺序**：仍为先写 SST → **持久化 MANIFEST** → 删旧文件 → **checkpoint**（含十期 `undo_log_safe_prefix_bytes` 重算）→ `compaction_merge_count_` 递增。  
- **十一期 `l0_compact_trigger_threshold`**：以 **`l0` 段长度**（leading level-0 个数）与阈值比较，而非「含 L1 的总 SST 数」。

## 5. 配置

| 位置 | 字段 | 默认 | 说明 |
|------|------|------|------|
| `EngineConfigSnapshot` | `l1_compact_output_from_l0_merge` | **false** | 为 **true** 时，上述合并输出写入 L1 并记入 MANIFEST level 1。 |
| `StorageEngine` | `set_l1_compact_output_from_l0_merge(bool)` | — | 由 `Engine::startup` 从快照注入。 |

## 6. 非目标（十三期或独立项）

- L2+、size-tiered、非重叠键区间上的复杂挑选。  
- **并发** compaction、与 **Scheduler/Orchestrator** 背压全链路联动。  
- **`undo.log` / WAL 分段轮转**（见 `UNDO_LOG_4C.md` §3.3）。

## 7. 单测与 filter

- `StorageEngine.Phase12*`、`Engine.L1CompactOutputFromConfig`（见 `tests/structdb_tests.cpp`）。  
- 事务链文档：[TESTING_TXN_CHAIN.md](TESTING_TXN_CHAIN.md) §9。

## 8. 相关文档

- [`COMPACTION.md`](COMPACTION.md)、[`POLICY.md`](POLICY.md)、[`CHANGELOG.md`](CHANGELOG.md)。
