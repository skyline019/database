# PHASE44：流式 persist 与 IMPORT_SEGMENT 设计草案（Wave 2 首段）

本文档为 **设计草案**；Wave 2 已落地部分为 `build_persist_command_batch` 的 **有序行 id 流式遍历**（`ordered_row_ids_for_persist`），与 PHASE40 分块 submit **正交**。

## 1. 已落地（Wave 2）

| 项 | 说明 |
|----|------|
| 流式行遍历 | 全量 persist 时按 `row_ids_ordered` 或排序后的 id 列表逐行 `append_row_puts`，避免一次性复制整表 `rows` map 再迭代 |
| 与分块 | `persist_table_chunked` 仍按 `mdb_persist_chunk_max_puts` 切帧；流式降低 **单帧构建前** 的峰值内存 |

**验收（二选一，本机 mega_data）**：`scripts/bench/mega_data_mdb_stress.ps1` 可选 `-SampleWorkingSet` 记录进程 `WorkingSet64`；对比 Wave 2 前后 JSON 中 `peak_working_set_bytes`；或 TPS ≥ 基线 90%（`scripts/weekly_bench.ps1`）。

## 2. IMPORT_SEGMENT（Wave 4 已落地）

| 项 | 说明 |
|----|------|
| MDB | **`IMPORT SEGMENT (token)`** 设置会话 token；切换段时自动 **persist** 上一段 |
| Idempotency | `idem:import:<table>:seg:<token>`（`mdb_resolve_persist_idem`） |
| 样例 | [`scripts/bench/import_segment_two_segments.mdb`](../../scripts/bench/import_segment_two_segments.mdb) |
| 验收 | `Mdb.Phase44ImportSegmentTwoSegments` |

## 3. 段边界与 WAL（文档）

目标：与 `wal.segments` v2、checkpoint 序号对齐的 **分段导入**，减少单进程 1M 行脚本的峰值与尾延迟。

| 主题 | 草案 |
|------|------|
| 段边界 | 每段独立 `idempotency_token`：`idem:import:<table>:seg:<n>` |
| 截断 | 仅允许在 checkpoint 边界截断未完成段；与 [`POLICY.md`](../POLICY.md) WAL 权威一致 |
| 索引 | 段结束可选 `REBUILD INDEX` / 命名索引全量重建（与 PHASE41 导入说明一致） |
| 存储 | 复用 `storage_import_store_raw_logical` + plain 行路径（PHASE40） |

**Spike（不入主链默认）**：sorted external → SST ingest；见 [`OPTIMIZATION_PLAN.md`](../OPTIMIZATION_PLAN.md)。

## 4. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-16 | 初稿：流式 persist 已落地说明 + IMPORT_SEGMENT 草案 |
| 2026-05-16 | Wave 3：`IMPORT SEGMENT` MDB 原型日志 |
| 2026-05-16 | Wave 4：段 idempotency + GTest + 样例脚本 |
