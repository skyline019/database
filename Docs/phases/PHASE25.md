# 二十五期：MDB 自定义命令库对标 newdb（语义映射版）

在 **二十四期** 文档与观测入口稳定的前提下，本期将 **MDB 解析与 dispatch** 扩展到与 newdb CLI `HELP`（参考 `e:/db/DB/newdb/cli/shell/dispatch/handlers/txn/txn_handler.cc` 中 `HELP` 字符串）**同名命令优先**对齐；**语义**以 StructDB 的 **KV（`mdb$v2$*`）+ LSM + `Engine`** 为准，不模拟 newdb 堆文件与侧车。

## 1. 子阶段与交付

| 子阶段 | 内容 |
|--------|------|
| **25A** | `DROP TABLE(name)`、`RENAME TABLE(new)`（当前 `USE` 表）、`RESET`（清空当前表行与 schema）；`BEGIN` 内 **拒绝** DDL/维护类命令。 |
| **25B** | `SHOW ATTR` / `DESCRIBE`、`SHOW KEY` / `SHOW PRIMARY KEY`、`SET PRIMARY KEY(col)`（列值唯一性校验；持久化在 schema 载荷 `PKCOL` 行）；`FINDPK` 使用 **逻辑 PK 列**（默认首列 DEFATTR，可被 `SET PRIMARY KEY` 覆盖）。**列类型**（`DEFATTR` 校验）：`int`、`string`/`varchar`/`text`、`char`（空或单 ASCII 字符）、`float`、`double`、`datetime`（`YYYY-MM-DD` 或 `YYYY-MM-DD HH:MM:SS`）、`timestamp`（十进制整数 **或** 与 `datetime` 相同格式）；未知类型拒绝；`INSERT`/`UPDATE`/`BULKINSERT` 等遇 **类型不匹配** 即报错并中止脚本/REPL 行。 |
| **25C** | `VACUUM` → `flush_memtable` + `drain_l0_compaction_queue(max=8)`；`SCAN`（最多 5000 行）；**`SCAN MORE`** / **`SCAN MORE(n)`**（分页续扫，会话游标）；**`SCAN RESET`**（清空游标）；`BULKINSERTFAST`（持久化 `fsync=false`）；`IMPORTDIR(path)`（仅遍历目录下 `*.mdb` 顺序执行 `run_mdb_script`）；`EXPORT` 允许 **`JSON path` 或裸路径**（默认按 JSON 导出）。 |
| **25D** | `EXIT` / `QUIT`（`MdbRunResult::repl_exit_requested` + `app/main` REPL）；`SHOWLOG`（`session.journal` 尾部）；`RELEASE SAVEPOINT name`。 |
| **25E** | `SHOW TUNING` / `SHOW STATUS`、`SHOW TUNING JSON` / `SHOW STATUS JSON`（StructDB 自有 JSON 形态）；`SHOW STORAGE` / `SHOW STORAGE JSON`（`storage_pressure_snapshot`）。 |
| **25F** | `QBAL(col,min_int)`（整列 `>= min` 的匹配行 **计数与求和**）；`SHOW PLAN` / `EXPLAIN WHERE`（**文本级**谓词 + 匹配行数 + 是否可能走 string 等值侧车提示）。 |
| **25G** | **拒绝矩阵**：`AUTOVACUUM`、`RECOVER TO …`、`WALADAPTIVE`、`SEGMENT`、`WRITECONFLICT`、`CREATE SCHEMA`、`DROP SCHEMA`、`LIST SCHEMAS`、`SHOW SCHEMAS` 等 → `[NOT_SUPPORTED]`。**`WALSYNC`/`GROUPCOMMIT`** → 定向文案（`SET DURABILITY 0|1|2`，见 **四十一期**）。**`HOTINDEX`** → 定向 `CREATE INDEX`。**`ALTER TABLE`**：仅 **未实现的子命令** 仍 NotPortable；**`ADD COLUMN` / `RENAME COLUMN`** 见 [`PHASE41.md`](PHASE41.md)。`CREATE SCHEMA` / `DROP SCHEMA` 在解析阶段即归入 **NotPortable**。**`CONFIRM_REORDER`** 见 [`PHASE38.md`](PHASE38.md)。 |

## 2. 非目标

- **不**扩展 `structdb_capi`（本期仅限 MDB/REPL）。
- **不**实现 newdb 堆专属语义（行重排、堆 `VACUUM` 文件紧凑、跨进程 index catalog 等）。
- **不**实现 `RECOVER TO LSN/TIME` 等 PITR CLI。
- **`RENAME TABLE`**：自 **四十一期** 起为单 `CommandBatch` 原子 submit（见 [`PHASE41.md`](PHASE41.md)）。**`DROP TABLE`** 仍为 gather + 单批；更复杂 DDL 崩溃窗口见 `PHASE31` / `POLICY`。

## 3. 验收

- `structdb_tests`：`Mdb.Phase25*`；**严格回归**：`Mdb.Phase25Strict*`（解析矩阵、`BEGIN` 内 DDL/维护拒绝、`RENAME` 与 catalog、`RESET`+`DESCRIBE` 错误路径、`QBAL`/`EXPLAIN WHERE`、`RELEASE SAVEPOINT` 错误、`DROP`/目录/`VACUUM`、`IMPORTDIR`+`EXPORT`+`BULKINSERTFAST`+`SHOW STORAGE JSON`+`QUIT` 等）；**`BEGIN` 内 `SCAN`**、**`SHOWLOG` 非空尾部**、**`VACUUM` 无 storage**；**`Mdb.ScanMoreCursorPaging`**（`SCAN MORE`/`SCAN RESET` 游标分页）；**`Mdb.Strict*`**（`DEFATTR` 未知类型、`INSERT` 类型不匹配中止、多类型空单元格合法路径）；**`Mdb.IntegrateTxnRecoverRollbackRestartChain`**（`SAVEPOINT`/`ROLLBACK TO`/`INSERT`/`session.txn` 恢复与全回退）。**三十八期**：`Mdb.Phase38*`（`CONFIRM_REORDER` / `[REORDER_MAP_JSON]`，见 [`PHASE38.md`](PHASE38.md)）。
- `HELP` 与 [`mdb_runner_dispatch.inc`](../src/client/mdb/src/mdb_runner_dispatch.inc) 文案可互相索引。

## 4. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-13 | 初稿：25A–25G 与 newdb 语义映射 |
| 2026-05-13 | 增补 **`Mdb.Phase25Strict*`** 回归；**`IMPORTDIR(path)`** 解析按关键字长度定位 `(`（修复无空格时误吞整行）。 |
| 2026-05-13 | **`DEFATTR`/`type_matches`**：`int`/`string`/`varchar`/`text`/`char`/`float`/`double`/`datetime`/`timestamp`；**`Mdb.Strict*`** 与 **`Mdb.IntegrateTxnRecoverRollbackRestartChain`**；`BEGIN` 内 **`SCAN`**、**`SHOWLOG`**、**`VACUUM` 无 storage** 测试。 |
| 2026-05-14 | **25G**：从 NotPortable 矩阵移除 **`CONFIRM_REORDER`**（实现见 [`PHASE38.md`](PHASE38.md)）。 |
| 2026-05-14 | **25C**：补充 **`SCAN MORE`** / **`SCAN MORE(n)`** / **`SCAN RESET`** 与验收 **`Mdb.ScanMoreCursorPaging`**。 |
| 2026-05-16 | **25G**：**PHASE41** — `SET DURABILITY`；`ALTER TABLE ADD|RENAME COLUMN`；`CREATE INDEX`；`WALSYNC`/`GROUPCOMMIT`/`HOTINDEX` 定向提示；`RENAME TABLE` 单批原子性。 |
