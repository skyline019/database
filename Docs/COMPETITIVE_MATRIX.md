# StructDB 竞品与能力矩阵（全维度）

本文档汇总 StructDB 与市面数据库的**全维度对比**（**刻意排除**客户端/服务器协议、复制拓扑、集群分片、连接池等**网络与分布式**维度），并附 **SQLite**、**RocksDB + 自建 SQL** 细表及 **MDB `[NOT_SUPPORTED]` 功能缺口清单**。

**维护约定**：能力以 [`POLICY.md`](POLICY.md)、[`CHANGELOG.md`](CHANGELOG.md)、[`phases/PHASE25.md`](phases/PHASE25.md)、[`phases/PHASE40_PERSIST_PERF.md`](phases/PHASE40_PERSIST_PERF.md) 为准；发版或里程碑合并时同步修订本文「性能」与「缺口」节。

**端到端峰值（查询 / 插入）**：[`PEAK_PERFORMANCE.md`](PEAK_PERFORMANCE.md)（100 万行、`mdb_query_complex_v1` 基线，2026-05-17）。

**图例**：**●** 成熟/强项 · **◐** 部分/受限 · **○** 弱或缺失 · **—** 不适用（产品形态不同）

---

## 1. StructDB 在谱系中的位置

StructDB 是 **同进程嵌入式** 的 C++17 **实验型** 存储运行时，不是通用「对外服务的数据库产品」。

| 维度 | StructDB 现状 |
|------|----------------|
| 部署形态 | 库 + `structdb_app` / Tauri GUI；`data_dir` + `session_dir` 本地文件 |
| 数据模型 | **逻辑表**（MDB）→ 引擎 **`mdb$v2$*`** 版本化 KV；无「每表独立权威数据文件」 |
| 查询语言 | **MDB**（对标 newdb CLI 命令名，**非 SQL**） |
| 存储 | **LSM 主线**（WAL / MemTable / SST / L0–L4 compaction）+ **页式子集**（buffer pool、undo、双槽 checkpoint） |
| 并发 | **单写者**；同 `data_dir` 不可并行独占 WAL；读路径 `shared_mutex` 与写并发（三十六期） |
| 分布式 | **明确非目标**（根 `README`：非分布式、非多主） |

更接近：**「带表语义与事务链的嵌入式 KV/LSM 引擎 + REPL/GUI」**，而非 PostgreSQL / MySQL 同级通用 RDBMS。

---

## 2. 全维度对照总表

| 维度 | StructDB | PostgreSQL / MySQL / SQL Server | SQLite | MongoDB | RocksDB / LevelDB | Redis |
|------|----------|----------------------------------|--------|---------|-------------------|-------|
| **SQL / 标准查询** | ○ MDB only | ● | ●（子集） | ○ 文档 API | — | — |
| **多表 JOIN / 子查询** | ○ | ● | ◐ | ◐ 聚合管道 | — | — |
| **二级索引 / 优化器** | ◐ 逻辑行索引、扫描、`EXPLAIN` 文本级 | ● | ● | ● | ○ 用户管 prefix | ○ |
| **ACID（单机）** | ◐ 三层事务链；默认 `ROLLBACK` 不链式撤销已落盘 KV | ● | ● | ◐ | ◐ | ◐ |
| **隔离级别** | ◐ `snapshot` / `read_committed` + `read_max_seq` | ● | ● | ◐ | — | — |
| **MVCC** | ◐ `commit_seq` 裁剪单版本可见性 | ● | ◐ | ● | ○ | — |
| **崩溃恢复** | ● WAL 权威 + checkpoint 双槽 | ● | ● | ● | ● | ◐ |
| **PITR / 时间点恢复** | ○ | ● | ◐ | ● | ○ | ◐ |
| **复制 / 高可用** | — | ● | ○ | ● | ○ | ● |
| **水平扩展** | — | ● / 分片 | ○ | ● | — | ● |
| **多写者并发** | ○ 单写者 | ● | ◐ | ● | ◐ | ●（单线程写模型） |
| **Schema 演进** | ◐ `ALTER TABLE ADD/RENAME COLUMN`（PHASE41）；无改类型/删列 | ● | ● | ● | — | — |
| **Bulk 导入吞吐** | ● 专线路径（四十期；纯插入峰值见 §7.3） | ● | ● | ● | ● | ● |
| **复杂查询（MDB 子集）** | ◐ 点查/聚合/PAGE_JSON/GROUP BY/SCAN INDEX（见 §7.3） | ● | ◐ | ◐ | — | — |
| **Compaction / 读放大** | ◐ L0–L4，路线图中深化 | ● | ◐ | ◐ | ●/◐ | — |
| **全文 / 地理 / JSON SQL** | ○ | ● | ◐ | ● | — | ◐ |
| **触发器 / 视图** | ○ | ● | ◐ | ○ | — | ○ |
| **可观测性（单机）** | ● trace、pressure、bench | ● | ◐ | ● | ◐ | ● |
| **驱动 / 生态** | ◐ C API + GUI | ● | ● | ● | ● | ● |
| **许可 / 成熟度** | MIT、**实验工程** | 成熟 | 极成熟 | 成熟 | 成熟 | 成熟 |

---

## 2.1 上市与头部商业数据库（单机语义，排除网络）

下表指 **在可比「单机、同进程或本地库文件」语义下** 与 StructDB 的对照；**不**比较连接池、复制、云托管 SLA。上市主体仅作产品谱系索引，非投资建议。

| 产品 | 典型上市/主体 | 部署 | 与 StructDB 同赛道？ | 能力胜负（单机） |
|------|----------------|------|----------------------|------------------|
| **PostgreSQL** | —（开源；生态含多家上市公司发行版） | 常作本机服务或嵌入式扩展 | ○ 不同赛道 | PG：**SQL/JOIN/MVCC/优化器/成熟运维** 全面领先；StructDB：**嵌入式一体、MDB/newdb 对齐、bulk 专线路径** |
| **MySQL / InnoDB** | Oracle（ORCL） | 服务或嵌入式 | ○ | 同上；MySQL **生态与复制** 远强 |
| **SQL Server** | Microsoft（MSFT） | 服务为主 | ○ | T-SQL、企业功能、HA；StructDB 无对等 |
| **Oracle Database** | Oracle（ORCL） | 服务 | ○ | 企业 RDBMS 全功能；StructDB 非替代 |
| **MongoDB** | MongoDB Inc.（MDB） | 服务 / 本地 | ◐ 部分重叠 | Mongo：**文档模型、分片、聚合管道**；StructDB：**逻辑表 + KV/LSM**，无副本集 |
| **Snowflake** | Snowflake（SNOW） | 云数仓 | — | 分析型云仓；StructDB **非分析仓、非 SQL 标准** |
| **Redis** | Redis Ltd.（私有；生态上市公司多） | 内存 + 可选 AOF/RDB | ◐ | Redis：**亚毫秒内存结构**；StructDB：**磁盘优先、表持久化、百万行 bulk** |
| **SQLite** | —（公有领域；Apple/Google 等生态） | 单文件嵌入 | **● 最接近** | 见 §4；SQL 与生态 SQLite 胜，MDB/LSM bulk 专线路径 StructDB 胜 |
| **RocksDB** | Meta 开源 / 多家基于其发行 | 库 | ◐ KV 层 | 见 §5；通用 KV + ingest RocksDB 胜；表语义 + MDB 已集成则 StructDB 胜 |

**全量能力一句话**：上市库在 **标准 SQL、并发写、HA、运维工具、合规与生态** 上全面领先；StructDB 仅在 **嵌入式 + MDB 工作台 + 可控 WAL/bulk 实验栈** 窄场景有差异化，**不能**作为 PG/MySQL/Oracle 的通用替代品。

---

## 3. 分品类细比

### 3.1 关系型数据库（PostgreSQL、MySQL、Oracle、SQL Server）

| 能力 | 市面 RDBMS | StructDB |
|------|------------|----------|
| 查询表达力 | SQL、JOIN、窗口函数、CTE、视图、约束 | MDB：`WHERE`、`PAGE_JSON`、`GROUP BY`（单表 COUNT/SUM）、`SCAN INDEX(STATS/IDS)`、`SUM`/`QBAL`（聚合缓存快路径）、`EXPLAIN WHERE`；**无 JOIN、无视图**；`ALTER` 仅 ADD/RENAME 列子集 |
| 优化与执行 | 代价优化器、多种索引 | **无 CBO**；`SHOW PLAN` 为文本级提示 |
| 事务语义 | 标准 ACID + 行锁 | MDB 会话 / Embed 批次 / `commit_seq`；默认 **`ROLLBACK` 不回滚已落盘 `mdb$`** |
| 存储引擎 | 成熟 B+Tree / InnoDB 等 | 自研 LSM + undo 子集；借鉴 InnoDB 思路但**非等价** |
| 运维 | 备份、PITR、复制 | **保底文件清单**（`POLICY` §4.0）；无 `RECOVER TO` 等产品化 CLI |

**结论**：通用业务库、复杂 SQL、频繁 Schema 变更 → **不在同一赛道**。嵌入式、newdb/MDB 对齐、可控 WAL 语义 → **差异化**。

### 3.2 SQLite（最接近的嵌入式参照）

见 **[§4 StructDB vs SQLite 细表](#4-structdb-vs-sqlite-细表)**。

### 3.3 文档库（MongoDB 等）

- **有**：逻辑表、列类型、`EXPORT JSON`、`SCAN MORE` 游标  
- **无**：副本集、分片、聚合管道、分布式多文档事务  
- 物理层为 **KV + LSM**，非 BSON 文档堆  

### 3.4 嵌入式 KV（RocksDB、LevelDB）

见 **[§5 StructDB vs RocksDB + 自建 SQL](#5-structdb-vs-rocksdb--自建-sql-细表)**。

### 3.5 内存库（Redis，仅单机语义）

Redis：**内存数据结构 + 可选持久化**；StructDB：**磁盘优先、WAL 权威**。Redis 胜在延迟与数据结构丰富度；StructDB 胜在表级持久化、大 bulk 落盘（非纯缓存场景）。

### 3.6 NewSQL / 分布式库（TiDB、CockroachDB 等）

核心能力为分布式一致性与水平扩展；StructDB **非目标**。若仅比单机存储子集：与 TiKV/Rocks 同源思路，但完整度与生产成熟度差距大。

---

## 4. StructDB vs SQLite 细表

| # | 维度 | SQLite | StructDB | 差异要点 |
|---|------|--------|----------|----------|
| 1 | 查询接口 | SQL-92 子集 + 扩展 | **MDB** | 无 `SELECT … JOIN …` |
| 2 | 部署单元 | 通常单文件 `.db` | `data_dir` + `session_dir` | 备份需理解双根（`POLICY` §4.0） |
| 3 | 表物理模型 | B+Tree 页内行 | `mdb$v2$*` KV | 无单文件直观表文件 |
| 4 | 索引 | CREATE INDEX、FTS 等 | 逻辑 `row_index`、**`CREATE INDEX` 命名侧车**（PHASE41）、`EXPLAIN WHERE` | 无 B+Tree 在线 DDL / FTS |
| 5 | 优化器 | 基于代价 | 无 CBO | 复杂查询需应用设计访问路径 |
| 6 | 写并发 | WAL 模式多读单写 | **单写者** | StructDB 读可与写 `shared_mutex` 并发 |
| 7 | 事务 | 标准 SQL 事务 | 三层事务链 | 默认 `ROLLBACK` 不链式弹 `undo_stack_` |
| 8 | 隔离级别 | DEFERRED 等 | `TXNISOLATION` + `read_max_seq` | 非 SQL 标准四级；非完整行 MVCC 链 |
| 9 | SAVEPOINT | ● | ● | 与存储 undo 默认不对齐 |
| 10 | 类型系统 | 动态亲和性 | 严格 `DEFATTR` 类型集 | StructDB bulk 路径更严 |
| 11 | DDL | `ALTER TABLE` 等 | `CREATE/DROP/RENAME`；**`ALTER TABLE ADD/RENAME COLUMN`**（PHASE41） | 无改类型/约束/删列 |
| 12 | 约束 | PK/UNIQUE/FK/CHECK | 逻辑 PK + 唯一性校验 | 无外键、无 CHECK 语言 |
| 13 | 触发器 / 视图 | ● / ◐ | ○ | — |
| 14 | 全文检索 | FTS3/4/5 | ○ | — |
| 15 | JSON | `json1` | `EXPORT JSON`、部分 `SHOW * JSON` | 无 SQL 内 JSON 函数族 |
| 16 | 崩溃恢复 | WAL + 页校验 | WAL 权威 + 双槽 checkpoint | StructDB 仍标实验工程 |
| 17 | PITR / 备份 | `.backup`、生态 | ○ | — |
| 18 | VACUUM | 页回收、文件收缩 | flush + L0 drain | LSM 语义，非文件紧凑 |
| 19 | Bulk 加载 | `.import`（简单表常 **数万～数十万行/秒** 量级，视磁盘与 PRAGMA） | `BULKINSERTFAST` + `IMPORT MODE`（一体加载峰值 **~3.8K TPS**；纯插入 **~328K TPS**，§7.3） | 同进程 **引擎 bulk** 路径 StructDB 更高；带索引+查询的一体脚本低于纯 mega_data |
| 19b | 复杂查询延迟 | SQL 优化器 + B-Tree | warm 点查/聚合 **亚毫秒**（§7.3）；全表 `SCAN INDEX(ik,STATS)` **~1.1 s**/100 万行 | 无 SQL；非索引列 `WHERE dept` **~0.6 ms** 仍弱于索引点查 |
| 20 | 跨平台 / 生态 | 极广 | Windows 优先 + C API + Tauri | SQLite 生态碾压 |
| 21 | 成熟度 | 数十年生产 | MIT 实验 / phase 化 | 选型风险在成熟度 |

**选型**：标准 SQL + 单文件 + 生态 → **SQLite**；newdb/MDB 栈、LSM bulk、WAL 可控 → **StructDB**。

---

## 5. StructDB vs RocksDB + 自建 SQL 细表

假设典型栈：**RocksDB** + 自写 **SQL 解析/计划/执行** + 可选备份工具。

| # | 层级 | RocksDB + 自建 SQL | StructDB 已内置 | 仍缺 / 自建仍要做 |
|---|------|-------------------|-----------------|-------------------|
| 1 | KV API | Put/Get/Iterator | `Engine::kv_put/get`、`visit_prefix` | — |
| 2 | 列族 | Column Family | `mdb$` 前缀 + catalog | 无 CF 级独立 compaction 产品化 |
| 3 | WAL / 恢复 | RocksDB WAL + MANIFEST | `wal.log`、多段 v2、checkpoint 双槽 | 工具链不互通 |
| 4 | MemTable | 可插拔 | SkipList 默认 + `MemTableArena` | 无 `WriteBufferManager` 生态 |
| 5 | Compaction | L0–Ln、Universal、TTL | L0–L4；size-tiered 路线图中 | 无 TTL CF 等 |
| 6 | 存储事务 | TransactionDB | `commit_seq` + undo + embed 批 | 非 RocksDB 事务 API |
| 7 | 表 / Catalog | 自建 | MDB `CREATE TABLE` + `mdb$v2$*` | — |
| 8 | SQL | 需引入 | **无**（MDB） | 整个 SQL 层 |
| 9 | JOIN / 子查询 | 执行器实现 | ○ | — |
| 10 | 二级索引 | 自建索引表 | 逻辑索引 + 扫描 | 无通用 B+Tree 维护 |
| 11 | 查询优化 | CBO | `EXPLAIN WHERE` 文本级 | — |
| 12 | 会话 / REPL | 另写 | MDB + GUI + C API | — |
| 13 | 幂等 / 会话恢复 | 应用自建 | Embed journal + `session.ckpt` | — |
| 14 | Bulk | WriteBatch + SST ingest | PHASE39/40 专线路径 | RocksDB **SST ingest** 更通用成熟 |
| 15 | 观测 | statistics | pressure、trace、perf gate | 指标命名不同 |
| 16 | 多语言绑定 | 官方多语言 | C API + Rust GUI | 需自行封装 |
| 17 | 运维 | `ldb`、备份 | 保底文件 + `SHOW STORAGE` | 无等价 `ldb` 产品 |

| 目标 | 更优选择 |
|------|----------|
| 只要 KV，上层完全自控 | RocksDB |
| 要 SQL 标准 + 最大生态 | RocksDB + SQLite/PG 嵌入式，而非从零 SQL |
| 要表语义 + WAL/embed/MDB/GUI 已打通 | StructDB |
| 要 RocksDB 级 TransactionDB / ingest / TTL | 继续 PHASE 路线或直接用 RocksDB |

---

## 6. 功能缺口清单（PHASE25 + 客观缺口）

### 6.1 显式 `[NOT_SUPPORTED]`（`mdb_command_parser.cpp`）

命中 `MdbVerb::NotPortable` 时输出：`[NOT_SUPPORTED] StructDB KV/LSM has no heap/newdb-txn equivalent for: …`

| 命令 / 前缀 | 类别 | 说明 |
|-------------|------|------|
| `AUTOVACUUM` | 维护 / 堆语义 | newdb 堆自动整理；StructDB 为 LSM `VACUUM` |
| `RECOVER TO …`（TIME/LSN） | 运维 / PITR | 定向：`RECOVER TO CHECKPOINT_SEQ n`（PHASE43） |
| `WALSYNC` | 耐久调优 | 定向：`SET DURABILITY 0|1|2`（PHASE41） |
| `GROUPCOMMIT` | 耐久 / 吞吐 | 同上 |
| `WALADAPTIVE` | WAL 策略 | 无自适应 WAL 产品化接口 |
| `SEGMENT` | 日志分段 | 引擎配置层多段，非此 CLI |
| `HOTINDEX` | 索引 | 定向：`CREATE INDEX` + `REBUILD INDEX`（PHASE41） |
| `WRITECONFLICT` | 并发 / OCC | 单写者模型；多进程用独占目录锁（见 ONBOARDING） |
| `CREATE SCHEMA` | 多库 | 解析期 NotPortable |
| `DROP SCHEMA` | 多库 | 同上 |
| `LIST SCHEMAS` | 多库 | 无 schema 列表 |
| `SHOW SCHEMAS` | 多库 | 同上 |
| `ALTER TABLE`（未实现子命令） | DDL | 仅 `ADD COLUMN` / `RENAME COLUMN` 见 [`phases/PHASE41.md`](phases/PHASE41.md) |

**出处**：[`phases/PHASE25.md`](phases/PHASE25.md) §25G、[`phases/PHASE41.md`](phases/PHASE41.md)；[`src/client/mdb/src/mdb_command_parser.cpp`](../src/client/mdb/src/mdb_command_parser.cpp)。

### 6.2 PHASE25 文档非目标（未必进解析器）

| 缺口 | 来源 |
|------|------|
| newdb **堆文件**语义（堆紧凑、跨进程 index catalog） | PHASE25 §2 |
| `RECOVER TO LSN/TIME` 等 **PITR** | PHASE25 §2 |
| `RENAME TABLE` 跨批非原子 | **已修复**（PHASE41 单 `CommandBatch`）；见 [`PHASE41.md`](phases/PHASE41.md) |
| 二十五期 **不扩展 C API**（仅 MDB/REPL） | PHASE25 §2 |

### 6.3 已支持但受限

| 能力 | StructDB 现状 | 相对 SQL 栈缺口 |
|------|---------------|-----------------|
| `VACUUM` | flush + L0 drain | 非页式文件收缩 |
| `SCAN` / `SCAN INDEX` | 表扫描 ≤5000 行；`SCAN MORE`/`RESET`；**`SCAN INDEX(ik,STATS|IDS|capped)`**（PHASE42+） | 无标准服务器游标；全索引 STATS 百万行约秒级 |
| `SHOW PLAN` / `EXPLAIN WHERE` | 文本 + 行数 + 侧车提示 | 无真实代价计划 |
| `REBUILD INDEX` | 逻辑行索引 + 命名索引 postings | 非多索引在线 DDL |
| `CREATE INDEX` / `SET DURABILITY` | PHASE41 命名侧车与会话耐久 | 非引擎全局 group commit 产品化 |
| `ALTER TABLE ADD/RENAME COLUMN` | PHASE41 子集 | 无改类型/删列 |
| `CONFIRM_REORDER` | PHASE38 已实现 | StructDB 扩展，非 SQL |
| `ROLLBACK` | 默认只回会话表 | 存储 undo 需 `mdb_chain_rollback_on_mdb_rollback` |
| `BEGIN` 内 | 拒绝 DDL/维护（含 `SCAN`） | 比 SQLite 更严 |

### 6.4 客观缺口（未列入 NOT_SUPPORTED 矩阵）

| 优先级 | 缺口 |
|--------|------|
| P0 | **SQL**；**JOIN / 子查询**；**GROUP BY** 仅单表 COUNT/SUM（已实现子集，无 HAVING/多列复杂分组） |
| P0 | **通用二级索引**深化：复合列仍缺口；**DROP INDEX** / **CREATE UNIQUE INDEX** 子集（PHASE45 ◐） |
| P1 | **ALTER TABLE** 扩面（改类型/删列）；**多 schema**；**外键 / CHECK** |
| P1 | **完整 MVCC + purge**；**跨表 2PC**；**行锁 / 死锁** |
| P2 | **PITR** 墙钟/LSN；**`RECOVER TO CHECKPOINT_SEQ`** 子集（PHASE43 ◐）；**在线物理备份 API** |
| P2 | **复制 / 只读副本**（README 非目标） |
| P3 | 全文 / 地理 / 向量；触发器 / 视图 / 存储过程；JDBC/ODBC |
| 工程 | L2+ / size-tiered 深化；MemTable 分片等（见 [`STORAGE_EVOLUTION_AND_OBSERVABILITY.md`](STORAGE_EVOLUTION_AND_OBSERVABILITY.md)） |

### 6.5 已实现 MDB 命令速查

**DDL / 表**：`CREATE TABLE`、`DROP TABLE`、`RENAME TABLE`、`ALTER TABLE ADD COLUMN`、`ALTER TABLE RENAME COLUMN`、`RESET`、`USE`、`DEFATTR`、`ADDATTR`、`DELATTR`、`RENATTR`、`SET PRIMARY KEY`、`CREATE INDEX`、`CREATE UNIQUE INDEX`、`DROP INDEX`

**DML**：`INSERT`、`UPDATE`、`DELETE`、`DELETEPK`、`BULKINSERT`、`BULKINSERTFAST`、`UPDATEWHERE`、`DELETEWHERE`、`SETATTR`、`SETATTRMULTI`

**查询**：`WHERE`、`WHEREP`、`FIND`、`FINDPK`、`COUNT`、`PAGE`、`PAGE_JSON`、`SCAN`/`SCAN MORE`/`SCAN RESET`、`SCAN INDEX`、`GROUP BY`、`SUM`/`AVG`/`MIN`/`MAX`、`QBAL`、`SHOW PLAN`、`EXPLAIN WHERE`

**事务**：`BEGIN`/`COMMIT`/`ROLLBACK`、`SAVEPOINT`、`ROLLBACK TO SAVEPOINT`、`RELEASE SAVEPOINT`、`TXNISOLATION`、`SHOW TXN`、`SHOW SNAPSHOT`

**运维**：`VACUUM`、`FLUSH PERSIST`、`IMPORT MODE`、`IMPORT SEGMENT`、`REBUILD INDEX`、`SET DURABILITY`、`SHOW CHECKPOINTS`、`RECOVER TO CHECKPOINT_SEQ`、`IMPORTDIR`、`EXPORT`、`SHOWLOG`、`SHOW TUNING`/`STATUS`（含 JSON）、`SHOW STORAGE`（含 JSON）、`CONFIRM_REORDER`

**元数据**：`LIST TABLES`、`SHOW TABLES`、`SHOW ATTR`/`DESCRIBE`、`SHOW KEY`

**其它**：`HELP`、`EXIT`/`QUIT`

---

## 7. 性能边界（四十期更新）

### 7.1 已解除的上限（bulk / persist 主路径）

三十九～四十期主要打掉 **MDB 大表落盘写放大**，而非单独替换 MemTable 结构：

| 旧瓶颈 | 对策 | 效果 |
|--------|------|------|
| hex / `ver$` 包装 | plain 行、raw 导入、零拷贝 WAL/MemTable | 固定成本下降 |
| O(n²) `row_ids_ordered` | 全表 bulk 延迟索引 | 百万行可承受 |
| 单帧百万键 | 分块 persist + `commit_embed_batch` 分帧 | 长尾缓解 |
| 脚本每批 persist | `mdb_script_amortize_bulk_dml` | EOF 摊销 |
| undo 查找 | 批量 undo、导入 skip undo | 导入批加速 |
| SkipList 分配 | `MemTableArena` | 分配压力减轻 |

**本机门禁**（`scripts/bench/mega_data_mdb_stress.ps1`，1M 行、`RowsPerLine=1000`）：

| 场景 | 约 TPS | 相对 PHASE39 ~7.5K |
|------|--------|---------------------|
| 默认（摊销 + 分块 + plain） | **~238K** | ~32× |
| `--mdb-bulk-import` | **~328K** | ~44× |

详见 [`phases/PHASE40_PERSIST_PERF.md`](phases/PHASE40_PERSIST_PERF.md)、[`CHANGELOG.md`](CHANGELOG.md) Unreleased 四十期。

### 7.2 仍有效的上限

| 场景 | 说明 |
|------|------|
| 通用 OLTP（单行 DML、频繁 persist） | 未走 plain/raw/分块/摊销全套；基线见 **`benchmarks/baselines/oltp_persist_baseline.json`**（`scripts/bench/oltp_persist_micro.ps1`）；合入门禁 **P99 ≤1.2×** 该基线 |
| REPL / 事务内 bulk | 可能每批立即 persist |
| 非 bulk 写 | WAL、锁、`commit_seq`、undo |
| Compaction P99 | flush 尾延迟、worker 队列 |
| 读 / 复杂查询 | `SCAN` 上限、无 SQL 优化器 |
| 下一档目标 | [`OPTIMIZATION_PLAN.md`](OPTIMIZATION_PLAN.md) §0.4（流式 chunk、IMPORT_SEGMENT、group commit 等） |

**宣称「上限解除」时须绑定**：`BULKINSERTFAST` + 脚本摊销 +（可选）`--mdb-bulk-import`；并说明导入批 **无 undo**、分块崩溃可能部分落盘（`PHASE40` 耐久表）。

### 7.3 端到端压测：StructDB 实测 vs 业界量级（2026-05-17）

**环境**：本机 Windows、`RelWithDebInfo`/`Release`、`structdb_app`；**100 万行**表 `qcx`（`id,dept,val,k` + `INDEX ik(k)`）。竞品列为 **文献/社区常见单机量级**（非同机对标），仅作谱系定位；**禁止**将不同协议/进程模型数值直接排名。

#### 插入 / 批量加载

| 场景 | StructDB（实测/归档） | PostgreSQL / MySQL（量级） | SQLite（量级） | 解读 |
|------|----------------------|------------------------------|----------------|------|
| 纯 bulk、引擎批量 | **~328K 行/秒**，墙钟 **~3.05 s**/1M（`mega_data` + `-EngineBulkImport`） | `COPY`/`LOAD DATA` 常 **5万～50万+ 行/秒**（硬件/索引/日志依赖极大） | `.import` 简单表常 **1万～20万+ 行/秒** | StructDB **专线路径**可与成熟库 bulk **同数量级**；无 SQL、无在线索引维护等价物 |
| 一体加载（建表+索引+后续查询压测） | **~3848 TPS** 峰值；典型 **3370～3410**（`IMPORT MODE` + 800 行/条 `BULKINSERTFAST`） | 带二级索引的 bulk 通常 **显著低于** 裸 `COPY` | 带索引导入通常 **低于** 无索引 `.import` | 真实「导入+索引」场景 StructDB **低于** 纯 mega_data，仍 **高于** 未优化 REPL 逐行 |
| 单行 OLTP persist | insert **P99 ~0.63 ms**，**~3K TPS**（`oltp_persist_micro`，1K 行） | 本机简单 INSERT 常 **1万～10万+ TPS**（无 fsync 时） | 单连接 INSERT 常 **数千～数万 TPS** | 通用 OLTP **仍弱于** 成熟库热路径；见 §7.2 |

#### 查询（warm 单会话，`ms_p95`）

| 场景 | StructDB（实测，基线 `024113`） | PG/MySQL（本机/同进程量级） | SQLite（同进程量级） | 解读 |
|------|------------------------------|-----------------------------|----------------------|------|
| 主键点查 `WHERE id` | **0.003 ms** | 内存命中常 **0.01～0.1 ms**；含解析/规划更高 | 索引点查常 **0.01～0.05 ms** | StructDB **同量级亚毫秒**（进程内、无网络） |
| 索引点查 `WHERE k` | **0.004 ms** | 类似 | 类似 | 命名索引侧车 + 缓存命中 |
| `SUM(val)` / `QBAL(val,0)` | **0.002～0.003 ms**（聚合缓存） | 全表聚合通常 **毫秒～秒级**（除非物化/索引仅扫描） | 全表 `SUM` 百万行常 **数十～数百 ms+** | **快路径** 在「可缓存元数据」子集上 **优于** 朴素全表扫描 |
| `PAGE_JSON` 分页 | **0.02～0.04 ms**（百行页） | ORM/驱动 + 服务端常 **更高** | 小页常 **亚毫秒～数 ms** | 键集/有序切片路径成熟 |
| `GROUP BY dept`（百档） | **~0.15 ms** | 简单分组常 **亚毫秒～数 ms**（视数据） | 类似 | 单表子集可用，无通用优化器 |
| `SCAN INDEX(ik,5000,STATS)` | **3.174 ms**（门禁） | 索引仅统计 5k 项：**亚毫秒～数 ms** | 依赖实现 | capped 扫描可 PR 门禁 |
| `SCAN INDEX(ik,STATS)` 全量 | **~1144 ms** | 百万行索引/堆扫描：**数百 ms～数 s**（列宽/缓存依赖） | 全表/索引扫描 **秒级** 常见 | 与「全索引遍历」同类，**非**亚毫秒场景 |
| 非索引列 `WHERE dept` | **~0.64 ms** | 有索引时更低；无索引常 **更差** | 全表过滤 **更差** | StructDB **弱项** 仍为无索引过滤 |

**查询优化里程碑（相对 2026-05-16 基线）**：`QBAL(val,0)` 由 **~75–85 ms** 降至 **~0.003 ms**（聚合缓存）；门禁 `scan_index_ik` 由全列/全表 **~1.1 s** 改为 `5000,STATS` **~3 ms**（语义变更，见 [`PEAK_PERFORMANCE.md`](PEAK_PERFORMANCE.md)）。

**复现与基线**：

- 查询门禁：[`benchmarks/baselines/mdb_query_complex_baseline.json`](../benchmarks/baselines/mdb_query_complex_baseline.json)（25 用例）
- 插入峰值参考：[`benchmarks/baselines/mdb_bulk_insert_peak.json`](../benchmarks/baselines/mdb_bulk_insert_peak.json)
- 对比：`python benchmarks/scripts/compare_mdb_query_summary.py`（`scan_index_ik_stats_full` 默认 ignore）

#### 全量对比结论（能力 × 性能）

| 维度 | StructDB | 上市 RDBMS（PG/MySQL/Oracle/SQL Server） | SQLite | MongoDB |
|------|----------|------------------------------------------|--------|---------|
| **标准 SQL / 生态** | ○ | ● | ●（子集） | ○ API |
| **HA / 复制 / 云** | — | ● | ○ | ● |
| **同进程 bulk 峰值** | ●（~328K 行/秒专线路径） | ●（配置得当时常更高或相当） | ◐ | ●（bulk write API） |
| **一体导入+索引+分析查询** | ◐（~3.8K TPS 加载 + 亚毫秒子集查询） | ● | ◐ | ◐ |
| **通用 OLTP** | ◐（~3K TPS 级 micro） | ● | ● | ● |
| **复杂 SQL（JOIN/窗口）** | ○ | ● | ◐ | ◐ |
| **MDB/newdb 对齐工作台** | ● | — | — | — |

---

## 8. 场景适配矩阵

| 场景 | 更合适 | StructDB |
|------|--------|----------|
| 通用 Web/ERP、复杂 SQL | PostgreSQL / MySQL | ○ |
| 移动端/边缘单文件 SQL | SQLite | ○（除非强绑 MDB/newdb） |
| 缓存、会话、排行榜 | Redis | ○ |
| 应用内 KV，自建上层 | RocksDB | ◐ |
| newdb 命令对齐的本地工作台 | — | ● |
| LSM + embed 事务链研究 | — | ● |
| 多机 HA、读写分离 | PG/Mongo/Redis Cluster | —（非目标） |

---

## 9. 缺口 → 路线对照

| 缺口簇 | 文档 |
|--------|------|
| LSM 深度（L2+、size-tiered） | [`phases/PHASE13_PLUS_PLAN.md`](phases/PHASE13_PLUS_PLAN.md)、[`COMPACTION.md`](COMPACTION.md) |
| 事务与存储对齐 | [`phases/PHASE17.md`](phases/PHASE17.md)、[`phases/PHASE23.md`](phases/PHASE23.md) |
| undo/WAL 分段 | [`phases/PHASE20.md`](phases/PHASE20.md)–[`PHASE22.md`](phases/PHASE22.md) |
| MDB 命令 / NOT_SUPPORTED | [`phases/PHASE25.md`](phases/PHASE25.md) |
| persist 性能 | [`phases/PHASE39_PERSIST_PERF.md`](phases/PHASE39_PERSIST_PERF.md)、[`phases/PHASE40_PERSIST_PERF.md`](phases/PHASE40_PERSIST_PERF.md) |
| 性能 backlog | [`OPTIMIZATION_PLAN.md`](OPTIMIZATION_PLAN.md) |
| **缺口排期与验收** | [`COMPETITIVE_IMPROVEMENT_PLAN.md`](COMPETITIVE_IMPROVEMENT_PLAN.md) |

---

## 10. 一句话定位

**忽略网络后**：StructDB **强于**嵌入式一体化、WAL/恢复文档化、MDB 表语义与 bulk 专线路径；**弱于** SQL 生态、通用索引与优化器、标准 ACID/MVCC、多写并发、Schema 演进、PITR/复制与生产成熟度。不宜作为 PostgreSQL/MySQL 替代品；适合 **SQLite 嵌入位 × RocksDB 存储内核 × 自研 MDB 应用层** 的交叉场景。

---

## 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-16 | 初稿：全维度表、SQLite/RocksDB 细表、PHASE25 缺口、四十期性能边界 |
| 2026-05-16 | 增加 [`COMPETITIVE_IMPROVEMENT_PLAN.md`](COMPETITIVE_IMPROVEMENT_PLAN.md) 索引 |
| 2026-05-16 | §7.2：OLTP 基线路径与 P99 门禁；[`BACKUP_RESTORE_RUNBOOK.md`](BACKUP_RESTORE_RUNBOOK.md) |
| 2026-05-17 | §2.1 上市/头部库索引；§7.3 端到端实测 vs 业界量级；独立 [`PEAK_PERFORMANCE.md`](PEAK_PERFORMANCE.md) |
