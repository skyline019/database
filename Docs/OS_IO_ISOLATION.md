# OS 级 compaction 与 WAL I/O 隔离（实践清单）

StructDB 已具备：**专用 `CompactionIoExecutor` 线程池**、compaction worker 低优先级线程、SST 读 **`FILE_FLAG_SEQUENTIAL_SCAN` / `posix_fadvise(POSIX_FADV_SEQUENTIAL)`**、WAL 与 merge **不同代码路径**（merge 不触碰 `WalWriter`）。本页补充 **操作系统层**可做的隔离，便于在噪声磁盘或共享盘上压测。

## 1. 进程 / 线程调度（Linux）

- **`ionice -c2 -n0`**（best-effort 空闲类）对 compaction worker 已部分对齐（`nice` / `SCHED_IDLE` 尽力）；批处理 job 可在外层用 `ionice` 启动整个引擎进程对比 P99。
- **CPU 亲和**：将 WAL 前台线程与 compaction 线程绑不同核可减少 L3 争用；需运维侧 `taskset`/`cpuset` 或后续引擎内可选亲和掩码（当前未内置，避免误绑笔记本小核）。

## 2. I/O 栈与设备

- **分卷**：数据目录 `data_dir` 与 `wal.log` 放在不同物理盘时，OS 页缓存与队列天然分离；单盘 NVMe 上收益有限但仍减少同队列上读写的交替排队。
- **cgroup v2 `io.weight` / `io.max`**：对 compaction 子 cgroup 降权，对前台 WAL 写保留权重（需容器/systemd 集成）。

## 3. Linux：WAL 追加路径提示

- 对 **`wal.log` 追加打开**的 `FileWriter`，在 `open(O_APPEND)` 成功后调用 **`posix_fadvise(..., POSIX_FADV_SEQUENTIAL)`**（见 `file_handle.cpp`），提示内核该 fd 主要为顺序写，与 compaction 的随机大块读形成对照（非强制隔离，零语义变更）。

## 4. Windows

- WAL 与 SST 已用不同 **句柄**；可进一步用 **不同卷**、关闭索引以外的后台扫描器（Defender 排除数据目录）减少与 compaction 读放大叠加。

## 5. 与产品文档的关系

- 详细行为仍以 `Docs/COMPACTION.md`、`Docs/PHASE21.md`（WAL pipeline）、`Docs/OPTIMIZATION_PLAN.md` 为准；本文件仅作 **OS 级**部署与实验索引。
