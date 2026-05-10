# newdb 文档索引

本目录按主题分子文件夹，便于检索与维护。

| 目录 | 内容 |
|------|------|
| [`api/`](api/) | C API：ABI 矩阵、错误处理、线程安全 |
| [`architecture/`](architecture/) | [模块边界与装配图](architecture/MODULE_BOUNDARIES.md)（`newdb_shell_*` OBJECT、`newdb_query`、`newdb_capi_adapter`、plugin/slim）；[解耦路线图索引](architecture/DECOUPLING_ROADMAP.md)；[全仓库数据流总览](architecture/PROJECT_DATAFLOW_WHOLE.md) 与 [逐项详解](architecture/PROJECT_DATAFLOW_WHOLE_详解.md)；设计点映射见 [NEWDB_DESIGN_POINT_TO_FILE_MAP.md](architecture/NEWDB_DESIGN_POINT_TO_FILE_MAP.md) |
| [`txn/`](txn/) | 事务隔离、锁与 catalog 运维语义 |
| [`testing/`](testing/) | 故障注入与事务/WAL 测试矩阵 |
| [`storage/`](storage/) | 存储治理、恢复与运行时预算 |
| [`ci/`](ci/) | 性能/CI 门禁、分支保护基线 |
| [`dev/`](dev/) | 构建与本地/CI 验证命令 |
| [`roadmap/`](roadmap/) | 优化路线图、评估、文件级修改计划、对标文档 |

从仓库根目录引用时，路径形如 `newdb/docs/<子目录>/<文件名>.md`。
