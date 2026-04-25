# database

[English](README.en.md) | 中文

一个以教学与工程实践为导向的数据库实验仓库，包含：

- `waterfall/`：底层存储与基础模块
- `newdb/`：数据库引擎（Heap Page / WAL / MVCC）、CLI、测试体系与 Rust GUI

## 为什么值得看

- **端到端链路完整**：从页存储、日志恢复、事务可见性到 GUI 操作与趋势看板
- **工程化闭环**：有回归测试、CI 工作流、脚本化压测与运行时质量门禁
- **可读文档齐全**：提供按模块拆解的源码解析与复评文档

## 快速入口

- 项目源码：`newdb/`
- 图形界面：`newdb/rust_gui/`
- 深度文档（PDF）：`newdb/intro/out/newdb-intro.pdf`
- 开发者手册：`docs/dev-guide.md`

## 文档导航

- 源码解析工程：`newdb/intro/`
- 开发与构建说明：`docs/dev-guide.md`

## 仓库地址

- GitHub: [skyline019/database](https://github.com/skyline019/database)