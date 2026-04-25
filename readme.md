# database

[English](README.en.md) | 中文

一个以教学与工程实践为导向的数据库实验仓库，包含：

- `waterfall/`：底层存储与基础模块
- `newdb/`：数据库引擎（Heap Page / WAL / MVCC）、CLI、测试体系与 Rust GUI

## 学术价值与研究可读性

- **实验方法导向**：围绕页式存储、WAL 恢复与 MVCC 可见性构建可观测实现，支持从机制假设到行为验证的分析路径。
- **可重复性保障**：提供标准化构建流程、回归测试与运行时质量门禁，降低环境差异对实验结论的干扰。
- **效度意识明确**：通过模块化分层与文档化对照，兼顾内部效度（机制一致性与结果可解释性）与外部效度（迁移到相近系统场景的参考价值）。

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