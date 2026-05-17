# StructDB / `intro` — 源码级架构手册（LaTeX）

本目录生成 **PDF 手册** `out/structdb-intro.pdf`，内容覆盖：

- **分层引擎**：\texttt{Engine}、\texttt{Orchestrator}、\texttt{ExecutionScheduler}、\texttt{GraphExecutor}、\texttt{ExecutionPlan}
- **存储**：\texttt{StorageEngine}、版本化 KV、WAL、MemTable、LSM/compaction、checkpoint/undo 协同（按头文件逐项梳理）
- **MDB / Embed / C API / GUI**：\texttt{mdb\_ops\_*} 源文件表、\texttt{EmbedClient}、C API 与 Tauri 入口路径
- **参数**：\texttt{EngineConfigSnapshot} 全字段表（默认值 + 语义）
- **测试**：正确性、失败路径、复杂/嵌套场景与 \texttt{gtest\_filter} 示例（含 **PHASE40** \texttt{Mdb.Phase40*} 与 \texttt{Mdb.*} 127 项回归）
- **MDB 导入性能**：三十九/四十期 persist 摊销、分块 WAL、plain/raw 快路径（见 \fpath{Docs/phases/PHASE40\_PERSIST\_PERF.md}）

## 重要区分

| 事项 | 说明 |
|------|------|
| **WSL 用途** | **仅**用于在本机安装 TeX Live 并用 `latexmk` **编译 PDF**。不是「要求在 WSL 里编译 StructDB 数据库」。 |
| **编译引擎/GUI** | 以仓库根 `README.md`、`gui/rust_gui/README.md`、`CMakeLists.txt` 为准（Windows/Linux 皆可）。 |

## 生成 PDF

```bash
cd /mnt/e/db/StructDB/intro
sudo apt update
sudo apt install -y latexmk texlive-xetex texlive-lang-chinese texlive-latex-extra fonts-noto-cjk
chmod +x build_wsl.sh
./build_wsl.sh
```

输出：`intro/out/structdb-intro.pdf`

## 与 newdb/intro 的对应

与 `E:\db\DB\newdb\intro` 相同：**`main.tex`** + **`0x-*/section.tex`** + **`latexmkrc`** + **`build_wsl.sh`**；主题从 newdb heap 栈改为 StructDB 分层引擎与 KV/LSM。

## 新增章节

在 `main.tex` 末尾（`\appendix` 前）增加一行 `\input{12-xxx/section}`，并新建目录与 `section.tex` 即可。
