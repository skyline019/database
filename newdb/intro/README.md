# newdb/intro（LaTeX 文档工程）

这里用于沉淀 `newdb/src` 的源码解析文档，采用 **LaTeX** 编写，并按“每次解析一个主题/模块建一个文件夹”的方式组织。

---

## 目录结构约定

- `main.tex`: 总入口（汇总各模块章节）
- `latexmkrc`: `latexmk` 构建配置（使用 `xelatex` + `ctex` 支持中文）
- `build_wsl.sh`: 在 WSL(Ubuntu) 下的一键编译脚本
- `0x-*/section.tex`: 每个模块/主题一个文件夹，内容写在 `section.tex`，由 `main.tex` 统一 `\input`

---

## 在 WSL(Ubuntu) 中编译

1) 进入仓库目录（WSL 下路径示例）

```bash
cd /mnt/e/db/DB/newdb/intro
```

2) 安装依赖（建议最小安装；如缺包再补）

```bash
sudo apt update
sudo apt install -y latexmk texlive-xetex texlive-lang-chinese texlive-latex-extra fonts-noto-cjk
```

3) 编译

```bash
./build_wsl.sh
```

生成物在 `out/` 下：

- `out/newdb-intro.pdf`

---

## 添加新模块文档

新增一个文件夹，例如 `07-something/`，并创建 `section.tex`，最后在 `main.tex` 中加入对应的 `\input{07-something/section}`。

