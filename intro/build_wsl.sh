#!/usr/bin/env bash
# 仅编译本目录 LaTeX → PDF（不编译 StructDB C++ / Tauri 工程）。
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v latexmk >/dev/null 2>&1; then
  echo "latexmk not found. On Ubuntu: sudo apt install -y latexmk texlive-xetex texlive-lang-chinese texlive-latex-extra fonts-noto-cjk" >&2
  exit 1
fi

latexmk -xelatex -jobname=structdb-intro main.tex

echo "OK: out/structdb-intro.pdf"
