#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v latexmk >/dev/null 2>&1; then
  echo "latexmk not found. On Ubuntu: sudo apt install -y latexmk texlive-xetex texlive-lang-chinese texlive-latex-extra" >&2
  exit 1
fi

# latexmk will auto-load ./latexmkrc in this directory.
latexmk -xelatex -jobname=newdb-intro main.tex

echo "OK: out/newdb-intro.pdf"

