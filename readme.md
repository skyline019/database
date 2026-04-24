# waterfall

日照香炉生紫烟，遥看瀑布挂前川。
飞流直下三千尺，疑是银河落九天。

## 源码编译

为避免生成的中间文件污染源码目录，不允许在源码目录直接编译。当build目录为空，使用如下命令执行编译：

```bash
cd build
make -f ../Makefile
```

如果在build目录下已经编译过，则可以直接make即可。

## 目录组织

waterfall根目录主要是4个子目录：waterfall、build、third、rules，分别是源码目录、编译目录、三方库目录、Makefile规则目录。

在源码目录下，按照模块划分各个子目录，一个模块对应一个子目录。每个模块目录下直接放头文件，src目录放源码、test目录放单元测试程序、doc目录放文档。

源码的组织以模块为中心，所有制品都放在该模块的目录下，构建和测试由Makefile负责。

## docker

在fedora环境下，需安装如下软件包：

```
gcc clang++ gdb libasan libubsan cmake clang-tools-extra gtest-devel mimalloc-devel
texlive texlive-xecjk texlive-lipsum texlive-pdfcrop texlive-ps2eps latexmk
```