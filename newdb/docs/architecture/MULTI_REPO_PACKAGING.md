# 多仓库 / 包化预备（阶段 4）

在单仓库内完成 **Track P（plugin）**、**OBJECT 链接图**、**`newdb_query`** 等里程碑后，若要将 `waterfall`、`newdb_core`、`newdb_query`、`newdb_capi_adapter`/CLI 拆成独立包，建议按下述原则降低耦合成本。

## 版本策略

- **语义化版本**：对外 C ABI（`newdb/c_api.h`）与 plugin 导出表（若保留）分别文档化兼容性承诺。
- **同构建树锁定**：宿主与 `newdb_cli_backend` 须来自 **同一配置矩阵**（同一 `NEWDB_C_API_PLUGIN_BACKEND` / 编译器 / CRT），见 [plugin_backend_packaging.md](../../scripts/ci/plugin_backend_packaging.md)。

## 集成方式（择一）

| 方式 | 适用 |
|------|------|
| `FetchContent` | 少量 C++ 依赖、可接受源码拉取 |
| `find_package` + 预编译包 | 对内发行二进制 SDK |
| Git submodule + `add_subdirectory` | 与当前 monorepo 最接近的过渡 |

## 契约测试下沉

拆仓后应保留 **组合级** 测试（最小宿主进程加载 `libnewdb` + backend），对应现有 `CliBackendPluginSmoke` 思路；引擎单测留在 core 包，全量 shell 测试留在 CLI 包或聚合 CI。

## 与当前仓库的关系

本文件仅为 **组织拆分 checklist**；默认开发与 CI 仍以单仓库 [MODULE_BOUNDARIES.md](MODULE_BOUNDARIES.md) 为准。
