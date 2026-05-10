# `newdb_query` 库边界（Track Q 第一步）

## 角色

[`newdb_query`](../../CMakeLists.txt) 是 **静态库**，当前承载原 `cli/modules/where` 下由 CMake 列出的翻译单元（parser / executor / plan / stats）。目标是把 **WHERE 解析与计划** 从「仅作为 shell OBJECT 砖」提升为 **可独立链接的编译单元集合**，便于后续继续向引擎侧迁移符号与能力分级（见 [C_API_CAPABILITY_TIERS.md](../dev/C_API_CAPABILITY_TIERS.md)）。

## 依赖

- **PUBLIC** [`newdb_core`](../../CMakeLists.txt)（与 `waterfall` 由 core 传递）。
- **不** 依赖其他 CLI OBJECT 砖；WHERE 与 catalog / txn / sidecar 的交互仍通过头文件与 **最终** 链接 `newdb_capi_adapter` / 测试可执行文件时合入的符号完成（与迁移前一致）。

## 与 `newdb_capi_adapter` 的关系

[`newdb_capi_adapter`](../../CMakeLists.txt) 继续由多个 shell OBJECT 砖合成，并对 **`newdb_query` 使用 PUBLIC 链接**，使嵌入方与 `newdb_cli_backend` 与迁移前一样只需链接适配器（及 core）。

## 后续工作（路线图）

- 将 `newdb_session_where_plan_json` 的实现逐步迁入 `newdb_query` 或 `newdb_core` 可控表面，并更新能力分级表。
- 收紧 `cli/modules/where` 与 `cli/shell` 之间的 include，必要时引入窄接口头。

### Track Q 检查清单（迭代时自证）

1. **编译边界**：`newdb_query` 源文件不 `#include` `cli/shell/dispatch/router/dispatch.h`（避免 query 砖依赖 shell 路由）；若 C API 路径必须调用 `ShellState::emit_where_plan_json`，保持经 [`c_api_cli_bridge`](../../cli/shell/c_api/c_api_cli_bridge.cc) / facade 的窄入口。
2. **能力文档**：每迁出一批符号，更新本文件与 [`C_API_CAPABILITY_TIERS.md`](../dev/C_API_CAPABILITY_TIERS.md) 的 Notes 列（是否仍属 CliEmbedTier-only）。
3. **Plugin**：宿主与 backend 成对发布不变；`where_plan` 逻辑仅在 backend DLL 内时仍满足「同树构建」约束（见 [`plugin_backend_packaging.md`](../../scripts/ci/plugin_backend_packaging.md)）。
