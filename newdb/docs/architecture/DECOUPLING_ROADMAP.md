# newdb 解耦路线图（执行摘要）

本仓库按多维度渐进推进「编译边界 / 发布形态 / 查询层 / 组织拆分」。详细阶段、风险与验收口径以团队确认的路线图为准；此处提供 **文档索引** 与 **门禁入口**，避免与模块边界说明重复。

## 文档索引

| 主题 | 文档 |
|------|------|
| 模块依赖方向、Track P / Q | [MODULE_BOUNDARIES.md](MODULE_BOUNDARIES.md) |
| C API 能力分级（Engine / CliEmbed / CliFull） | [../dev/C_API_CAPABILITY_TIERS.md](../dev/C_API_CAPABILITY_TIERS.md) |
| `newdb_core` 禁止 `#include cli/` | [../dev/CORE_SHELL_INCLUDE_BOUNDARY.md](../dev/CORE_SHELL_INCLUDE_BOUNDARY.md) |
| Slim / Full / Plugin 双配置矩阵 | [../dev/CI_SLIM_FULL_MATRIX.md](../dev/CI_SLIM_FULL_MATRIX.md) |
| 运行时 `NEWDB_CLI_BACKEND_PATH` | [../dev/C_API_PLUGIN_BACKEND.md](../dev/C_API_PLUGIN_BACKEND.md) |
| Plugin 打包与 GUI 同步 | [../../scripts/ci/plugin_backend_packaging.md](../../scripts/ci/plugin_backend_packaging.md) |
| 多仓库/包化预备说明 | [MULTI_REPO_PACKAGING.md](MULTI_REPO_PACKAGING.md) |
| `newdb_query` 静态库边界 | [NEWDB_QUERY_LAYER.md](NEWDB_QUERY_LAYER.md) |

## CI 门禁（维持解耦基线）

- **Include 扇入**：`newdb/tools/count_shell_state_includes.py`、`count_bridge_dispatch_includes.py`（及部分 job 中的 `audit_object_includes.py`）。
- **矩阵**：`linux-gcc`、`linux-release-slim-shared`、`linux-c-api-plugin-backend-smoke`、Windows 对应 job；手动/标签发行见 [`.github/workflows/newdb-plugin-release.yml`](../../../.github/workflows/newdb-plugin-release.yml)。详见 [.github/workflows/newdb-ci-reusable.yml](../../../.github/workflows/newdb-ci-reusable.yml)。

## CMake 预设

集成方可使用 [CMakePresets.json](../../CMakePresets.json) 中的 **`plugin-shared-release`**（官方发行）、`plugin-shared`、`slim-shared`、`full-shared` 及对应 `buildPresets` / `testPresets`。

**发行物**：以 **plugin 宿主 + `newdb_cli_backend`** 为默认对外形态；用 `shared_bundle` 目标收集二者与说明文件（见 [plugin_backend_packaging.md](../../scripts/ci/plugin_backend_packaging.md)）。
