# newdb 设计点逐条讲解（含源码映射）

本文按“设计点 -> 作用/行为 -> 对应文件”的方式，梳理当前 `newdb` 的核心实现，便于后续评审、回归和继续优化。

---

## 1. 数据结构设计（Data Structures）

### 1.1 结构化 Schema 与属性类型约束
- **设计点**：通过 `DEFATTR` 建立表属性类型（如 `int/string/bool/date/datetime`）并在 DML 路径做统一校验。
- **作用/行为**：保证 `INSERT/UPDATE/SETATTR` 输入类型不匹配时直接报错，不写入脏数据。
- **对应文件**：
  - `newdb/demo/demo_commands.cc`（类型校验、DML 写入前检查、schema 刷新兜底）

### 1.2 Session 运行态与 schema 失配修复
- **设计点**：当 Session 内 schema 缺失/过期时，执行命令前尝试从 sidecar 重新加载 schema。
- **作用/行为**：避免因运行态丢失导致“本应报 mismatch 却误判为 ok”的问题。
- **对应文件**：
  - `newdb/demo/demo_commands.cc`（`refresh_schema_if_missing` 相关逻辑）

### 1.3 C API 错误语义结构化输出
- **设计点**：在 C API 层把业务错误上提为可机器识别的结构化错误前缀（`[CAPI_ERROR]`）。
- **作用/行为**：Rust/Tauri/GUI 可以稳定提取 error code，不再依赖脆弱字符串猜测。
- **对应文件**：
  - `newdb/src/c_api.cpp`（`output_indicates_business_error`、`prepend_capi_error_line`、`last_error` 赋值）

---

## 2. 核心算法与执行语义（Algorithms & Semantics）

### 2.1 DML 统一前置校验流水线
- **设计点**：解析命令 -> 确认 schema -> 校验类型 -> 再进入写路径。
- **作用/行为**：先验失败即停止，保证错误语句不落盘，数据保持上一条成功语句状态。
- **对应文件**：
  - `newdb/demo/demo_commands.cc`

### 2.2 脚本执行“遇错即停”
- **设计点**：`--run-mdb` 与 GUI script path 都执行“first failure stop”。
- **作用/行为**：脚本在 mismatch 等业务错误处停止，并报告停在第几行。
- **对应文件**：
  - `newdb/demo/demo_shell.cc`（CLI 脚本停机判断与 stop line 输出）
  - `newdb/rust_gui/src-tauri/src/lib.rs`（`run_script_ex`、错误提取和 stopLine 返回）

### 2.3 命令执行结构化返回（command_ex）
- **设计点**：将命令结果升级为结构体（`ok/output/errorCode/...`）而非纯文本。
- **作用/行为**：前端可基于字段做策略决策（是否中断、是否写历史、是否刷新）。
- **对应文件**：
  - `newdb/rust_gui/src-tauri/src/lib.rs`（`CommandExecResult`、`execute_command_ex`）
  - `newdb/rust_gui/src/App.vue`（调用 `execute_command_ex` 并消费 `errorCode`）

---

## 3. 性能与稳定性设计（Performance & Robustness）

### 3.1 PAGE 结果解析容错与回填
- **设计点**：`PAGE_JSON` 不完整时，回填 columns/headers；文本 PAGE 解析时增强分隔线/列识别容错。
- **作用/行为**：降低 GUI “nodata” 假空表概率，提高异常输出场景下可视化稳定性。
- **对应文件**：
  - `newdb/rust_gui/src-tauri/src/lib.rs`（`query_page` 增强解析与回填）

### 3.2 前端页面应用策略（防误覆盖）
- **设计点**：刷新结果在可疑空结果/错误提示下不覆盖当前已展示有效数据。
- **作用/行为**：后端暂时异常时，前端保留“最后一次正确数据”，避免用户看到空表误判数据丢失。
- **对应文件**：
  - `newdb/rust_gui/src/pagePolicy.ts`（`shouldApplyPageResult`）
  - `newdb/rust_gui/src/App.vue`（`refreshPage` 应用策略）

### 3.3 运行时工件可观测性
- **设计点**：GUI 启动时展示 GUI/demo/dll 路径及修改时间。
- **作用/行为**：快速定位“跑的是旧二进制”类问题，降低排障时间。
- **对应文件**：
  - `newdb/rust_gui/src-tauri/src/lib.rs`（`runtime_artifact_info`）
  - `newdb/rust_gui/src/App.vue`（启动时读取并展示）

---

## 4. GUI 架构与交互策略（Vue + Tauri + C API）

### 4.1 命令层策略：错误即阻断后续 UI 误动作
- **设计点**：`INSERT/UPDATE` 失败后，触发 stop-and-skip-history 策略。
- **作用/行为**：避免失败命令被当成成功历史，减少误导与二次误操作。
- **对应文件**：
  - `newdb/rust_gui/src/commandPolicy.ts`
  - `newdb/rust_gui/src/App.vue`

### 4.2 脚本层策略：统一展示 stopLine + errorCode
- **设计点**：脚本返回结构体中包含 `errorCode` 和 `stopLine`。
- **作用/行为**：GUI 能明确提示“第 N 行停止”而非笼统失败。
- **对应文件**：
  - `newdb/rust_gui/src-tauri/src/lib.rs`
  - `newdb/rust_gui/src/App.vue`

### 4.3 跨层错误契约闭环
- **设计点**：C++ 业务错误 -> C API 前缀化 -> Rust 解析 -> Vue 决策。
- **作用/行为**：实现同一错误在 CLI/DLL/GUI 的一致语义，减少路径分叉导致的不一致行为。
- **对应文件**：
  - `newdb/src/c_api.cpp`
  - `newdb/rust_gui/src-tauri/src/lib.rs`
  - `newdb/rust_gui/src/App.vue`

---

## 5. 测试体系与回归防线（Testing）

### 5.1 C++：CLI 脚本语义回归
- **设计点**：覆盖 `--run-mdb` 下 mismatch 停机且保持前序数据。
- **作用/行为**：防止脚本路径再次出现“错误不停止/错误写入”回归。
- **对应文件**：
  - `newdb/tests/test_demo_mdb_stop.cpp`
  - `newdb/CMakeLists.txt`（测试目标纳入）

### 5.2 C++：C API 契约回归
- **设计点**：校验 mismatch 时 `newdb_session_execute` 返回失败、`last_error` 正确、输出带 `[CAPI_ERROR]`。
- **作用/行为**：保证 DLL/GUI 依赖的 ABI 契约长期稳定。
- **对应文件**：
  - `newdb/tests/test_c_api.cpp`
  - `newdb/src/c_api.cpp`

### 5.3 Rust：错误解析与结果封装单测
- **设计点**：测试 error code 提取、前缀剥离、stop line 解析、结构化结果构建。
- **作用/行为**：防止桥接层“字符串处理”细节回归。
- **对应文件**：
  - `newdb/rust_gui/src-tauri/src/lib.rs`（`#[cfg(test)]` 模块）

### 5.4 Frontend：策略单测
- **设计点**：测试命令策略与页面应用策略。
- **作用/行为**：保证前端在异常路径下仍保持正确 UX（不误清空、不误记成功）。
- **对应文件**：
  - `newdb/rust_gui/src/commandPolicy.test.ts`
  - `newdb/rust_gui/src/pagePolicy.test.ts`

---

## 6. CI/发布门禁（Quality Gates）

### 6.1 一体化验证脚本
- **设计点**：统一在 PowerShell 验证脚本中串联 C++/Rust/Frontend gates。
- **作用/行为**：单入口复现完整质量门禁，减少“本地过、集成不过”。
- **对应文件**：
  - `newdb/scripts/ci/verify_clean_reconfigure.ps1`

### 6.2 ReleaseGrade 强制策略
- **设计点**：`-ReleaseGrade` 下禁止任何 Skip 参数。
- **作用/行为**：发布级验证不允许跳项，提升出包可信度。
- **对应文件**：
  - `newdb/scripts/ci/verify_clean_reconfigure.ps1`

---

## 7. 当前结论（工程状态）

- mismatch 的“报错 + 停止 + 保留上一条成功数据”语义，已从 C++ 核心贯通到 C API、Rust/Tauri、Vue GUI。
- 当前主要剩余提升方向：GUI 端到端（浏览器级）自动化覆盖、以及更正式的安全治理门禁（依赖审计/威胁建模文档化）。

