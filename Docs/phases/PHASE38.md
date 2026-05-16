# 三十八期：重排映射与撤销栈一致性

## 1. 背景与目标

- **MDB**：`CONFIRM_REORDER` 从 **NotPortable / `[NOT_SUPPORTED]`** 中拆出，在 **当前 `USE` 表** 上执行 **行锚（KV 主键 / 逻辑 `rows` 映射键）** 的批量重命名；成功后向日志输出一行 **`[REORDER_MAP_JSON]{…}`**（与 GUI 解析字段一致：`table`、`pairs` 为 `[old,new]` 数组）。
- **GUI**：同一引擎输出中的 **每一行** `[REORDER_MAP_JSON]` 均 push 到 `id_remap_chain`（一层一张表），供 `rewrite_stack_line_row_ids` 等路径做撤销栈行号重写。
- **文档**：约定「删行 + 重排」时 `pairs` 与 `DELETE`/`UPDATE` 撤销单元的交互；推荐 **稳定句柄列**（可选 `uuid` / `internal_id`）与 **展示用 `id` 列** 的产品建模（不修改 `mdb$v2$row$` 前缀规则）。

## 2. MDB 语法与语义

- **语法**：`CONFIRM_REORDER({…JSON…})`，其中 JSON 为对象，至少包含：
  - `"table"`：字符串，**必须与当前 `USE` 表名完全一致**；
  - `"pairs"`：`[[old,new],…]`，`old`/`new` 可为 JSON 字符串或十进制数字（与 GUI 一致）。
- **括号**：整段 JSON 包在 **最外层一对圆括号** 内；内层 JSON **勿在字符串值中写入未转义的 `)`**，否则与解析器 `(`…`)` 切片冲突。
- **语义**：
  - 在内存中按 **两阶段临时键**（`__structdb_reorder_tmp__<i>`）应用映射，支持 **互换** 等环形重排；
  - 成功后 **`persist_table`** 重写 `mdb$v2$row$*`、行索引与二级索引（与既有持久化路径一致）；
  - **不得在 `BEGIN`…事务内进行**（与 `RENAME TABLE` / `SET PRIMARY KEY` 同类限制；无 `session.txn` V2 重放条目）。
- **错误**：任一 `old` 缺失、`new` 与现存行冲突、JSON 非法、或 `table` 与 `USE` 不一致 → 整命令失败，**不写**映射行、不部分改表。

## 3. 与撤销栈 / `pairs` 的约定

- **`pairs` 是否包含已删行 id**：以 **引擎实际发出的映射** 为准。若某 `old` 已在存储中删除，则 **不应** 出现在成功提交的 `pairs` 中；GUI 侧仅对已解析到的 `old→new` 做栈行重写。
- **`DELETE` / `UPDATE` 撤销单元**：`id_remap_chain` 按层自栈顶向下应用；与 **PHASE25** 事务日志中的 `DELETE`/`UPDATE` 载荷格式独立，但 **展示行号** 若与行锚一致，应依赖本期的多行 `[REORDER_MAP_JSON]` 摄取以保证链完整。

## 4. 稳定行键与 PK（产品建议）

- **KV 行键**仍由 `mdb_keyspace::row_key(table, pk)` 决定：`pk` 即 **行锚**；重排等价于 **删旧键 + 插新键**（由 `persist_table` 一次性批处理）。
- 若产品需要「UI 上 id 可变、但引用稳定」，建议在表 schema 中增加 **不参与重排的稳定列**（如 `uuid` / `internal_id`），将 **业务外键** 指向该列；**展示列** `id` 可参与 `CONFIRM_REORDER` 映射。本期 **不** 提供自动双键迁移工具。

## 5. GUI：`id_remap_chain` 生命周期

- **`set_workspace`**（切换工作区 `data_dir`）时会 **`id_remap_chain.clear()`** 并 `reset_cap_session`，避免跨库串改。
- 其他路径若仅重启 embed 而未切工作区，仍以 **产品层** 是否在适当时机调用 `set_workspace` / 清空栈为准。

## 6. 验收命令

```text
.\build\tests\Release\mdb_tests.exe --gtest_filter=*Phase38*
```

（`RelWithDebInfo` 下将 `Release` 替换为对应配置。）

## 7. 修订记录

| 日期 | 摘要 |
|------|------|
| 2026-05-14 | 初稿：`CONFIRM_REORDER`、`[REORDER_MAP_JSON]`、GUI 多行摄取、回归用例名 `*Phase38*`。 |
