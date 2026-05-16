/**
 * StructDB GUI：命令输出判定与（预留）运行时指标分组。
 * 历史 newdb 的 `where_plan_json` / `runtime_stats` C API 在 StructDB 中不可用；相关常量已移除。
 */

export function isInsertOrUpdateCommand(text: string): boolean {
  const t = text.trim().toUpperCase();
  return t.startsWith("INSERT(") || t.startsWith("UPDATE(");
}

export function hasCommandError(raw: string, errorCode?: string | null): boolean {
  if (errorCode && errorCode.toLowerCase() !== "ok") return true;
  const t = raw || "";
  if (/\[CAPI_ERROR\]\s+code=/i.test(t)) return true;
  if (/\[NOT_SUPPORTED\]/i.test(t)) return true;
  if (/\[ERROR\]/i.test(t)) return true;
  if (/expects .*?, got '/i.test(t)) return true;
  if (/\[SHOW PLAN\]\s+(blocked|invalid|usage)/i.test(t)) return true;
  if (/\b(fail|failed|invalid|duplicate|missing|usage)\b/i.test(t)) return true;
  return false;
}

export function shouldStopAndSkipHistory(
  command: string,
  rawOutput: string,
  errorCode?: string | null
): boolean {
  if (!isInsertOrUpdateCommand(command)) return false;
  return hasCommandError(rawOutput, errorCode);
}

/** 预留：StructDB 暂无与 newdb 对齐的 `SHOW TUNING JSON` runtime_stats 键表；仪表盘仅展示占位或文件型结果。 */
export const RUNTIME_TUNING_DIAGNOSTIC_GROUPS: ReadonlyArray<{ title: string; keys: readonly string[] }> = [];
