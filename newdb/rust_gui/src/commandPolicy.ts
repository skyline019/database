export function isInsertOrUpdateCommand(text: string): boolean {
  const t = text.trim().toUpperCase();
  return t.startsWith("INSERT(") || t.startsWith("UPDATE(");
}

export function hasCommandError(raw: string, errorCode?: string | null): boolean {
  if (errorCode && errorCode.toLowerCase() !== "ok") return true;
  const t = raw || "";
  if (/\[CAPI_ERROR\]\s+code=/i.test(t)) return true;
  if (/\[ERROR\]/i.test(t)) return true;
  if (/expects .*?, got '/i.test(t)) return true;
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
