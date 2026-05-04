export type PageLikeResult = {
  headers: string[];
  rows: string[][];
  raw: string;
};

export function shouldApplyPageResult(next: PageLikeResult): boolean {
  const raw = next.raw || "";
  const hasErrorHint = /error|fail|invalid|load_failed|filesystem error/i.test(raw);
  const hasUsefulData = next.rows.length > 0 || next.headers.length > 1;
  const looksLikePageButParsedEmpty = /\[PAGE\]/i.test(raw) && !hasUsefulData;
  const looksLikeShowPlanJson =
    /"plan_id"\s*:/.test(raw) &&
    /"plan_candidates"\s*:\s*\[/.test(raw) &&
    /"table_stats_stale"\s*:/.test(raw);
  return (
    ((!hasErrorHint || hasUsefulData) && !looksLikePageButParsedEmpty) ||
    looksLikeShowPlanJson
  );
}

