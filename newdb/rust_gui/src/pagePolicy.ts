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
  return (!hasErrorHint || hasUsefulData) && !looksLikePageButParsedEmpty;
}

