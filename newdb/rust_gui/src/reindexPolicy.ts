export function buildReindexInsertArgs(headers: string[], row: string[], newId: string): string {
  const idxId = headers.findIndex((h) => h.trim().toLowerCase() === "id");
  const idIdx = idxId >= 0 ? idxId : 1;
  const hashIdx = headers.findIndex((h) => h.trim() === "#");
  const values = row
    .filter((_, idx) => idx !== idIdx && idx !== hashIdx)
    .map((v) => {
      const t = String(v ?? "").trim();
      // UI placeholders for "empty" cells must not be written back during reindex.
      if (t === "-" || t === "—") return "";
      return t;
    });
  return [newId, ...values].join(",");
}

