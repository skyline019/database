export type AttrColumn = {
  name: string;
  ty: string;
};

export function buildDefattrAppendCommand(columns: AttrColumn[], addName: string, addType: string): string {
  const existing = columns
    .map((c) => ({ name: String(c.name ?? "").trim(), ty: String(c.ty ?? "string").trim() || "string" }))
    .filter((c) => !!c.name && c.name !== "#" && c.name.toLowerCase() !== "id");
  const allDefs = [...existing.map((c) => `${c.name}:${c.ty}`), `${addName}:${addType}`];
  return `DEFATTR(${allDefs.join(",")})`;
}

