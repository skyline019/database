/**
 * DEFATTR 列类型：与 `Client/mdb/src/mdb_runner_ops.cpp` 中 `known_logical_type` / `type_matches` 对齐。
 * 不含 bool、date 等非引擎类型。
 */

export const MDB_DEFATTR_TYPE_OPTIONS = [
  "int",
  "string",
  "varchar",
  "text",
  "char",
  "float",
  "double",
  "datetime",
  "timestamp"
] as const;

export type MdbKnownDefattrType = (typeof MDB_DEFATTR_TYPE_OPTIONS)[number];

function asciiStartsWithCi(s: string, prefix: string): boolean {
  return s.length >= prefix.length && s.slice(0, prefix.length).toLowerCase() === prefix.toLowerCase();
}

/** 与 `field_type_is_char` 一致：`char` 或 `char(...)` */
export function fieldTypeIsChar(typ: string): boolean {
  const s = typ.trim();
  if (!asciiStartsWithCi(s, "char")) return false;
  if (s.length === 4) return true;
  return s.length > 4 && s[4] === "(";
}

/** 与 `known_logical_type` 一致 */
export function isKnownDefattrType(typ: string): boolean {
  const t = typ.trim();
  if (!t) return false;
  if (asciiStartsWithCi(t, "int")) return true;
  if (asciiStartsWithCi(t, "string")) return true;
  if (fieldTypeIsChar(t)) return true;
  if (asciiStartsWithCi(t, "varchar")) return true;
  if (asciiStartsWithCi(t, "text")) return true;
  if (asciiStartsWithCi(t, "float")) return true;
  if (asciiStartsWithCi(t, "double")) return true;
  if (asciiStartsWithCi(t, "datetime")) return true;
  if (asciiStartsWithCi(t, "timestamp")) return true;
  return false;
}

function isIntLiteral(v: string): boolean {
  return /^-?\d+$/.test(v.trim());
}

function isFloatLiteralStr(v: string): boolean {
  let t = v.trim();
  if (t.endsWith("f") || t.endsWith("F")) t = t.slice(0, -1).trim();
  if (!/^-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(t)) return false;
  const n = Number(t);
  return Number.isFinite(n);
}

function isDoubleLiteralStr(v: string): boolean {
  const t = v.trim();
  if (!/^-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(t)) return false;
  const n = Number(t);
  return Number.isFinite(n);
}

/** 与 `is_datetime_literal` 常见形态一致：仅日期 10 字符，或 19 字符含 ` ` / `T` 与时间 */
export function isDatetimeLiteralStr(v: string): boolean {
  const s = v.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return true;
  if (s.length === 19 && (s[10] === " " || s[10] === "T")) {
    return /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$/.test(s);
  }
  return false;
}

/**
 * 与 `type_matches` 对齐的单元格校验（用于网格编辑预览）。
 * 未知类型返回 null（不拦截），便于兼容旧元数据。
 */
export function validateMdbDefattrCell(typeStr: string, raw: string): string | null {
  const v = raw.trim();
  if (v === "") return null;
  const t = typeStr.trim();
  if (!isKnownDefattrType(t)) return null;

  if (asciiStartsWithCi(t, "int")) {
    return isIntLiteral(v) ? null : "应为整数";
  }
  if (asciiStartsWithCi(t, "string") || asciiStartsWithCi(t, "varchar") || asciiStartsWithCi(t, "text")) {
    return null;
  }
  if (fieldTypeIsChar(t)) {
    return v.length <= 1 ? null : "char 列最多 1 个字符（或留空）";
  }
  if (asciiStartsWithCi(t, "float")) {
    return isFloatLiteralStr(v) ? null : "应为 float 字面量（可选 f 后缀）";
  }
  if (asciiStartsWithCi(t, "double")) {
    return isDoubleLiteralStr(v) ? null : "应为 double 字面量";
  }
  if (asciiStartsWithCi(t, "datetime")) {
    return isDatetimeLiteralStr(v) ? null : "应为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS";
  }
  if (asciiStartsWithCi(t, "timestamp")) {
    if (isIntLiteral(v) || isDatetimeLiteralStr(v)) return null;
    return "应为整数时间戳或 datetime 格式";
  }
  return null;
}
