import { describe, expect, it } from "vitest";
import {
  fieldTypeIsChar,
  isDatetimeLiteralStr,
  isKnownDefattrType,
  validateMdbDefattrCell
} from "./mdbDefattrTypes";

describe("mdbDefattrTypes", () => {
  it("isKnownDefattrType mirrors known_logical_type", () => {
    expect(isKnownDefattrType("int")).toBe(true);
    expect(isKnownDefattrType("INTEGER")).toBe(true);
    expect(isKnownDefattrType("string")).toBe(true);
    expect(isKnownDefattrType("varchar(200)")).toBe(true);
    expect(isKnownDefattrType("text")).toBe(true);
    expect(isKnownDefattrType("char")).toBe(true);
    expect(isKnownDefattrType("char(1)")).toBe(true);
    expect(isKnownDefattrType("float")).toBe(true);
    expect(isKnownDefattrType("double")).toBe(true);
    expect(isKnownDefattrType("datetime")).toBe(true);
    expect(isKnownDefattrType("timestamp")).toBe(true);
    expect(isKnownDefattrType("bool")).toBe(false);
    expect(isKnownDefattrType("date")).toBe(false);
    expect(isKnownDefattrType("")).toBe(false);
  });

  it("fieldTypeIsChar", () => {
    expect(fieldTypeIsChar("char")).toBe(true);
    expect(fieldTypeIsChar("CHAR(5)")).toBe(true);
    expect(fieldTypeIsChar("varchar")).toBe(false);
  });

  it("validateMdbDefattrCell", () => {
    expect(validateMdbDefattrCell("int", "42")).toBeNull();
    expect(validateMdbDefattrCell("int", "x")).toBeTruthy();
    expect(validateMdbDefattrCell("char", "a")).toBeNull();
    expect(validateMdbDefattrCell("char", "ab")).toBeTruthy();
    expect(validateMdbDefattrCell("datetime", "2026-05-13")).toBeNull();
    expect(validateMdbDefattrCell("timestamp", "1715625600")).toBeNull();
    expect(validateMdbDefattrCell("timestamp", "2026-05-13 12:00:00")).toBeNull();
    expect(validateMdbDefattrCell("unknown_legacy", "anything")).toBeNull();
  });

  it("isDatetimeLiteralStr", () => {
    expect(isDatetimeLiteralStr("2026-01-02")).toBe(true);
    expect(isDatetimeLiteralStr("2026-01-02 03:04:05")).toBe(true);
    expect(isDatetimeLiteralStr("2026-01-02T03:04:05")).toBe(true);
    expect(isDatetimeLiteralStr("2026-1-2")).toBe(false);
  });
});
