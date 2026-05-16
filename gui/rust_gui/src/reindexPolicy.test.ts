import { describe, expect, it } from "vitest";
import { buildReindexInsertArgs } from "./reindexPolicy";

describe("reindexPolicy", () => {
  it("builds INSERT args when headers include # column", () => {
    const headers = ["#", "id", "name", "age"];
    const row = ["0", "8", "Alice", "20"];
    expect(buildReindexInsertArgs(headers, row, "1")).toBe("1,Alice,20");
  });

  it("builds INSERT args when headers do not include # column", () => {
    const headers = ["id", "name", "age"];
    const row = ["8", "Bob", "30"];
    expect(buildReindexInsertArgs(headers, row, "2")).toBe("2,Bob,30");
  });

  it("builds INSERT args when id is not the second column", () => {
    const headers = ["name", "id", "age"];
    const row = ["Carol", "9", "28"];
    expect(buildReindexInsertArgs(headers, row, "3")).toBe("3,Carol,28");
  });

  it("treats '-' and '—' placeholders as empty values", () => {
    const headers = ["id", "salary", "note"];
    const row = ["8", "-", "—"];
    expect(buildReindexInsertArgs(headers, row, "1")).toBe("1,,");
  });
});

