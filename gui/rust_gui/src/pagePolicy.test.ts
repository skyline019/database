import { describe, expect, it } from "vitest";
import { shouldApplyPageResult } from "./pagePolicy";

describe("pagePolicy", () => {
  it("applies useful page data", () => {
    expect(
      shouldApplyPageResult({
        headers: ["#", "id", "name", "age"],
        rows: [["0", "1", "Alice", "20"]],
        raw: "[PAGE] table=t1 order_by=id asc"
      })
    ).toBe(true);
  });

  it("rejects parsed-empty PAGE payload to avoid nodata flicker", () => {
    expect(
      shouldApplyPageResult({
        headers: ["id"],
        rows: [],
        raw: "[PAGE] table=t1 order_by=id asc\nPage 1 / 1"
      })
    ).toBe(false);
  });

  it("keeps existing data when raw has explicit error hints", () => {
    expect(
      shouldApplyPageResult({
        headers: ["id"],
        rows: [],
        raw: "[ERROR] load_failed"
      })
    ).toBe(false);
  });
});

