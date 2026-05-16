import { describe, expect, it } from "vitest";
import { buildAddAttrCommand } from "./attrPolicy";

describe("attrPolicy", () => {
  it("builds ADDATTR for a single new column", () => {
    expect(buildAddAttrCommand("salary", "int")).toBe("ADDATTR(salary:int)");
  });

  it("trims name and type", () => {
    expect(buildAddAttrCommand("  note  ", "  text  ")).toBe("ADDATTR(note:text)");
  });
});
