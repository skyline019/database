import { describe, expect, it } from "vitest";
import { buildDefattrAppendCommand } from "./attrPolicy";

describe("attrPolicy", () => {
  it("builds DEFATTR command by appending new attribute without overwriting existing ones", () => {
    const cmd = buildDefattrAppendCommand(
      [
        { name: "#", ty: "string" },
        { name: "id", ty: "int" },
        { name: "name", ty: "string" },
        { name: "dept", ty: "string" },
        { name: "age", ty: "int" }
      ],
      "salary",
      "int"
    );
    expect(cmd).toBe("DEFATTR(name:string,dept:string,age:int,salary:int)");
  });

  it("excludes # and id columns from DEFATTR command", () => {
    const cmd = buildDefattrAppendCommand(
      [
        { name: "#", ty: "string" },
        { name: "id", ty: "int" },
        { name: "name", ty: "string" }
      ],
      "age",
      "int"
    );
    expect(cmd).toBe("DEFATTR(name:string,age:int)");
  });
});

