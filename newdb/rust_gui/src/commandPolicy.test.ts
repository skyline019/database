import { describe, expect, it } from "vitest";
import { hasCommandError, isInsertOrUpdateCommand, shouldStopAndSkipHistory } from "./commandPolicy";

describe("commandPolicy", () => {
  it("matches INSERT and UPDATE commands", () => {
    expect(isInsertOrUpdateCommand("INSERT(1,Alice,20)")).toBe(true);
    expect(isInsertOrUpdateCommand("  update(1,Alice,30)")).toBe(true);
    expect(isInsertOrUpdateCommand("DELETE(1)")).toBe(false);
  });

  it("detects type mismatch and generic errors", () => {
    expect(hasCommandError("[UPDATE] attribute 'age' expects int, got 'a'")).toBe(true);
    expect(hasCommandError("[ERROR] command failed: unknown exception")).toBe(true);
    expect(hasCommandError("[INSERT] duplicate id=1 in table 't', insert rejected.")).toBe(true);
    expect(hasCommandError("[INSERT] ok", "execution_failed")).toBe(true);
  });

  it("does not treat success output as error", () => {
    expect(hasCommandError("[INSERT] ok: table='users' now has 1 rows.")).toBe(false);
    expect(hasCommandError("[UPDATE] ok: id=1 updated (table='users').")).toBe(false);
  });

  it("stops and skips history only for failed insert/update", () => {
    expect(shouldStopAndSkipHistory("UPDATE(1,Alice,a)", "[UPDATE] attribute 'age' expects int, got 'a'")).toBe(true);
    expect(shouldStopAndSkipHistory("INSERT(1,Alice,20)", "[INSERT] ok: table='users' now has 1 rows.")).toBe(false);
    expect(shouldStopAndSkipHistory("DELETE(1)", "[DELETE] usage: DELETE(id)")).toBe(false);
  });
});
