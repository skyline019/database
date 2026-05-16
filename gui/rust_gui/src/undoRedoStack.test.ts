import { describe, expect, it } from "vitest";
import { buildUndoUnit, initialRecorder, trackTxnCommand, type UndoOp } from "./undoRedoStack";

function op(forward: string, table = "users"): UndoOp {
  return {
    type: "generic",
    title: forward,
    forward,
    table,
    time: "10:00:00"
  };
}

describe("undoRedoStack", () => {
  it("creates savepoint-level unit on SAVEPOINT boundary", () => {
    let rec = initialRecorder();
    rec = trackTxnCommand(rec, op("BEGIN"), "10:00:00", "users").recorder;
    rec = trackTxnCommand(rec, op("INSERT(1,Alice,20)"), "10:00:01", "users").recorder;
    const r2 = trackTxnCommand(rec, op("SAVEPOINT sp1"), "10:00:02", "users");
    expect(r2.emittedUnits.length).toBe(1);
    expect(r2.emittedUnits[0].savepointName).toBe("sp1");
    expect(r2.emittedUnits[0].ops.length).toBe(1);
  });

  it("flushes pending ops as commit unit", () => {
    let rec = initialRecorder();
    rec = trackTxnCommand(rec, op("BEGIN"), "10:00:00", "users").recorder;
    rec = trackTxnCommand(rec, op("UPDATE(1,Alice,21)"), "10:00:01", "users").recorder;
    const r = trackTxnCommand(rec, op("COMMIT"), "10:00:02", "users");
    expect(r.emittedUnits.length).toBe(1);
    expect(r.emittedUnits[0].savepointName).toBe("__commit__");
    expect(r.recorder.active).toBe(false);
  });

  it("supports cross-table tableTouched dedup", () => {
    const unit = buildUndoUnit(
      "spx",
      [op("UPDATE(1,a)", "users"), op("INSERT(2,b)", "orders"), op("DELETE(3)", "users")],
      "10:00:00",
      "txn-x",
      "users",
      "ready",
      "u1"
    );
    expect(unit.tablesTouched.sort()).toEqual(["orders", "users"]);
  });
});

