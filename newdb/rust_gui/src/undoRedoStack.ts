export type OperationType =
  | "table_create"
  | "table_drop"
  | "table_rename"
  | "table_use"
  | "schema_defattr"
  | "schema_set_pk"
  | "data_insert"
  | "data_delete"
  | "data_update"
  | "query_where"
  | "query_wherep"
  | "query_page"
  | "aggregate"
  | "txn_begin"
  | "txn_commit"
  | "txn_rollback"
  | "txn_savepoint"
  | "txn_rollback_to"
  | "txn_release_savepoint"
  | "pitr_recover_lsn"
  | "pitr_recover_time"
  | "generic";

export type UndoOp = {
  type: OperationType;
  title: string;
  forward: string;
  backward?: string;
  table?: string;
  time: string;
  ok?: boolean;
};

export type UndoUnitStatus = "open" | "ready" | "applied" | "failed" | "frozen";
export type UndoUnit = {
  unitId: string;
  txnId: string;
  savepointName: string;
  tablesTouched: string[];
  ops: UndoOp[];
  createdAt: string;
  status: UndoUnitStatus;
  lastError?: string;
};

export type TxnSessionRecorder = {
  active: boolean;
  txnId: string;
  currentSavepoint: string;
  pendingOps: UndoOp[];
};

export function initialRecorder(): TxnSessionRecorder {
  return { active: false, txnId: "", currentSavepoint: "__autosave__", pendingOps: [] };
}

export function normalizeCmd(v: string): string {
  return v.trim().replace(/\s+/g, " ");
}

export function buildUndoUnit(
  savepointName: string,
  ops: UndoOp[],
  nowText: string,
  txnId: string,
  currentTable: string,
  status: UndoUnitStatus = "ready",
  unitId?: string
): UndoUnit {
  const id = unitId || `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const tables = Array.from(new Set(ops.map((x) => x.table || currentTable).filter((x) => !!x)));
  return {
    unitId: id,
    txnId: txnId || `txn-${Date.now()}`,
    savepointName,
    tablesTouched: tables as string[],
    ops,
    createdAt: nowText,
    status
  };
}

export function trackTxnCommand(
  recorder: TxnSessionRecorder,
  op: UndoOp,
  nowText: string,
  currentTable: string
): { recorder: TxnSessionRecorder; emittedUnits: UndoUnit[] } {
  const cmd = normalizeCmd(op.forward).toUpperCase();
  const next: TxnSessionRecorder = {
    active: recorder.active,
    txnId: recorder.txnId,
    currentSavepoint: recorder.currentSavepoint,
    pendingOps: [...recorder.pendingOps]
  };
  const emitted: UndoUnit[] = [];
  if (cmd.startsWith("BEGIN")) {
    next.active = true;
    next.txnId = `txn-${Date.now()}`;
    next.currentSavepoint = "__autosave__";
    next.pendingOps = [];
    return { recorder: next, emittedUnits: emitted };
  }
  if (!next.active) {
    emitted.push(buildUndoUnit("__single__", [op], nowText, next.txnId, currentTable));
    return { recorder: next, emittedUnits: emitted };
  }
  if (cmd.startsWith("SAVEPOINT ")) {
    const sp = op.forward.replace(/^SAVEPOINT\s+/i, "").trim() || "__savepoint__";
    const chunk = next.pendingOps.splice(0);
    if (chunk.length > 0) {
      emitted.push(buildUndoUnit(sp, chunk, nowText, next.txnId, currentTable));
    }
    next.currentSavepoint = sp;
    return { recorder: next, emittedUnits: emitted };
  }
  if (cmd.startsWith("COMMIT")) {
    const chunk = next.pendingOps.splice(0);
    if (chunk.length > 0) {
      emitted.push(buildUndoUnit("__commit__", chunk, nowText, next.txnId, currentTable));
    }
    next.active = false;
    return { recorder: next, emittedUnits: emitted };
  }
  if (cmd.startsWith("ROLLBACK")) {
    next.pendingOps = [];
    next.active = false;
    return { recorder: next, emittedUnits: emitted };
  }
  next.pendingOps.push(op);
  return { recorder: next, emittedUnits: emitted };
}

