<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { WebviewWindow } from "@tauri-apps/api/webviewWindow";
import { shouldStopAndSkipHistory } from "./commandPolicy";
import { shouldApplyPageResult } from "./pagePolicy";
import { buildReindexInsertArgs } from "./reindexPolicy";
import { buildDefattrAppendCommand } from "./attrPolicy";
import {
  FolderOpened,
  Coin,
  DocumentCopy,
  Plus,
  QuestionFilled,
  Setting,
  DataBoard,
  EditPen,
  House,
  ArrowLeft,
  ArrowRight,
  Sort,
  VideoPlay,
  Monitor,
  Select,
  UploadFilled,
  Tools,
  Key,
  Delete
} from "@element-plus/icons-vue";

type PageResult = { headers: string[]; rows: string[][]; raw: string; columns?: { name: string; ty: string }[] };
type State = { dataDir: string; currentTable: string; pageSize: number };
type DllInfo = { loaded: boolean; version: string; path: string; message: string };
type CommandExecResult = { ok: boolean; output: string; errorCode?: string | null };
type ScriptExecResult = { ok: boolean; output: string; errorCode?: string | null; stopLine?: number | null };
type RuntimeArtifactInfo = {
  guiExePath: string;
  guiExeModified: string;
  demoPath: string;
  demoModified: string;
  perfPath: string;
  perfModified: string;
  runtimeReportPath: string;
  runtimeReportModified: string;
  dllPath: string;
  dllModified: string;
};
type RuntimeTrendDashboard = {
  schema_version?: string;
  generated_at?: string;
  health?: {
    tier?: "healthy" | "warning" | "critical" | string;
    reasons?: string[];
    latest_query_avg_ms?: number | null;
    latest_cm_tps_min?: number | null;
    latest_hp_max_query_avg_ms?: number | null;
    latest_txn_normal_avg_ms?: number | null;
  };
  sources?: {
    test_loop_rows?: number;
    nightly_rows?: number;
  };
  nightly_status?: {
    pass_rate?: number | null;
  };
  perf_metrics?: Record<string, { count: number; min: number | null; max: number | null; avg: number | null }>;
  recent_runs?: Array<{
    source?: string | null;
    timestamp?: string | null;
    runtime_run_id?: string | null;
    status?: string | null;
    dashboard_quality_gate_status?: string | null;
    query_avg_ms_max?: number | null;
    cm_tps_min?: number | null;
  }>;
};
type TableTabState = {
  key: string;
  table: string;
  page: number;
  orderKey: string;
  desc: boolean;
  pageData: PageResult;
  editingRowIndex: number | null;
  editingDraft: string[];
};
type UiSettings = {
  accent: string;
  bgMode: "gradient" | "image";
  bgImageUrl: string;
  bgImageOpacity: number;
  panelOpacity: number;
  fontScale: number;
  denseMode: boolean;
  animations: boolean;
};
type OperationType =
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
  | "generic";
type OperationRecord = {
  type: OperationType;
  title: string;
  forward: string;
  backward?: string;
  time: string;
};
type MenuAction =
  | { kind: "command"; command: string; opType?: OperationType; title?: string; reversible?: boolean }
  | { kind: "workspace" }
  | { kind: "help" }
  | { kind: "logWindow" }
  | { kind: "cliTerminalWindow" }
  | { kind: "perfBench" }
  | { kind: "nightlySoak" }
  | { kind: "settings" }
  | { kind: "createTableWizard" }
  | {
      kind: "dialog";
      opType: OperationType;
      title: string;
      template: string;
      fields: { key: string; label: string; value: string }[];
      reversible?: boolean;
    };
type MenuNode = {
  label: string;
  divider?: boolean;
  action?: MenuAction;
  children?: MenuNode[];
};
type SchemaNode = { schema: string; tables: string[] };
type UiTreeNode = {
  key: string;
  label: string;
  type: "schema" | "table";
  fullName?: string;
  children?: UiTreeNode[];
};
type UiMessageKind = "alert" | "confirm" | "prompt";
type ColumnType = "string" | "int" | "float" | "double" | "bool" | "date" | "datetime" | "timestamp" | "char";

const state = ref<State>({ dataDir: "", currentTable: "", pageSize: 12 });
const tables = ref<string[]>([]);
const tableTabs = ref<TableTabState[]>([]);
const activeTableKey = ref<string>("");
const command = ref("");
const scriptText = ref("# 每行一条命令\nLIST TABLES\n");
const logs = ref<string[]>([]);
const validationErrors = ref<Record<string, boolean>>({});
const activeTab = ref<"data" | "mdb">("data");
const busy = ref(false);
const dll = ref<DllInfo>({ loaded: false, version: "n/a", path: "", message: "" });
const runtimeArtifacts = ref<RuntimeArtifactInfo | null>(null);
const runtimeDashboard = ref<RuntimeTrendDashboard | null>(null);
const runtimeDashboardUpdatedAt = ref("");
const runtimeDashboardPrevTier = ref<string>("");
const runtimeDashboardTierChangeNote = ref("");
const undoStack = ref<OperationRecord[]>([]);
const redoStack = ref<OperationRecord[]>([]);
const showHelp = ref(false);
const showDllModal = ref(false);
const showSettingsModal = ref(false);
const showWorkspaceWarning = ref(false);
const showDialog = ref(false);
const showRowAttrTools = ref(true);
const showCreateTableWizard = ref(false);
const showUiMessage = ref(false);
const logCollapsed = ref(true);
const uiMessageTitle = ref("提示");
const uiMessageText = ref("");
const uiMessageKind = ref<UiMessageKind>("alert");
const uiMessageInput = ref("");
let uiMessageResolver: ((value: string | boolean | null) => void) | null = null;
const bgFileInput = ref<HTMLInputElement | null>(null);
const helpExpanded = ref<Record<number, boolean>>({});
const dialogTitle = ref("");
const dialogFields = ref<{ key: string; label: string; value: string }[]>([]);
const dialogTemplate = ref("");
const dialogOpType = ref<OperationType>("generic");
const dialogReversible = ref(false);
const contextMenu = ref({
  visible: false,
  x: 0,
  y: 0,
  table: ""
});
function initialViewMode(): "main" | "log" | "cli" {
  const h = window.location.hash;
  if (h === "#/log") return "log";
  if (h === "#/cli") return "cli";
  return "main";
}
const viewMode = ref<"main" | "log" | "cli">(initialViewMode());

function tabKeyForTable(table: string) {
  return `tab:${table}`;
}

function ensureTableTab(table: string): TableTabState {
  const t = table.trim();
  const key = tabKeyForTable(t);
  const existing = tableTabs.value.find((x) => x.key === key);
  if (existing) return existing;
  const created: TableTabState = {
    key,
    table: t,
    page: 1,
    orderKey: "id",
    desc: false,
    pageData: { headers: ["id"], rows: [], raw: "" },
    editingRowIndex: null,
    editingDraft: []
  };
  tableTabs.value.push(created);
  if (!activeTableKey.value) activeTableKey.value = created.key;
  return created;
}

const activeTableTab = computed<TableTabState | null>(() => {
  if (!activeTableKey.value) return null;
  return tableTabs.value.find((t) => t.key === activeTableKey.value) ?? null;
});

const page = computed({
  get: () => activeTableTab.value?.page ?? 1,
  set: (v: number) => {
    if (!activeTableTab.value) return;
    activeTableTab.value.page = v;
  }
});
const orderKey = computed({
  get: () => activeTableTab.value?.orderKey ?? "id",
  set: (v: string) => {
    if (!activeTableTab.value) return;
    activeTableTab.value.orderKey = v;
  }
});
const desc = computed({
  get: () => activeTableTab.value?.desc ?? false,
  set: (v: boolean) => {
    if (!activeTableTab.value) return;
    activeTableTab.value.desc = v;
  }
});
const pageData = computed({
  get: () => activeTableTab.value?.pageData ?? { headers: ["id"], rows: [], raw: "" },
  set: (v: PageResult) => {
    if (!activeTableTab.value) return;
    activeTableTab.value.pageData = v;
  }
});
const editingRowIndex = computed({
  get: () => activeTableTab.value?.editingRowIndex ?? null,
  set: (v: number | null) => {
    if (!activeTableTab.value) return;
    activeTableTab.value.editingRowIndex = v;
  }
});
const editingDraft = computed({
  get: () => activeTableTab.value?.editingDraft ?? [],
  set: (v: string[]) => {
    if (!activeTableTab.value) return;
    activeTableTab.value.editingDraft = v;
  }
});

async function activateTableTabByKey(key: string) {
  const tab = tableTabs.value.find((t) => t.key === key);
  if (!tab) return;
  activeTableKey.value = tab.key;
  if (state.value.currentTable !== tab.table) {
    state.value.currentTable = tab.table;
    await invoke("set_current_table", { table: tab.table });
  }
  await refreshPage();
}

function closeTableTab(key: string) {
  const idx = tableTabs.value.findIndex((t) => t.key === key);
  if (idx < 0) return;
  const wasActive = activeTableKey.value === key;
  tableTabs.value.splice(idx, 1);
  if (wasActive) {
    const next = tableTabs.value[idx] ?? tableTabs.value[idx - 1] ?? tableTabs.value[0];
    activeTableKey.value = next ? next.key : "";
  }
}

watch(
  activeTableKey,
  async (k) => {
    if (!k) return;
    await activateTableTabByKey(k);
  },
  { flush: "post" }
);

let cliUnlisten: UnlistenFn | null = null;
let cliTerm: import("@xterm/xterm").Terminal | null = null;
let cliFit: import("@xterm/addon-fit").FitAddon | null = null;
let cliLinePending = "";
let cliSessionReady = false;
const cliWorkspace = ref("");
const cliTable = ref("");
const cliLastCommand = ref("");
const cliLastResponseAt = ref("");
const cliFeedbackLines = ref<string[]>([]);

const cliConnLabel = computed(() => (cliSessionReady ? "已连接" : "未连接"));

function pushCliFeedback(line: string) {
  const stamped = `[${now()}] ${line}`;
  cliFeedbackLines.value.unshift(stamped);
  if (cliFeedbackLines.value.length > 12) {
    cliFeedbackLines.value = cliFeedbackLines.value.slice(0, 12);
  }
}
const helpKeyword = ref("");
const workspaceWarningText = ref("");
const addRowId = ref("");
const addRowValues = ref("");
const addAttrName = ref("");
const addAttrType = ref<
  "string" | "int" | "float" | "double" | "bool" | "date" | "datetime" | "timestamp" | "char"
>("string");
const delAttrName = ref("");

type CreateCol = { name: string; type: ColumnType };
const createTableName = ref("hr.employees");
const createCols = ref<CreateCol[]>([
  { name: "name", type: "string" },
  { name: "dept", type: "string" },
  { name: "age", type: "int" }
]);
const createPk = ref("id");
const createAutoUse = ref(true);
const createAlsoSetPk = ref(true);
const defaultSettings: UiSettings = {
  accent: "#3b82f6",
  bgMode: "gradient",
  bgImageUrl: "",
  bgImageOpacity: 0.22,
  panelOpacity: 0.9,
  fontScale: 1,
  denseMode: false,
  animations: true
};
const settings = ref<UiSettings>({ ...defaultSettings });
let settingsPersistTimer: number | null = null;

function openUiMessage(
  kind: UiMessageKind,
  title: string,
  text: string,
  defaultValue = ""
): Promise<string | boolean | null> {
  uiMessageKind.value = kind;
  uiMessageTitle.value = title;
  uiMessageText.value = text;
  uiMessageInput.value = defaultValue;
  showUiMessage.value = true;
  return new Promise((resolve) => {
    uiMessageResolver = resolve;
  });
}

function closeUiMessage(result: string | boolean | null) {
  showUiMessage.value = false;
  if (uiMessageResolver) {
    uiMessageResolver(result);
    uiMessageResolver = null;
  }
}

function handleUiMessageClosed() {
  // Ensure Promise is always resolved if dialog is closed via X / ESC.
  if (!uiMessageResolver) return;
  const fallback = uiMessageKind.value === "prompt" ? null : false;
  uiMessageResolver(fallback);
  uiMessageResolver = null;
}

function parseGridFromRaw(raw: string): { headers: string[]; rows: string[][] } | null {
  const lines = raw.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
  const rowLines = lines.filter((line) =>
    (line.startsWith("|") && line.endsWith("|")) || (line.startsWith("│") && line.endsWith("│"))
  );
  if (!rowLines.length) return null;
  const parsed = rowLines.map((line) =>
    line
      .trim()
      .replace(/^[|│]/, "")
      .replace(/[|│]$/, "")
      .split(/[|│]/)
      .map((x) => x.trim())
  );
  const headers = parsed[0] ?? [];
  const rows = parsed.slice(1);
  if (!headers.length) return null;
  return { headers, rows };
}

const tableViewData = computed<PageResult>(() => {
  const p = pageData.value;
  if (!p.headers.length) {
    return { headers: ["id"], rows: [], raw: p.raw };
  }
  if (p.headers.length === 1 && p.headers[0] === "raw") {
    const parsed = parseGridFromRaw(p.raw);
    if (parsed) {
      return { headers: parsed.headers, rows: parsed.rows, raw: p.raw };
    }
    return { headers: ["id"], rows: [], raw: p.raw };
  }
  return p;
});

const tableColumns = computed(() => {
  if (Array.isArray(pageData.value.columns) && pageData.value.columns!.length) return pageData.value.columns!;
  return tableViewData.value.headers.map((name) => ({ name, ty: name.trim().toLowerCase() === "id" ? "int" : "string" }));
});

const idColumnIndex = computed(() => tableViewData.value.headers.findIndex((h) => h.trim().toLowerCase() === "id"));
const editableColumnIndices = computed(() => tableViewData.value.headers.map((_, idx) => idx).filter((idx) => idx !== idColumnIndex.value));

const totalRowsHint = computed(() => tableViewData.value.rows.length);
const sortableAttrs = computed(() =>
  tableViewData.value.headers.filter((h) => !!h && h !== "#")
);
const tableStatusText = computed(() => {
  if (tableViewData.value.rows.length > 0) return "";
  const raw = pageData.value.raw || "";
  if (/Illegal byte sequence|filesystem error|load_failed/i.test(raw)) {
    return "分页失败：请检查 workspace 路径编码。";
  }
  if (/invalid page_no|total_pages=0/i.test(raw)) {
    return "暂无数据。";
  }
  if (/error|fail|invalid/i.test(raw)) {
    return "操作失败：已保留原数据。";
  }
  return "暂无数据。";
});
const canUndo = computed(() => undoStack.value.some((x) => !!x.backward));
const canRedo = computed(() => redoStack.value.length > 0);
const dashboardTier = computed(() => String(runtimeDashboard.value?.health?.tier || "unknown").toLowerCase());
const dashboardTierClass = computed(() => {
  if (dashboardTier.value === "healthy") return "tier-healthy";
  if (dashboardTier.value === "warning") return "tier-warning";
  if (dashboardTier.value === "critical") return "tier-critical";
  return "tier-unknown";
});
const dashboardRecentRuns = computed(() => {
  const rows = runtimeDashboard.value?.recent_runs ?? [];
  if (!Array.isArray(rows)) return [];
  return rows.slice(-8).reverse();
});
const layoutStyle = computed(() => {
  const s = settings.value;
  const gradient = "radial-gradient(circle at 15% 15%, #1e3a8a 0%, #0f172a 40%, #030712 100%)";
  const bg =
    s.bgMode === "image" && s.bgImageUrl.trim()
      ? `linear-gradient(rgba(3,7,18,${1 - s.bgImageOpacity}), rgba(3,7,18,${1 - s.bgImageOpacity})), url("${s.bgImageUrl}")`
      : gradient;
  return {
    "--accent": s.accent,
    "--panel-opacity": String(s.panelOpacity),
    "--font-scale": String(s.fontScale),
    backgroundImage: bg
  } as Record<string, string>;
});
const currentBreadcrumb = computed(() => {
  const root = state.value.dataDir || "(workspace)";
  const table = state.value.currentTable || "(no table)";
  return `${root} / ${table}`;
});
const filteredHelp = computed(() => {
  const kw = helpKeyword.value.trim().toLowerCase();
  if (!kw) return helpEntries;
  return helpEntries.filter((x) =>
    [x.category, x.command, x.syntax, x.example, x.desc, x.detail, ...(x.overloads ?? [])]
      .join(" ")
      .toLowerCase()
      .includes(kw)
  );
});
const tableTree = computed<SchemaNode[]>(() => {
  const groups: Record<string, string[]> = {};
  for (const t of tables.value) {
    const name = t.trim();
    if (!name) continue;
    let schema = "default";
    let table = name;
    if (name.includes(".")) {
      const idx = name.indexOf(".");
      schema = name.slice(0, idx) || "default";
      table = name.slice(idx + 1) || name;
    } else if (name.includes("/")) {
      const idx = name.indexOf("/");
      schema = name.slice(0, idx) || "default";
      table = name.slice(idx + 1) || name;
    }
    if (!groups[schema]) groups[schema] = [];
    groups[schema].push(table);
  }
  return Object.keys(groups)
    .sort((a, b) => a.localeCompare(b))
    .map((schema) => ({ schema, tables: groups[schema].sort((a, b) => a.localeCompare(b)) }));
});
const treeData = computed<UiTreeNode[]>(() =>
  tableTree.value.map((s) => ({
    key: `schema:${s.schema}`,
    label: s.schema,
    type: "schema",
    children: s.tables.map((t) => ({
      key: `table:${s.schema}.${t}`,
      label: t,
      type: "table",
      fullName: s.schema === "default" ? t : `${s.schema}.${t}`
    }))
  }))
);

const helpEntries: Array<{
  category: string;
  command: string;
  syntax: string;
  overloads?: string[];
  example: string;
  desc: string;
  detail: string;
}> = [
  { category: "表管理", command: "CREATE TABLE", syntax: "CREATE TABLE(name)", overloads: ["CREATE TABLE(users)", "CREATE TABLE(hr.employees)"], example: "CREATE TABLE(hr.employees)", desc: "创建一个空表文件", detail: "创建后通常要执行 USE + DEFATTR。若表已存在会返回错误。name 支持 schema.table；当 workspace 路径非法时会在日志中提示 load_failed。" },
  { category: "表管理", command: "DROP TABLE", syntax: "DROP TABLE(name)", overloads: ["DROP TABLE(users)", "DROP TABLE(hr.employees)"], example: "DROP TABLE(users)", desc: "删除表及属性侧文件", detail: "会删除 .bin 及对应 .attr。删除前建议 EXPORT 备份。若删除当前表，UI 会清空当前上下文并提示重新选择。" },
  { category: "表管理", command: "USE", syntax: "USE(name)", overloads: ["USE(users)", "USE(hr.employees)"], example: "USE(hr.employees)", desc: "切换当前工作表", detail: "后续 INSERT/UPDATE/DELETE/PAGE 都默认作用于该表。右侧面包屑、分页查询、属性下拉都依赖当前 USE 状态。" },
  { category: "表管理", command: "RENAME TABLE", syntax: "RENAME TABLE(new_name)", overloads: ["RENAME TABLE(users_v2)", "RENAME TABLE(hr.staff)"], example: "RENAME TABLE(hr.staff)", desc: "重命名当前已 USE 的表", detail: "该命令重命名的是“当前表”，不是传入旧名。执行后建议刷新表树并重新分页，避免旧名称缓存。" },
  { category: "架构", command: "DEFATTR", syntax: "DEFATTR(name:type,...)", overloads: ["DEFATTR(name:string,age:int)", "DEFATTR(name:string,dept:string,salary:int)"], example: "DEFATTR(name:string,dept:string,age:int)", desc: "定义或追加属性", detail: "字段顺序决定 INSERT/UPDATE 参数顺序。常用类型：int/string/date/datetime/bool/float/double/char。若属性重复，底层会返回冲突信息。" },
  { category: "架构", command: "SETATTR / RENATTR / DELATTR", syntax: "SETATTR(id,attr,val) / RENATTR(old,new) / DELATTR(attr)", overloads: ["SETATTR(1,dept,ENG)", "RENATTR(salary,total_salary)", "DELATTR(temp_col)"], example: "SETATTR(1,dept,FIN)", desc: "属性值修改、重命名、删除", detail: "SETATTR 用于单属性更新（行级快速改值）；RENATTR 修改列名；DELATTR 删除列会影响所有行结构，建议先备份并确认前端排序键同步更新。" },
  { category: "架构", command: "SHOW ATTR / SHOW PRIMARY KEY", syntax: "SHOW ATTR / SHOW PRIMARY KEY", overloads: ["SHOW ATTR", "SHOW KEY", "SHOW PRIMARY KEY"], example: "SHOW ATTR", desc: "查看表结构信息", detail: "SHOW ATTR 输出列与类型；SHOW PRIMARY KEY 输出主键。排序键输入、创建向导主键设置可参考该结果，避免拼写错误。" },
  { category: "架构", command: "SET PRIMARY KEY", syntax: "SET PRIMARY KEY(key)", overloads: ["SET PRIMARY KEY(id)", "SET PRIMARY KEY(emp_id)"], example: "SET PRIMARY KEY(id)", desc: "指定主键字段", detail: "主键要求唯一。设置到非唯一列会失败。建议先通过 WHERE/FIND 检查重复再设置；失败时 UI 日志会显示具体冲突。" },
  { category: "数据", command: "INSERT / BULKINSERT / BULKINSERTFAST", syntax: "INSERT(id,v1,v2,...) / BULKINSERT(start_id,count[,dept]) / BULKINSERTFAST(start_id,count[,dept])", overloads: ["INSERT(1)", "INSERT(1,Alice,ENG,29)", "BULKINSERT(100000,5000)", "BULKINSERTFAST(200000,10000)", "BULKINSERTFAST(300000,50000,ENG)"], example: "BULKINSERTFAST(100000,5000)", desc: "单条与高吞吐批量插入", detail: "INSERT 适合交互编辑；BULKINSERT 适合通用批量导入；BULKINSERTFAST 在可保证 ID 新鲜不冲突时跳过逐条重复检查，速度更高。批量插入后建议执行 SHOW TUNING 确认写入策略。" },
  { category: "数据", command: "UPDATE", syntax: "UPDATE(id,v1,v2,...)", overloads: ["UPDATE(1,Alice,ENG,30)", "UPDATE(2,Bob,FIN,33)"], example: "UPDATE(1,Alice,ENG,30,22000)", desc: "按 id 覆盖更新整行值", detail: "参数顺序与 DEFATTR 一致。前端单元格失焦自动保存最终也会转成 UPDATE 命令。若缺少 id 或字段数不匹配会报错。" },
  { category: "数据", command: "DELETE / DELETEPK / FIND / FINDPK", syntax: "DELETE(id) / DELETEPK(key) / FIND(id) / FINDPK(key)", overloads: ["DELETE(1)", "DELETEPK(1001)", "FIND(2)", "FINDPK(1001)"], example: "DELETE(3)", desc: "删除与定位记录", detail: "DELETE 按 id 删除；DELETEPK 按主键删除。FIND/FINDPK 用于定位验证。删除后若启用自动重排，UI 会触发 id 重新编号。" },
  { category: "查询", command: "WHERE", syntax: "WHERE(attr,op,val[,AND|OR,...])", overloads: ["WHERE(age,>=,18)", "WHERE(dept,=,ENG,AND,salary,>,20000)"], example: "WHERE(dept,=,ENG,AND,age,>=,30)", desc: "条件筛选查询", detail: "常用操作符：= != > < >= <= contains。复杂条件建议在 MDB 脚本中多行维护，便于复用与调试。" },
  { category: "查询", command: "WHEREP", syntax: "WHEREP(proj_attr,WHERE,key_attr,=,key_value)", overloads: ["WHEREP(name,WHERE,dept,=,ENG)", "WHEREP(salary,WHERE,dept,=,FIN)"], example: "WHEREP(name,WHERE,dept,=,ENG)", desc: "等值过滤 + 单列投影（只读快速命中）", detail: "适用于“WHERE(单列=) + 只看某一列”的读路径优化。该路径会优先命中 covering projection sidecar，最多输出前 50 行。key_attr 需为非 id 的等值条件；若要按 id 定位请用 FIND。" },
  { category: "查询", command: "PAGE", syntax: "PAGE(page,size,order,asc|desc)", overloads: ["PAGE(1,12,id,asc)", "PAGE(2,50,salary,desc)"], example: "PAGE(1,25,join_date,desc)", desc: "分页+排序输出", detail: "UI 的分页器、排序框最终都会映射到该语义。order 为空时默认 id。页码越界返回空页，不会导致崩溃。" },
  { category: "查询", command: "COUNT / SUM / AVG / MIN / MAX", syntax: "COUNT() / SUM(attr) / AVG(attr) / MIN(attr) / MAX(attr)", overloads: ["COUNT()", "SUM(salary)", "AVG(age)", "MIN(join_date)", "MAX(salary)"], example: "SUM(salary)", desc: "聚合统计", detail: "仅可聚合字段（通常为数值/可比较类型）有效。可先 WHERE 过滤后再统计；结果输出在日志区，可复制用于报表。" },
  { category: "导入导出", command: "IMPORTDIR / EXPORT", syntax: "IMPORTDIR(path) / EXPORT CSV file / EXPORT JSON file", overloads: ["IMPORTDIR(C:/tmp/newdb_import)", "EXPORT CSV out.csv", "EXPORT JSON out.json"], example: "EXPORT CSV hr_employees.csv", desc: "批量导入与导出", detail: "IMPORTDIR 扫描目录加载表文件；EXPORT 默认针对当前 USE 表。导出前建议确认分页排序键，避免误导出其他表。" },
  { category: "事务", command: "BEGIN / COMMIT / ROLLBACK", syntax: "BEGIN [table] / COMMIT / ROLLBACK", overloads: ["BEGIN", "BEGIN hr.employees", "COMMIT", "ROLLBACK"], example: "BEGIN", desc: "事务控制与回滚", detail: "BEGIN 后进行多条写操作，COMMIT 提交，ROLLBACK 回退。当前 GUI 通过 DLL 会话保持事务状态，exe 仅用于分页读取，不影响回滚可用性。" },
  { category: "维护", command: "VACUUM / SCAN / RESET / SHOWLOG", syntax: "VACUUM / SCAN / RESET / SHOWLOG", overloads: ["VACUUM", "SCAN", "RESET", "SHOWLOG"], example: "VACUUM", desc: "诊断、整理、重置维护", detail: "VACUUM 整理碎片并压缩存储；SCAN 扫描底层结构；RESET 清空表（危险）；SHOWLOG 查看日志。" },
  { category: "维护", command: "AUTOVACUUM / WALSYNC / SHOW TUNING", syntax: "AUTOVACUUM [0|1|on|off] | WALSYNC [full|normal [interval_ms]|off] | SHOW TUNING", overloads: ["AUTOVACUUM", "AUTOVACUUM on", "AUTOVACUUM off", "WALSYNC normal 20", "SHOW TUNING"], example: "SHOW TUNING", desc: "写入路径调优与状态查看", detail: "AUTOVACUUM 支持 on/off 开关；WALSYNC 支持 full/normal/off 并可配置 normal interval；SHOW TUNING 统一输出 WAL 与自动 VACUUM 当前状态，便于压测前后校验。百万级压测建议先从 100k 单档开始，确认耗时后再放大规模。" }
];

const topMenus: { label: string; key: string; items: MenuNode[] }[] = [
  {
    label: "文件(File)",
    key: "file",
    items: [
      { label: "设置数据目录...", action: { kind: "workspace" } },
      { label: "导入目录...", action: { kind: "dialog", opType: "generic", title: "导入目录", template: "IMPORTDIR({path})", fields: [{ key: "path", label: "目录路径", value: "C:/tmp/newdb_import" }] } },
      { divider: true, label: "-" },
      { label: "导出 CSV...", action: { kind: "dialog", opType: "generic", title: "导出 CSV", template: "EXPORT CSV {file}", fields: [{ key: "file", label: "导出文件", value: "out.csv" }] } },
      { label: "导出 JSON...", action: { kind: "dialog", opType: "generic", title: "导出 JSON", template: "EXPORT JSON {file}", fields: [{ key: "file", label: "导出文件", value: "out.json" }] } }
    ]
  },
  {
    label: "编辑(Edit)",
    key: "edit",
    items: [
      { label: "撤销", action: { kind: "command", command: "__UNDO__" } },
      { label: "重做", action: { kind: "command", command: "__REDO__" } }
    ]
  },
  {
    label: "表(Table)",
    key: "table",
    items: [
      { label: "刷新表列表", action: { kind: "command", command: "SHOW TABLES", opType: "generic", title: "刷新表列表" } },
      { label: "创建表(向导)...", action: { kind: "createTableWizard" } },
      { label: "创建表...", action: { kind: "dialog", opType: "table_create", title: "创建表", template: "CREATE TABLE({name})", fields: [{ key: "name", label: "表名", value: "users" }], reversible: true } },
      { label: "删除表...", action: { kind: "dialog", opType: "table_drop", title: "删除表", template: "DROP TABLE({name})", fields: [{ key: "name", label: "表名", value: "users" }] } },
      { label: "使用表", action: { kind: "dialog", opType: "table_use", title: "使用表", template: "USE({name})", fields: [{ key: "name", label: "表名", value: "users" }] } },
      { label: "重命名表...", action: { kind: "dialog", opType: "table_rename", title: "重命名表", template: "RENAME TABLE({newName})", fields: [{ key: "newName", label: "新表名", value: "users_new" }], reversible: true } },
      { divider: true, label: "-" },
      { label: "定义属性...", action: { kind: "dialog", opType: "schema_defattr", title: "定义属性", template: "DEFATTR({attrs})", fields: [{ key: "attrs", label: "属性定义", value: "name:string,age:int" }] } },
      { label: "显示属性", action: { kind: "command", command: "SHOW ATTR", opType: "generic", title: "显示属性" } },
      { label: "显示主键", action: { kind: "command", command: "SHOW PRIMARY KEY", opType: "generic", title: "显示主键" } },
      { label: "设置主键...", action: { kind: "dialog", opType: "schema_set_pk", title: "设置主键", template: "SET PRIMARY KEY({pk})", fields: [{ key: "pk", label: "主键字段", value: "id" }] } },
      { divider: true, label: "-" },
      { label: "清空表数据", action: { kind: "command", command: "RESET", opType: "generic", title: "清空表数据" } },
      { label: "整理表碎片", action: { kind: "command", command: "VACUUM", opType: "generic", title: "整理表碎片" } },
      { label: "扫描原始数据", action: { kind: "command", command: "SCAN", opType: "generic", title: "扫描原始数据" } }
    ]
  },
  {
    label: "数据(Data)",
    key: "data",
    items: [
      { label: "插入数据...", action: { kind: "dialog", opType: "data_insert", title: "插入数据", template: "INSERT({values})", fields: [{ key: "values", label: "参数", value: "1,alice,20" }], reversible: true } },
      { label: "批量插入(压测)...", action: { kind: "dialog", opType: "data_insert", title: "批量插入", template: "BULKINSERT({start},{count})", fields: [{ key: "start", label: "起始ID", value: "100000" }, { key: "count", label: "条数", value: "5000" }] } },
      { label: "更新数据...", action: { kind: "dialog", opType: "data_update", title: "更新数据", template: "UPDATE({values})", fields: [{ key: "values", label: "参数", value: "1,alice,21" }] } },
      { label: "删除数据...", action: { kind: "dialog", opType: "data_delete", title: "删除数据", template: "DELETE({id})", fields: [{ key: "id", label: "ID", value: "1" }] } },
      { divider: true, label: "-" },
      { label: "条件查询...", action: { kind: "dialog", opType: "query_where", title: "条件查询", template: "WHERE({expr})", fields: [{ key: "expr", label: "条件", value: "age,>=,18" }] } },
      { label: "条件投影(只读命中)...", action: { kind: "dialog", opType: "query_wherep", title: "条件投影", template: "WHEREP({proj},WHERE,{key},=,{value})", fields: [{ key: "proj", label: "投影字段", value: "name" }, { key: "key", label: "等值字段", value: "dept" }, { key: "value", label: "等值", value: "ENG" }] } },
      { label: "分页查询...", action: { kind: "dialog", opType: "query_page", title: "分页查询", template: "PAGE({page},{size},{order},{dir})", fields: [{ key: "page", label: "页码", value: "1" }, { key: "size", label: "每页", value: "12" }, { key: "order", label: "排序键", value: "id" }, { key: "dir", label: "方向", value: "asc" }] } },
      { divider: true, label: "-" },
      { label: "统计行数", action: { kind: "command", command: "COUNT()", opType: "aggregate", title: "统计行数" } },
      { label: "求和...", action: { kind: "dialog", opType: "aggregate", title: "求和", template: "SUM({attr})", fields: [{ key: "attr", label: "字段", value: "age" }] } },
      { label: "平均值...", action: { kind: "dialog", opType: "aggregate", title: "平均值", template: "AVG({attr})", fields: [{ key: "attr", label: "字段", value: "age" }] } },
      { label: "最小值...", action: { kind: "dialog", opType: "aggregate", title: "最小值", template: "MIN({attr})", fields: [{ key: "attr", label: "字段", value: "age" }] } },
      { label: "最大值...", action: { kind: "dialog", opType: "aggregate", title: "最大值", template: "MAX({attr})", fields: [{ key: "attr", label: "字段", value: "age" }] } }
    ]
  },
  {
    label: "事务(Transaction)",
    key: "txn",
    items: [
      { label: "开始事务", action: { kind: "command", command: "BEGIN", opType: "txn_begin", title: "开始事务" } },
      { label: "提交事务", action: { kind: "command", command: "COMMIT", opType: "txn_commit", title: "提交事务" } },
      { label: "回滚事务", action: { kind: "command", command: "ROLLBACK", opType: "txn_rollback", title: "回滚事务" } }
    ]
  },
  {
    label: "工具(Tools)",
    key: "tools",
    items: [
      { label: "设置(Settings)", action: { kind: "settings" } },
      { label: "日志窗口", action: { kind: "logWindow" } },
      { label: "CLI 终端窗口", action: { kind: "cliTerminalWindow" } },
      { label: "百万级性能压测(可执行)...", action: { kind: "perfBench" } },
      { label: "Nightly Soak 趋势跑批...", action: { kind: "nightlySoak" } },
      { divider: true, label: "-" },
      { label: "调优状态", action: { kind: "command", command: "SHOW TUNING", opType: "generic", title: "调优状态" } },
      { label: "WALSYNC normal 20", action: { kind: "command", command: "WALSYNC normal 20", opType: "generic", title: "WALSYNC normal 20" } },
      { label: "AUTOVACUUM on", action: { kind: "command", command: "AUTOVACUUM on", opType: "generic", title: "AUTOVACUUM on" } },
      { label: "AUTOVACUUM off", action: { kind: "command", command: "AUTOVACUUM off", opType: "generic", title: "AUTOVACUUM off" } },
      { divider: true, label: "-" },
      { label: "清空日志", action: { kind: "command", command: "__CLEAR_LOG__", title: "清空日志" } },
      { label: "帮助", action: { kind: "help" } }
    ]
  }
];

function now() {
  return new Date().toLocaleTimeString();
}

function persistLogs() {
  localStorage.setItem("newdb_gui_logs", JSON.stringify(logs.value));
}

function loadLogCollapsed() {
  const raw = localStorage.getItem("newdb_gui_log_collapsed");
  if (raw === null) {
    logCollapsed.value = true;
    return;
  }
  logCollapsed.value = raw === "1";
}

function persistLogCollapsed() {
  localStorage.setItem("newdb_gui_log_collapsed", logCollapsed.value ? "1" : "0");
}

async function loadSettings() {
  try {
    const s = await invoke<UiSettings>("get_settings");
    settings.value = { ...defaultSettings, ...(s as Partial<UiSettings>) };
  } catch {
    settings.value = { ...defaultSettings };
  }
}

async function persistSettings() {
  await invoke("set_settings", { settings: settings.value });
}

watch(
  settings,
  () => {
    if (settingsPersistTimer !== null) {
      window.clearTimeout(settingsPersistTimer);
    }
    settingsPersistTimer = window.setTimeout(() => {
      void persistSettings();
      settingsPersistTimer = null;
    }, 180);
  },
  { deep: true }
);

function loadLogs() {
  const raw = localStorage.getItem("newdb_gui_logs");
  if (!raw) return;
  try {
    const parsed = JSON.parse(raw) as string[];
    logs.value = parsed;
  } catch {
    logs.value = [];
  }
}

function hasNonAsciiPath(path: string) {
  for (let i = 0; i < path.length; i += 1) {
    if (path.charCodeAt(i) > 127) return true;
  }
  return false;
}

function checkWorkspacePathOrWarn(path: string) {
  if (!path) return;
  if (hasNonAsciiPath(path)) {
    workspaceWarningText.value =
      `当前 workspace 路径包含非 ASCII 字符，可能导致底层 C++/filesystem 在 Windows + MinGW 下无法创建文件。\n\n` +
      `当前路径: ${path}\n\n` +
      `建议改为纯英文路径，例如:\n` +
      `C:/tmp/newdb_data\n` +
      `C:/tmp/newdb_gui_case`;
    showWorkspaceWarning.value = true;
  }
}

function logLine(line: string) {
  logs.value.push(line);
  if (logs.value.length > 500) logs.value.shift();
  persistLogs();
}

function extractScriptStopLine(raw: string): number | null {
  const m = /\[SCRIPT\]\s+stopped at line\s+(\d+)\s+due to command error\./i.exec(raw);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) ? n : null;
}

function normalizeCmd(v: string) {
  return v.trim().replace(/\s+/g, " ");
}

function escapeHtml(text: string) {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function highlight(text: string) {
  const esc = escapeHtml(text);
  const kw = helpKeyword.value.trim();
  if (!kw) return esc;
  const safe = kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const re = new RegExp(`(${safe})`, "ig");
  return esc.replace(re, "<mark>$1</mark>");
}

function inferInverse(type: OperationType, forward: string): string | undefined {
  const cmd = normalizeCmd(forward).toUpperCase();
  if (type === "table_create") {
    const m = /CREATE TABLE\(([^)]+)\)/i.exec(forward);
    if (m) return `DROP TABLE(${m[1]})`;
  }
  if (type === "data_insert") {
    const m = /INSERT\(([^,)\s]+)/i.exec(forward);
    if (m) return `DELETE(${m[1]})`;
  }
  if (type === "table_rename") {
    const m = /RENAME TABLE\(([^)]+)\)/i.exec(forward);
    if (m && state.value.currentTable) return `RENAME TABLE(${state.value.currentTable})`;
  }
  if (cmd === "BEGIN") return "ROLLBACK";
  return undefined;
}

function applyStateSideEffects(cmd: string) {
  const useMatch = /USE\(([^)]+)\)/i.exec(cmd);
  if (useMatch) {
    const t = useMatch[1].trim();
    state.value.currentTable = t;
    const tab = ensureTableTab(t);
    activeTableKey.value = tab.key;
  }
  const renameMatch = /RENAME TABLE\(([^)]+)\)/i.exec(cmd);
  if (renameMatch) {
    const t = renameMatch[1].trim();
    state.value.currentTable = t;
    const tab = ensureTableTab(t);
    activeTableKey.value = tab.key;
  }
}

async function refreshTables() {
  tables.value = await invoke<string[]>("list_tables");
}

async function refreshDllInfo() {
  dll.value = await invoke<DllInfo>("dll_info");
}

async function refreshRuntimeArtifacts() {
  runtimeArtifacts.value = await invoke<RuntimeArtifactInfo>("runtime_artifact_info");
}

async function refreshRuntimeDashboard() {
  try {
    const raw = await invoke<string>("runtime_trend_dashboard_json");
    const next = JSON.parse(raw) as RuntimeTrendDashboard;
    const nextTier = String(next?.health?.tier || "unknown").toLowerCase();
    const prevTier = String(runtimeDashboard.value?.health?.tier || runtimeDashboardPrevTier.value || "unknown").toLowerCase();
    runtimeDashboard.value = next;
    runtimeDashboardUpdatedAt.value = now();
    if (prevTier && nextTier && prevTier !== nextTier) {
      runtimeDashboardTierChangeNote.value = `${prevTier} -> ${nextTier}`;
      logLine(`[DASHBOARD] health tier changed: ${prevTier} -> ${nextTier}`);
    }
    runtimeDashboardPrevTier.value = nextTier;
  } catch (e) {
    runtimeDashboard.value = null;
    logLine(`[DASHBOARD][WARN] ${String(e)}`);
  }
}

async function openDllStatusModal() {
  await refreshDllInfo();
  await refreshRuntimeArtifacts();
  await refreshRuntimeDashboard();
  showDllModal.value = true;
}

function resetSettings() {
  settings.value = { ...defaultSettings };
  void persistSettings();
}

function pickBackgroundFile() {
  bgFileInput.value?.click();
}

function onBackgroundFileChange(ev: Event) {
  const input = ev.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file) return;
  if (!file.type.startsWith("image/")) {
    void openUiMessage("alert", "文件类型错误", "请选择图片文件（png/jpg/webp等）");
    input.value = "";
    return;
  }
  const reader = new FileReader();
  reader.onload = () => {
    settings.value.bgMode = "image";
    settings.value.bgImageUrl = String(reader.result ?? "");
    void persistSettings();
  };
  reader.readAsDataURL(file);
  input.value = "";
}

async function refreshPage() {
  if (!activeTableTab.value) return;
  try {
    // Multi-table mode: always align backend current table with active tab
    // before querying page data, otherwise USE(...) from other actions can
    // make PAGE return a different table's result.
    if (state.value.currentTable !== activeTableTab.value.table) {
      state.value.currentTable = activeTableTab.value.table;
      await invoke("set_current_table", { table: activeTableTab.value.table });
    }
    const next = await invoke<PageResult>("query_page", {
      pageNo: activeTableTab.value.page,
      pageSize: state.value.pageSize,
      orderKey: activeTableTab.value.orderKey.trim() || "id",
      descending: activeTableTab.value.desc
    });
    if (shouldApplyPageResult(next)) {
      pageData.value = next;
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    logLine(`[PAGE][ERROR] ${msg}`);
  }
}

function toggleHelpCard(idx: number) {
  helpExpanded.value[idx] = !helpExpanded.value[idx];
}

function expandAllHelp() {
  const next: Record<number, boolean> = {};
  for (let i = 0; i < filteredHelp.value.length; i += 1) next[i] = true;
  helpExpanded.value = next;
}

function collapseAllHelp() {
  helpExpanded.value = {};
}

function startEditRow(rowIndex: number, row: string[]) {
  editingRowIndex.value = rowIndex;
  clearValidationErrors();
  const draft = [...row];
  if (draft.length < tableViewData.value.headers.length) {
    draft.length = tableViewData.value.headers.length;
  }
  editingDraft.value = draft;
}

function validateCellValue(type: string, value: string): string | null {
  const v = value.trim();
  if (v === "") return null; // allow empty cells
  const t = type.trim().toLowerCase();
  if (t === "int") {
    return /^-?\d+$/.test(v) ? null : "应为整数";
  }
  if (t === "float" || t === "double") {
    return /^-?(?:\d+(?:\.\d+)?|\.\d+)$/.test(v) ? null : "应为数字";
  }
  if (t === "bool") {
    return /^(true|false|0|1|yes|no|on|off)$/i.test(v) ? null : "应为布尔值";
  }
  if (t === "date") {
    return /^\d{4}-\d{2}-\d{2}$/.test(v) ? null : "应为 YYYY-MM-DD";
  }
  if (t === "datetime" || t === "timestamp") {
    return /^\d{4}-\d{2}-\d{2}[ t]\d{2}:\d{2}(:\d{2})?$/.test(v) ? null : "应为 YYYY-MM-DD HH:MM[:SS]";
  }
  return null;
}

function clearValidationErrors() {
  validationErrors.value = {};
}

function cancelEditRow() {
  editingRowIndex.value = null;
  editingDraft.value = [];
  clearValidationErrors();
}

function focusCellEditor(rowIndex: number, colIndex: number) {
  window.setTimeout(() => {
    const el = document.querySelector<HTMLInputElement>(`[data-edit-row="${rowIndex}"][data-edit-col="${colIndex}"]`);
    el?.focus();
    el?.select?.();
  }, 0);
}

function setCellInvalid(rowIndex: number, colIndex: number) {
  validationErrors.value = { [`${rowIndex}:${colIndex}`]: true };
  window.setTimeout(() => {
    const key = `${rowIndex}:${colIndex}`;
    const next = { ...validationErrors.value };
    delete next[key];
    validationErrors.value = next;
  }, 3000);
}

async function deleteRow(row: string[]) {
  const idIdx = idColumnIndex.value >= 0 ? idColumnIndex.value : 1;
  const id = (row?.[idIdx] ?? "").trim();
  if (!id) {
    await openUiMessage("alert", "删除失败", "当前行缺少 id，无法删除");
    return;
  }
  const ok = await openUiMessage("confirm", "确认删除", `确认删除行 id=${id} ?`);
  if (!ok) return;
  await runCommand(`DELETE(${id})`, { opType: "data_delete", title: `删除行 id=${id}` });
  await reindexIds();
}

async function reindexIds() {
  if (!state.value.currentTable.trim()) return;
  // Fetch all rows by paging with stable order.
  const allRows: string[][] = [];
  const headers = tableViewData.value.headers;
  const pageSize = 500;
  for (let p = 1; p <= 2000; p += 1) {
    const pr = await invoke<PageResult>("query_page", {
      pageNo: p,
      pageSize,
      orderKey: "id",
      descending: false
    });
    const tv = pr.headers.length === 1 && pr.headers[0] === "raw" ? tableViewData.value : pr;
    const rows = (tv.rows ?? []).filter((r) => Array.isArray(r) && r.length >= 2);
    if (!rows.length) break;
    allRows.push(...rows);
    if (rows.length < pageSize) break;
  }

  // Rebuild table with consecutive ids.
  const cmds: string[] = [];
  cmds.push("RESET");
  for (let i = 0; i < allRows.length; i += 1) {
    const r = allRows[i];
    const newId = String(i + 1);
    const args = buildReindexInsertArgs(headers, r, newId);
    cmds.push(`INSERT(${args})`);
  }
  await runScriptText(cmds.join("\n"));
  await refreshPage();
}

async function quickAddRow() {
  // Qt 版常用：INSERT(newId)；如需补全其它值，允许直接追加逗号参数。
  const id = addRowId.value.trim();
  const vals = addRowValues.value.trim();
  let cmd = "";
  if (id) {
    cmd = vals ? `INSERT(${id},${vals})` : `INSERT(${id})`;
  } else {
    const idIdx = idColumnIndex.value >= 0 ? idColumnIndex.value : 1;
    let maxId = 0;
    for (const row of tableViewData.value.rows) {
      const n = Number((row?.[idIdx] ?? "").trim());
      if (Number.isFinite(n) && n > maxId) {
        maxId = n;
      }
    }
    const next = String(maxId > 0 ? maxId + 1 : 1);
    cmd = vals ? `INSERT(${next},${vals})` : `INSERT(${next})`;
  }
  await runCommand(cmd, { opType: "data_insert", title: "新增行" });
  addRowId.value = "";
  addRowValues.value = "";
}

function isEnterKey(ev: KeyboardEvent): boolean {
  return ev.key === "Enter" || ev.key === "NumpadEnter";
}

function handleQuickToolEnter(ev: KeyboardEvent, action: "addRow" | "addAttr" | "delAttr") {
  if (!isEnterKey(ev)) return;
  ev.preventDefault();
  ev.stopPropagation();
  if (action === "addRow") {
    void quickAddRow();
    return;
  }
  // Schema mutations can invalidate current row editor shape; close editor first.
  cancelEditRow();
  if (action === "addAttr") {
    void addAttribute();
  } else {
    void deleteAttribute();
  }
}

async function addAttribute() {
  const name = addAttrName.value.trim();
  if (!name) {
    await openUiMessage("alert", "属性名为空", "请输入属性名");
    return;
  }
  if (name.toLowerCase() === "id") {
    await openUiMessage("alert", "保留字段", "id 为保留主键列，请使用其它名称");
    return;
  }
  const existing = tableColumns.value
    .map((c) => ({ name: String(c.name ?? "").trim(), ty: String(c.ty ?? "string").trim() || "string" }))
    .filter((c) => !!c.name && c.name !== "#" && c.name.toLowerCase() !== "id");
  if (existing.some((c) => c.name.toLowerCase() === name.toLowerCase())) {
    await openUiMessage("alert", "属性已存在", `属性 ${name} 已存在，请使用其它名称`);
    return;
  }
  const cmd = buildDefattrAppendCommand(tableColumns.value, name, addAttrType.value);
  await runCommand(cmd, { opType: "schema_defattr", title: `新增属性 ${name}:${addAttrType.value}` });
  addAttrName.value = "";
  delAttrName.value = "";
}

async function deleteAttribute() {
  const name = delAttrName.value.trim();
  if (!name) {
    await openUiMessage("alert", "未选择属性", "请选择要删除的属性");
    return;
  }
  const ok = await openUiMessage("confirm", "确认删除属性", `确认删除属性 ${name} ?（会影响表结构）`);
  if (!ok) return;
  await runCommand(`DELATTR(${name})`, { opType: "generic", title: `删除属性 ${name}` });
  delAttrName.value = "";
}

function openCreateTableWizard() {
  showCreateTableWizard.value = true;
}

function toggleLogCollapsed() {
  logCollapsed.value = !logCollapsed.value;
  persistLogCollapsed();
}

function addCreateCol() {
  createCols.value.push({ name: "", type: "string" });
}

function removeCreateCol(i: number) {
  createCols.value.splice(i, 1);
}

function buildCreateTableScript(): string {
  const tname = createTableName.value.trim();
  const cols = createCols.value
    .map((c) => ({ name: c.name.trim(), type: c.type.trim() }))
    .filter((c) => !!c.name && !!c.type);
  const lines: string[] = [];
  lines.push(`CREATE TABLE(${tname})`);
  if (createAutoUse.value) lines.push(`USE(${tname})`);
  if (cols.length) {
    const attr = cols.map((c) => `${c.name}:${c.type}`).join(",");
    lines.push(`DEFATTR(${attr})`);
  }
  if (createAlsoSetPk.value && createPk.value.trim()) {
    lines.push(`SET PRIMARY KEY(${createPk.value.trim()})`);
  }
  return lines.join("\n");
}

async function submitCreateTableWizard() {
  const tname = createTableName.value.trim();
  if (!tname) {
    await openUiMessage("alert", "表名为空", "请输入表名");
    return;
  }
  const script = buildCreateTableScript();
  showCreateTableWizard.value = false;
  await runScriptText(script);
  if (createAutoUse.value) {
    state.value.currentTable = tname;
    await invoke("set_current_table", { table: tname });
    const tab = ensureTableTab(tname);
    activeTableKey.value = tab.key;
  }
  page.value = 1;
  await refreshTables();
  await refreshPage();
}

async function saveEditRow() {
  if (editingRowIndex.value === null) return;
  const rowIndex = editingRowIndex.value;
  const idIdx = idColumnIndex.value;
  if (idIdx < 0) {
    await openUiMessage("alert", "更新失败", "当前表缺少 id 列，无法更新");
    return;
  }
  const headers = tableViewData.value.headers;
  const cols = tableColumns.value;
  const draft = headers.map((_, idx) => (editingDraft.value[idx] ?? "").trim());
  const id = draft[idIdx];
  if (!id) {
    await openUiMessage("alert", "更新失败", "当前行缺少 id，无法更新");
    focusCellEditor(rowIndex, idIdx);
    return;
  }

  for (let i = 0; i < headers.length; i += 1) {
    if (i === idIdx) continue;
    const err = validateCellValue(cols[i]?.ty ?? "string", draft[i]);
    if (err) {
      await openUiMessage("alert", "更新失败", `${headers[i]} ${err}`);
      setCellInvalid(rowIndex, i);
      focusCellEditor(rowIndex, i);
      return;
    }
  }

  const values = draft.filter((_, idx) => idx !== idIdx);
  const cmd = `UPDATE(${[id, ...values].join(",")})`;
  const ok = await runCommand(cmd, { opType: "data_update", title: `更新行 id=${id}` });
  if (ok !== false) {
    cancelEditRow();
  }
}

function onEditorBlur(rowIndex: number) {
  window.setTimeout(() => {
    if (editingRowIndex.value !== rowIndex) return;
    const active = document.activeElement as HTMLElement | null;
    const stillEditingSameRow = active?.getAttribute("data-edit-row") === String(rowIndex);
    if (!stillEditingSameRow) {
      void saveEditRow();
    }
  }, 0);
}

function onEditorKeydown(ev: KeyboardEvent, rowIndex: number) {
  if (ev.key !== "Enter") return;
  ev.preventDefault();
  ev.stopPropagation();
  void saveEditRow();
}

async function applySort() {
  const key = orderKey.value.trim();
  if (!key) {
    orderKey.value = "id";
  } else if (!sortableAttrs.value.includes(key)) {
    logLine(`[SORT] unknown key: ${key}, available: ${sortableAttrs.value.join(", ")}`);
    await openUiMessage(
      "alert",
      "排序属性无效",
      `排序属性无效：${key}\n可用属性：${sortableAttrs.value.join(", ")}`
    );
    return;
  }
  page.value = 1;
  await refreshPage();
}

async function onPageSizeChange(v: number | undefined) {
  const n = Number(v ?? state.value.pageSize ?? 12);
  state.value.pageSize = Math.max(1, Math.min(5000, Number.isFinite(n) ? Math.floor(n) : 12));
  page.value = 1;
  await refreshPage();
}

function pushHistory(item: OperationRecord) {
  undoStack.value.push(item);
  if (undoStack.value.length > 120) undoStack.value.shift();
  redoStack.value = [];
}

async function runCommand(
  text: string,
  opts?: { skipHistory?: boolean; opType?: OperationType; title?: string; backward?: string }
) {
  if (!text.trim()) return;
  busy.value = true;
  try {
    const exec = await invoke<CommandExecResult>("execute_command_ex", { command: text });
    const result = exec.output ?? "";
    logLine(`> ${text}`);
    logLine(result);
    const failed = shouldStopAndSkipHistory(text, result, exec.errorCode ?? null);
    if (!failed) {
      applyStateSideEffects(text);
      if (!opts?.skipHistory) {
        const backward = opts?.backward ?? inferInverse(opts?.opType ?? "generic", text);
        pushHistory({
          type: opts?.opType ?? "generic",
          title: opts?.title ?? text,
          forward: text,
          backward,
          time: now()
        });
      }
    } else {
      logLine("[CMD] insert/update stopped due to command error.");
      await openUiMessage("alert", "命令已中断", "INSERT/UPDATE 执行失败，本次不计入历史。");
    }
    await refreshTables();
    await refreshPage();
    return !failed;
  } finally {
    busy.value = false;
  }
}

async function runPrompt(template: string, opType: OperationType = "generic", title = "命令") {
  const cmd = await openUiMessage("prompt", "输入命令参数", "可直接改整条命令", template);
  if (typeof cmd !== "string") return;
  if (!cmd) return;
  await runCommand(cmd, { opType, title });
}

async function runScriptText(script: string) {
  busy.value = true;
  try {
    const exec = await invoke<ScriptExecResult>("run_script_ex", { script });
    const result = exec.output ?? "";
    logLine("[MDB] script executed");
    logLine(result);
    const stopLine = exec.stopLine ?? extractScriptStopLine(result);
    if (stopLine !== null) {
      logLine(`[MDB] script stopped on line ${stopLine} due to command error.`);
      await openUiMessage("alert", "脚本已中断", `第 ${stopLine} 行执行失败，后续语句未执行。`);
    }
    const st = await invoke<State>("get_state");
    state.value = st;
    if (st.currentTable?.trim()) {
      const tab = ensureTableTab(st.currentTable.trim());
      activeTableKey.value = tab.key;
    }
    await refreshTables();
    await nextTick();
    await refreshPage();
  } finally {
    busy.value = false;
  }
}

async function runScript() {
  busy.value = true;
  try {
    const exec = await invoke<ScriptExecResult>("run_script_ex", { script: scriptText.value });
    const result = exec.output ?? "";
    logLine("[MDB] script executed");
    logLine(result);
    const stopLine = exec.stopLine ?? extractScriptStopLine(result);
    if (stopLine !== null) {
      logLine(`[MDB] script stopped on line ${stopLine} due to command error.`);
      await openUiMessage("alert", "脚本已中断", `第 ${stopLine} 行执行失败，后续语句未执行。`);
    }
    pushHistory({
      type: "generic",
      title: "执行脚本",
      forward: "[SCRIPT]",
      time: now()
    });
    const st = await invoke<State>("get_state");
    state.value = st;
    if (st.currentTable?.trim()) {
      const tab = ensureTableTab(st.currentTable.trim());
      activeTableKey.value = tab.key;
    }
    await refreshTables();
    await nextTick();
    await refreshPage();
  } finally {
    busy.value = false;
  }
}

async function undo() {
  for (let i = undoStack.value.length - 1; i >= 0; i -= 1) {
    if (undoStack.value[i].backward) {
      const [item] = undoStack.value.splice(i, 1);
      redoStack.value.push(item);
      logLine(`[UNDO] ${item.title}`);
      await runCommand(item.backward!, { skipHistory: true, opType: item.type, title: `UNDO:${item.title}` });
      return;
    }
  }
}

async function redo() {
  if (!canRedo.value) return;
  const last = redoStack.value.pop()!;
  undoStack.value.push(last);
  logLine(`[REDO] ${last.title}`);
  await runCommand(last.forward, { skipHistory: true, opType: last.type, title: `REDO:${last.title}` });
}

async function redoFromStack(stackIndex: number, editable: boolean) {
  const item = redoStack.value[stackIndex];
  if (!item) return;
  let command = item.forward;
  if (editable) {
    const edited = await openUiMessage("prompt", "编辑重做命令", "重做前可编辑命令", command);
    if (typeof edited !== "string") return;
    if (!edited) return;
    command = edited;
  }
  redoStack.value.splice(stackIndex, 1);
  undoStack.value.push({ ...item, forward: command, title: item.title + (editable ? " (edited)" : "") });
  logLine(`[REDO] ${item.title}${editable ? " (edited)" : ""}`);
  await runCommand(command, { skipHistory: true, opType: item.type, title: `REDO:${item.title}` });
}

async function useTable(name: string) {
  state.value.currentTable = name;
  await invoke("set_current_table", { table: name });
  const tab = ensureTableTab(name);
  activeTableKey.value = tab.key;
  await refreshPage();
}

async function setWorkspace() {
  const dir = await openUiMessage("prompt", "设置数据目录", "输入数据目录绝对路径", state.value.dataDir);
  if (typeof dir !== "string") return;
  if (!dir) return;
  await invoke("set_workspace", { dataDir: dir });
  const newState = await invoke<State>("get_state");
  state.value = newState;
  checkWorkspacePathOrWarn(state.value.dataDir);
  page.value = 1;
  await refreshTables();
  await refreshPage();
}

function cliResizeHandler() {
  try {
    cliFit?.fit();
  } catch {
    /* ignore */
  }
}

async function teardownCliTerminal() {
  cliSessionReady = false;
  pushCliFeedback("CLI 会话已断开");
  if (cliUnlisten) {
    cliUnlisten();
    cliUnlisten = null;
  }
  cliFit = null;
  if (cliTerm) {
    cliTerm.dispose();
    cliTerm = null;
  }
  cliLinePending = "";
  window.removeEventListener("resize", cliResizeHandler);
  try {
    await invoke("cli_terminal_stop");
  } catch {
    /* ignore */
  }
}

async function mountCliTerminal() {
  await teardownCliTerminal();
  const { Terminal } = await import("@xterm/xterm");
  const { FitAddon } = await import("@xterm/addon-fit");
  const st = await invoke<State>("get_state");
  cliWorkspace.value = st.dataDir || "(未设置)";
  cliTable.value = st.currentTable || "(未选择)";
  await nextTick();
  const el = document.getElementById("cli-terminal-host");
  if (!el) return;
  cliTerm = new Terminal({
    cursorBlink: true,
    convertEol: true,
    theme: {
      background: "#0c0c0e",
      foreground: "#e5e7eb",
      cursor: "#22c55e"
    },
    fontFamily: "'MesloLGS Nerd Font Mono', ui-monospace, monospace",
    fontSize: 13
  });
  cliFit = new FitAddon();
  cliTerm.loadAddon(cliFit);
  cliTerm.open(el);
  cliTerm.focus();
  el.addEventListener("click", () => cliTerm?.focus());
  cliFit.fit();
  cliLinePending = "";
  cliUnlisten = await listen<{ chunk: string; stream: string }>("cli-term-chunk", (ev) => {
    const ch = ev.payload?.chunk;
    if (ch && cliTerm) {
      cliTerm.write(ch);
      cliLastResponseAt.value = now();
      const preview = ch.replace(/\s+/g, " ").trim().slice(0, 72);
      if (preview) {
        pushCliFeedback(`RX/${ev.payload?.stream ?? "stdout"}: ${preview}`);
      }
    }
  });
  cliTerm.onData((data) => {
    for (const ch of data) {
      if (ch === "\r" || ch === "\n") {
        if (!cliSessionReady) continue;
        const sentLine = cliLinePending;
        cliTerm?.write("\r\n");
        cliLastCommand.value = sentLine;
        pushCliFeedback(`TX: ${sentLine || "(空命令)"}`);
        void invoke("cli_terminal_write_line", { line: sentLine })
          .then(() => {
            pushCliFeedback("系统反馈: 命令已发送到 CLI 进程");
          })
          .catch((e) => {
            const msg = String(e);
            cliTerm?.writeln(`\r\n\x1b[31m${msg}\x1b[0m`);
            pushCliFeedback(`系统反馈错误: ${msg}`);
          });
        cliLinePending = "";
      } else if (ch === "\x7f" || ch === "\b") {
        if (cliLinePending.length > 0) {
          cliLinePending = cliLinePending.slice(0, -1);
          cliTerm?.write("\b \b");
        }
      } else if (ch === "\u0003") {
        cliTerm?.write("^C\r\n");
        cliLinePending = "";
      } else if (ch === "\t" || (ch >= " " && ch !== "\ufeff")) {
        cliLinePending += ch;
        cliTerm?.write(ch);
      }
    }
  });
  window.addEventListener("resize", cliResizeHandler);
  try {
    await invoke("cli_terminal_start", { dataDir: st.dataDir, table: st.currentTable });
    cliSessionReady = true;
    pushCliFeedback(`CLI 会话建立成功（workspace=${cliWorkspace.value}, table=${cliTable.value}）`);
  } catch (e) {
    cliTerm.writeln(`\r\n\x1b[31m[启动 CLI 失败] ${String(e)}\x1b[0m`);
    pushCliFeedback(`CLI 启动失败: ${String(e)}`);
  }
}

async function remountCliTerminal() {
  await mountCliTerminal();
}

async function openDetachedCliTerminalWindow() {
  if (viewMode.value === "cli") return;
  const label = "newdb-cli-terminal";
  const exists = await WebviewWindow.getByLabel(label);
  if (exists) {
    await exists.show();
    await exists.setFocus();
    return;
  }
  const w = new WebviewWindow(label, {
    title: "newdb CLI 终端",
    width: 920,
    height: 640,
    url: "index.html#/cli"
  });
  await w.once("tauri://created", () => {
    logLine("[UI] 已打开独立 CLI 终端窗口");
  });
  await w.once("tauri://error", (e) => {
    logLine(`[UI][ERROR] CLI 终端窗口打开失败: ${JSON.stringify(e)}`);
  });
}

async function openDetachedLogWindow() {
  if (viewMode.value === "log") return;
  const label = "newdb-log-window";
  const exists = await WebviewWindow.getByLabel(label);
  if (exists) {
    await exists.show();
    await exists.setFocus();
    return;
  }
  const w = new WebviewWindow(label, {
    title: "newdb 日志窗口",
    width: 980,
    height: 560,
    url: "index.html#/log"
  });
  await w.once("tauri://created", () => {
    logLine("[UI] 已打开独立日志窗口");
  });
  await w.once("tauri://error", (e) => {
    logLine(`[UI][ERROR] 日志窗口打开失败: ${JSON.stringify(e)}`);
  });
}

function openDialog(
  title: string,
  template: string,
  fields: { key: string; label: string; value: string }[],
  opType: OperationType,
  reversible = false
) {
  dialogTitle.value = title;
  dialogTemplate.value = template;
  dialogFields.value = fields;
  dialogOpType.value = opType;
  dialogReversible.value = reversible;
  showDialog.value = true;
}

function buildDialogCommand() {
  let cmd = dialogTemplate.value;
  for (const f of dialogFields.value) {
    cmd = cmd.replaceAll(`{${f.key}}`, f.value);
  }
  return cmd;
}

async function submitDialog() {
  const cmd = buildDialogCommand();
  showDialog.value = false;
  const backward = dialogReversible.value ? inferInverse(dialogOpType.value, cmd) : undefined;
  await runCommand(cmd, { opType: dialogOpType.value, title: dialogTitle.value, backward });
}

async function runMenuAction(action: MenuAction) {
  if (action.kind === "workspace") {
    await setWorkspace();
    return;
  }
  if (action.kind === "help") {
    showHelp.value = true;
    return;
  }
  if (action.kind === "settings") {
    showSettingsModal.value = true;
    return;
  }
  if (action.kind === "createTableWizard") {
    openCreateTableWizard();
    return;
  }
  if (action.kind === "logWindow") {
    await openDetachedLogWindow();
    return;
  }
  if (action.kind === "cliTerminalWindow") {
    await openDetachedCliTerminalWindow();
    return;
  }
  if (action.kind === "perfBench") {
    const sizes = (await openUiMessage("prompt", "压测规模 SizesCsv", "例如：100000,500000,1000000", "100000")) ?? "";
    if (!sizes.trim()) return;
    const qLoopsRaw = (await openUiMessage("prompt", "QueryLoops", "每档查询循环次数", "1")) ?? "1";
    const txnRaw = (await openUiMessage("prompt", "TxnPerMode", "每种事务模式执行次数", "60")) ?? "60";
    const chunkRaw = (await openUiMessage("prompt", "BuildChunkSize", "构建阶段分块大小", "50000")) ?? "50000";
    const queryLoops = Number.parseInt(qLoopsRaw, 10);
    const txnPerMode = Number.parseInt(txnRaw, 10);
    const buildChunkSize = Number.parseInt(chunkRaw, 10);
    busy.value = true;
    try {
      const result = await invoke<string>("run_perf_bench", {
        sizesCsv: sizes.trim(),
        queryLoops: Number.isFinite(queryLoops) ? queryLoops : 1,
        txnPerMode: Number.isFinite(txnPerMode) ? txnPerMode : 60,
        buildChunkSize: Number.isFinite(buildChunkSize) ? buildChunkSize : 50000
      });
      logs.value.unshift(`[${now()}] perf bench\n${result}`);
      persistLogs();
      await refreshTables();
      await openUiMessage("alert", "压测完成", "性能压测执行完成，请在日志中查看 CSV 输出路径。");
    } catch (e: any) {
      await openUiMessage("alert", "压测失败", `性能压测执行失败: ${String(e)}`);
    } finally {
      busy.value = false;
    }
    return;
  }
  if (action.kind === "nightlySoak") {
    const runsRaw =
      (await openUiMessage("prompt", "Nightly Runs", "连续跑批次数（例如 7）", "3")) ?? "3";
    const soakRaw =
      (await openUiMessage("prompt", "SoakMinutes", "每轮 soak 分钟数（例如 30）", "30")) ?? "30";
    const sleepRaw =
      (await openUiMessage("prompt", "SleepSecondsBetweenRuns", "每轮间隔秒数（例如 60）", "30")) ?? "30";
    const runs = Number.parseInt(runsRaw, 10);
    const soakMinutes = Number.parseInt(soakRaw, 10);
    const sleepSeconds = Number.parseInt(sleepRaw, 10);
    busy.value = true;
    try {
      const result = await invoke<string>("run_nightly_soak", {
        runs: Number.isFinite(runs) ? runs : 1,
        soakMinutes: Number.isFinite(soakMinutes) ? soakMinutes : 30,
        sleepSecondsBetweenRuns: Number.isFinite(sleepSeconds) ? sleepSeconds : 30,
        continueOnFailure: true
      });
      logs.value.unshift(`[${now()}] nightly soak\n${result}`);
      persistLogs();
      await refreshRuntimeDashboard();
      await openUiMessage(
        "alert",
        "Nightly Soak 完成",
        "Nightly soak 跑批完成，趋势已写入 scripts/results/nightly_soak_trend.jsonl。"
      );
    } catch (e: any) {
      logs.value.unshift(`[${now()}] nightly soak failed\n${String(e)}`);
      persistLogs();
      await openUiMessage("alert", "Nightly Soak 失败", `跑批失败：${String(e)}`);
    } finally {
      busy.value = false;
    }
    return;
  }
  if (action.kind === "dialog") {
    openDialog(action.title, action.template, action.fields.map((x) => ({ ...x })), action.opType, !!action.reversible);
    return;
  }
  if (action.command === "__UNDO__") {
    await undo();
    return;
  }
  if (action.command === "__REDO__") {
    await redo();
    return;
  }
  if (action.command === "__CLEAR_LOG__") {
    logs.value = [];
    persistLogs();
    return;
  }
  await runCommand(action.command, { opType: action.opType, title: action.title });
}

function onTableContextMenu(ev: MouseEvent, table: string) {
  ev.preventDefault();
  contextMenu.value = {
    visible: true,
    x: ev.clientX,
    y: ev.clientY,
    table
  };
}

async function runContext(command: string) {
  contextMenu.value.visible = false;
  const resolved = command.replaceAll("{table}", contextMenu.value.table);
  await runCommand(resolved, { opType: "generic", title: "右键菜单操作" });
}

function hideContextMenu() {
  contextMenu.value.visible = false;
}

async function onTreeNodeClick(data: UiTreeNode) {
  if (data.type !== "table" || !data.fullName) return;
  await useTable(data.fullName);
}

onMounted(async () => {
  loadSettings();
  loadLogs();
  loadLogCollapsed();
  if (viewMode.value === "cli") {
    await mountCliTerminal();
    return;
  }
  if (viewMode.value === "log") {
    setInterval(loadLogs, 1000);
    return;
  }
  state.value = await invoke<State>("get_state");
  checkWorkspacePathOrWarn(state.value.dataDir);
  await refreshDllInfo();
  try {
    const rt = await invoke<RuntimeArtifactInfo>("runtime_artifact_info");
    runtimeArtifacts.value = rt;
    logLine(`[RUNTIME] gui=${rt.guiExePath} mtime=${rt.guiExeModified}`);
    logLine(`[RUNTIME] demo=${rt.demoPath} mtime=${rt.demoModified}`);
    logLine(`[RUNTIME] perf=${rt.perfPath} mtime=${rt.perfModified}`);
    logLine(`[RUNTIME] runtime_report=${rt.runtimeReportPath} mtime=${rt.runtimeReportModified}`);
    logLine(`[RUNTIME] dll=${rt.dllPath} mtime=${rt.dllModified}`);
  } catch (e) {
    logLine(`[RUNTIME][ERROR] ${String(e)}`);
  }
  await refreshRuntimeDashboard();
  await refreshTables();
  if (state.value.currentTable?.trim()) {
    const tab = ensureTableTab(state.value.currentTable.trim());
    activeTableKey.value = tab.key;
  }
  await refreshPage();
  window.addEventListener("click", () => {
    hideContextMenu();
  });
});

onUnmounted(() => {
  if (viewMode.value === "cli") {
    void teardownCliTerminal();
  }
});
</script>

<template>
  <div v-if="viewMode === 'log'" class="log-only">
    <h2>独立日志窗口</h2>
    <div class="logs">{{ logs.join("\n") }}</div>
  </div>

  <div v-else-if="viewMode === 'cli'" class="cli-only">
    <h2>newdb 交互式 CLI（独立进程持久会话）</h2>
    <div class="cli-toolbar">
      <el-button type="primary" @click="remountCliTerminal">重新连接</el-button>
      <el-button @click="teardownCliTerminal">断开会话</el-button>
    </div>
    <div class="cli-status-grid">
      <div class="cli-status-item"><strong>连接状态：</strong>{{ cliConnLabel }}</div>
      <div class="cli-status-item"><strong>当前数据目录：</strong>{{ cliWorkspace }}</div>
      <div class="cli-status-item"><strong>当前表：</strong>{{ cliTable }}</div>
      <div class="cli-status-item"><strong>最近命令：</strong>{{ cliLastCommand || "(暂无)" }}</div>
      <div class="cli-status-item"><strong>最近响应：</strong>{{ cliLastResponseAt || "(暂无)" }}</div>
    </div>
    <div id="cli-terminal-host" />
  </div>

  <div
    v-else
    class="layout"
    :class="{ dense: settings.denseMode, 'no-anim': !settings.animations }"
    :style="layoutStyle"
  >
    <aside class="sidebar">
      <div class="tables-title">架构 / 表</div>
      <el-button plain @click="refreshTables">刷新表</el-button>
      <div style="height: 8px" />
      <el-tree
        :data="treeData"
        node-key="key"
        default-expand-all
        :expand-on-click-node="false"
        class="table-tree"
        @node-click="onTreeNodeClick"
      >
        <template #default="{ data }">
          <div
            class="table-tree-node"
            :class="{
              active:
                data.type === 'table' &&
                (state.currentTable === data.fullName || state.currentTable === data.label)
            }"
            @contextmenu.prevent="data.type === 'table' && data.fullName && onTableContextMenu($event, data.fullName)"
          >
            <span class="tree-icon">{{ data.type === "schema" ? "🗂" : "▦" }}</span>
            <span>{{ data.label }}</span>
          </div>
        </template>
      </el-tree>
      <div class="stack-panel">
        <div class="stack-title">撤销栈 / 重做栈</div>
        <div class="stack-actions">
          <button class="secondary" :disabled="!canUndo || busy" @click="undo">撤销</button>
          <button class="secondary" :disabled="!canRedo || busy" @click="redo">重做</button>
        </div>
        <div class="stack-list">
          <div v-for="(item, idx) in undoStack.slice().reverse().slice(0, 8)" :key="`u-${idx}`">
            U {{ item.time }} - {{ item.title }}
          </div>
          <div v-for="(item, idx) in redoStack.slice().reverse().slice(0, 6)" :key="`r-${idx}`">
            <div class="redo-line">
              <span>R {{ item.time }} - {{ item.title }}</span>
              <button
                class="secondary mini"
                @click="redoFromStack(redoStack.length - 1 - idx, true)"
              >编辑重做</button>
            </div>
          </div>
        </div>
      </div>
    </aside>

    <section class="content" :class="{ 'console-collapsed': logCollapsed }">
      <div class="menu-bar">
        <el-dropdown v-for="menu in topMenus" :key="menu.key" trigger="click">
          <el-button class="menu-title" text>{{ menu.label }}</el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item
                v-for="item in menu.items"
                :key="item.label + menu.key"
                :divided="!!item.divider"
                @click="item.action && runMenuAction(item.action)"
              >
                <span v-if="!item.divider">{{ item.label }}</span>
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>

      <div class="toolbar">
        <div class="toolbar-group">
          <span class="toolbar-group-title">高频</span>
          <el-button type="primary" :icon="FolderOpened" @click="setWorkspace">数据目录</el-button>
          <el-button type="primary" plain :icon="Plus" @click="openCreateTableWizard">创建表</el-button>
          <el-button :icon="Setting" @click="showSettingsModal = true">设置</el-button>
        </div>
        <div class="toolbar-group toolbar-group-low">
          <span class="toolbar-group-title">低频</span>
          <el-button plain :icon="Coin" @click="openDllStatusModal">DLL状态</el-button>
          <el-button plain :icon="DocumentCopy" @click="openDetachedLogWindow">日志窗口</el-button>
          <el-button plain :icon="Monitor" @click="openDetachedCliTerminalWindow">CLI 终端</el-button>
          <el-button plain :icon="QuestionFilled" @click="showHelp = true">帮助</el-button>
        </div>
        <div class="status">workspace: {{ state.dataDir || "(未设置)" }}</div>
      </div>
      <div class="breadcrumb-bar">
        <span class="breadcrumb-label">当前路径</span>
        <span class="breadcrumb-value">{{ currentBreadcrumb }}</span>
      </div>

      <div class="main-panel">
        <div class="tabs">
          <el-button :icon="DataBoard" :type="activeTab === 'data' ? 'primary' : undefined" @click="activeTab = 'data'">表数据视图</el-button>
          <el-button :icon="EditPen" :type="activeTab === 'mdb' ? 'primary' : undefined" @click="activeTab = 'mdb'">MDB编辑器</el-button>
        </div>

        <template v-if="activeTab === 'data'">
          <div class="fold-panel">
            <div class="fold-header">
              <strong>Runtime Dashboard</strong>
              <div class="dashboard-header-actions">
                <span v-if="runtimeDashboard?.health?.tier" class="dashboard-tier-wrap">
                  健康等级：
                  <span class="dashboard-tier-pill" :class="dashboardTierClass">{{ runtimeDashboard.health.tier }}</span>
                </span>
                <el-button size="small" plain :disabled="busy" @click="refreshRuntimeDashboard">刷新</el-button>
              </div>
            </div>
            <div class="fold-body">
              <template v-if="runtimeDashboard">
                <div class="dashboard-meta-line">
                  <span>last refresh: {{ runtimeDashboardUpdatedAt || "n/a" }}</span>
                  <span>generated_at: {{ runtimeDashboard.generated_at || "n/a" }}</span>
                  <span v-if="runtimeDashboardTierChangeNote" class="dashboard-tier-change">
                    tier changed: {{ runtimeDashboardTierChangeNote }}
                  </span>
                </div>
                <div style="height: 8px" />
                <div class="tool-grid">
                  <div class="tool-card">
                    <div class="tool-title">健康状态</div>
                    <div><strong>tier:</strong> {{ runtimeDashboard.health?.tier || "unknown" }}</div>
                    <div><strong>nightly rows:</strong> {{ runtimeDashboard.sources?.nightly_rows ?? 0 }}</div>
                    <div><strong>test rows:</strong> {{ runtimeDashboard.sources?.test_loop_rows ?? 0 }}</div>
                    <div><strong>nightly pass_rate:</strong> {{ runtimeDashboard.nightly_status?.pass_rate ?? "n/a" }}</div>
                  </div>
                  <div class="tool-card">
                    <div class="tool-title">关键性能快照</div>
                    <div><strong>latest query_avg_ms:</strong> {{ runtimeDashboard.health?.latest_query_avg_ms ?? "n/a" }}</div>
                    <div><strong>latest cm_tps_min:</strong> {{ runtimeDashboard.health?.latest_cm_tps_min ?? "n/a" }}</div>
                    <div><strong>latest hp_query_avg_ms:</strong> {{ runtimeDashboard.health?.latest_hp_max_query_avg_ms ?? "n/a" }}</div>
                    <div><strong>latest txn_normal_avg_ms:</strong> {{ runtimeDashboard.health?.latest_txn_normal_avg_ms ?? "n/a" }}</div>
                  </div>
                  <div class="tool-card">
                    <div class="tool-title">健康原因</div>
                    <div v-if="runtimeDashboard.health?.reasons?.length">
                      {{ runtimeDashboard.health?.reasons?.join(" | ") }}
                    </div>
                    <div v-else>无</div>
                  </div>
                </div>
                <div style="height: 10px" />
                <div class="tool-card">
                  <div class="tool-title">最近运行（recent_runs）</div>
                  <div v-if="dashboardRecentRuns.length === 0" class="mini-tip">暂无 recent_runs 数据</div>
                  <div v-else class="recent-runs-list">
                    <div v-for="(r, idx) in dashboardRecentRuns" :key="`run-${idx}`" class="recent-run-row">
                      <span class="mono">{{ r.timestamp || "n/a" }}</span>
                      <span>{{ r.source || "unknown" }}</span>
                      <span class="mono">{{ r.runtime_run_id || "-" }}</span>
                      <span>q={{ r.query_avg_ms_max ?? "n/a" }}</span>
                      <span>cm={{ r.cm_tps_min ?? "n/a" }}</span>
                      <span>status={{ r.status || "-" }}</span>
                    </div>
                  </div>
                </div>
              </template>
              <template v-else>
                <div class="mini-tip">未找到 runtime_trend_dashboard.json（先运行 test_loop/nightly_soak 产样）</div>
              </template>
            </div>
          </div>

          <div class="open-table-tabs">
            <div class="open-table-tabs-left">
              <strong>已打开表</strong>
            </div>
            <div class="open-table-tabs-right" v-if="tableTabs.length">
              <el-button
                v-for="t in tableTabs"
                :key="t.key"
                size="small"
                :type="activeTableKey === t.key ? 'primary' : undefined"
                @click="activeTableKey = t.key"
              >
                {{ t.table }}
                <span style="margin-left:6px; opacity:0.8;" @click.stop="closeTableTab(t.key)">×</span>
              </el-button>
            </div>
            <div class="open-table-tabs-right" v-else>
              <span class="mini-tip">从左侧选择表</span>
            </div>
          </div>

          <div class="fold-panel">
            <div class="fold-header" @click="showRowAttrTools = !showRowAttrTools">
              <strong>行 / 属性操作</strong>
              <span>{{ showRowAttrTools ? "▾" : "▸" }}</span>
            </div>
            <div v-if="showRowAttrTools" class="fold-body">
              <div class="tool-grid">
                <div class="tool-card">
                  <div class="tool-title">新增行</div>
                  <el-form label-width="0">
                    <el-form-item>
                      <div class="tool-row">
                        <el-input
                          v-model="addRowId"
                          placeholder="id（可空，自动+1）"
                          style="width: 160px"
                          @keydown="handleQuickToolEnter($event, 'addRow')"
                        />
                        <el-input
                          v-model="addRowValues"
                          placeholder="其它值（可空，如 Alice,ENG,29）"
                          style="flex: 1"
                          @keydown="handleQuickToolEnter($event, 'addRow')"
                        />
                        <el-button type="primary" :disabled="busy" @click="quickAddRow">新增</el-button>
                      </div>
                    </el-form-item>
                  </el-form>
                </div>
                <div class="tool-card">
                  <div class="tool-title">属性管理</div>
                  <el-form label-width="0">
                    <el-form-item>
                      <div class="tool-row">
                        <el-input
                          v-model="addAttrName"
                          placeholder="属性名，如 salary"
                          style="width: 180px"
                          @keydown="handleQuickToolEnter($event, 'addAttr')"
                        />
                        <el-select v-model="addAttrType" style="width: 140px">
                          <el-option value="string" label="string" />
                          <el-option value="int" label="int" />
                          <el-option value="float" label="float" />
                          <el-option value="double" label="double" />
                          <el-option value="bool" label="bool" />
                          <el-option value="date" label="date" />
                          <el-option value="datetime" label="datetime" />
                          <el-option value="timestamp" label="timestamp" />
                          <el-option value="char" label="char" />
                        </el-select>
                        <el-button type="primary" plain :disabled="busy" @click="addAttribute">新增属性</el-button>
                      </div>
                    </el-form-item>
                    <el-form-item>
                      <div class="tool-row" style="width: 100%;">
                        <el-select
                          v-model="delAttrName"
                          style="flex: 1"
                          placeholder="选择要删除的属性"
                          @keydown="handleQuickToolEnter($event, 'delAttr')"
                        >
                          <el-option
                            v-for="h in sortableAttrs.filter((x) => x !== 'id')"
                            :key="`attr-${h}`"
                            :value="h"
                            :label="h"
                          />
                        </el-select>
                        <el-button
                          type="danger"
                          plain
                          :disabled="busy"
                          @click="deleteAttribute"
                          @keydown="handleQuickToolEnter($event, 'delAttr')"
                        >删除属性</el-button>
                      </div>
                    </el-form-item>
                  </el-form>
                </div>
              </div>
            </div>
          </div>
          <div class="data-table-wrap">
            <el-table :data="tableViewData.rows" border height="100%" stripe table-layout="auto">
              <el-table-column
                v-for="(h, colIdx) in tableViewData.headers"
                :key="`col-${h}-${colIdx}`"
                :label="h"
                min-width="160"
                show-overflow-tooltip
              >
                <template #default="scope">
                  <template v-if="editingRowIndex === scope.$index && colIdx === idColumnIndex">
                    <el-input
                      :model-value="editingDraft[colIdx]"
                      class="cell-editor cell-editor-readonly"
                      disabled
                    />
                  </template>
                  <template v-else-if="editingRowIndex === scope.$index">
                    <div class="cell-editor-shell" :class="{ 'cell-editor-shell-invalid': validationErrors[`${scope.$index}:${colIdx}`] }">
                      <el-tooltip
                        v-if="validationErrors[`${scope.$index}:${colIdx}`]"
                        placement="top"
                        effect="dark"
                        content="该值格式不合法，请修改后再保存"
                      >
                        <el-input
                          v-model="editingDraft[colIdx]"
                          class="cell-editor cell-editor-editable"
                          :class="{ 'cell-editor-empty': !String(editingDraft[colIdx] ?? '').trim() }"
                          :data-edit-row="scope.$index"
                          :data-edit-col="colIdx"
                          @blur="onEditorBlur(scope.$index)"
                          @keydown="onEditorKeydown($event, scope.$index)"
                        />
                      </el-tooltip>
                      <el-input
                        v-else
                        v-model="editingDraft[colIdx]"
                        class="cell-editor cell-editor-editable"
                        :class="{ 'cell-editor-empty': !String(editingDraft[colIdx] ?? '').trim() }"
                        :data-edit-row="scope.$index"
                        :data-edit-col="colIdx"
                        @blur="onEditorBlur(scope.$index)"
                        @keydown="onEditorKeydown($event, scope.$index)"
                      />
                    </div>
                  </template>
                  <template v-else>
                    <span>{{ (scope.row[colIdx] ?? "") }}</span>
                  </template>
                </template>
              </el-table-column>
              <el-table-column label="操作" width="180" fixed="right">
                <template #default="scope">
                  <div class="row-actions">
                    <template v-if="editingRowIndex === scope.$index">
                      <el-button size="small" @click="cancelEditRow">取消</el-button>
                    </template>
                    <template v-else>
                      <el-button size="small" type="primary" plain @click="startEditRow(scope.$index, scope.row)">编辑</el-button>
                      <el-button size="small" type="danger" plain @click="deleteRow(scope.row)">删除</el-button>
                    </template>
                  </div>
                </template>
              </el-table-column>
            </el-table>
            <div v-if="tableViewData.rows.length === 0" class="empty-table-hint">{{ tableStatusText }}</div>
          </div>
        </template>

        <template v-else>
          <textarea v-model="scriptText" />
          <div style="height: 8px" />
          <el-button type="primary" :icon="VideoPlay" @click="runScript" :disabled="busy">执行脚本</el-button>
        </template>
      </div>

      <div class="pager">
        <el-button plain :icon="House" @click="page = 1; refreshPage()">首页</el-button>
        <el-button plain :icon="ArrowLeft" @click="page = Math.max(1, page - 1); refreshPage()">上一页</el-button>
        <span>第 {{ page }} 页</span>
        <el-button plain :icon="ArrowRight" @click="page = page + 1; refreshPage()">下一页</el-button>
        <el-select
          :model-value="state.pageSize"
          style="width: 120px"
          placeholder="预设条数"
          @change="onPageSizeChange"
        >
          <el-option :value="12" label="12" />
          <el-option :value="25" label="25" />
          <el-option :value="50" label="50" />
          <el-option :value="100" label="100" />
          <el-option :value="200" label="200" />
          <el-option :value="500" label="500" />
        </el-select>
        <el-input-number
          v-model="state.pageSize"
          :min="1"
          :max="5000"
          :step="1"
          controls-position="right"
          style="width: 150px"
          @change="onPageSizeChange"
        />
        <input v-model="orderKey" list="sort-attrs" placeholder="排序键，如 id" @keyup.enter="applySort" />
        <datalist id="sort-attrs">
          <option v-for="k in sortableAttrs" :key="`sort-${k}`" :value="k">{{ k }}</option>
        </datalist>
        <label><input type="checkbox" v-model="desc" @change="applySort" /> 倒序</label>
        <el-button plain :icon="Sort" @click="applySort">应用排序</el-button>
        <span class="status">当前页行数: {{ totalRowsHint }}</span>
      </div>

      <div class="console">
        <div class="console-header">
          <span>命令与日志</span>
          <el-button text @click="toggleLogCollapsed">{{ logCollapsed ? "展开" : "折叠" }}</el-button>
        </div>
        <template v-if="!logCollapsed">
          <div class="console-input">
          <input
            v-model="command"
            placeholder="输入命令，如 INSERT(1,alice,20)"
            style="flex: 1"
            @keyup.enter="runCommand(command); command=''"
          />
          <el-button type="primary" :icon="VideoPlay" @click="runCommand(command); command=''" :disabled="busy">执行命令</el-button>
          </div>
          <div class="logs command-output-box">{{ logs.join('\n') }}</div>
        </template>
      </div>
    </section>

    <div
      v-if="contextMenu.visible"
      class="context-menu"
      :style="{ left: `${contextMenu.x}px`, top: `${contextMenu.y}px` }"
    >
      <button @click="runContext('USE({table})')">
        <el-icon><Select /></el-icon>
        <span>使用表</span>
      </button>
      <div class="submenu-group">
        <span class="submenu-title">
          <el-icon><UploadFilled /></el-icon>
          <span>导出</span>
        </span>
        <div class="submenu-panel">
          <button @click="runContext('EXPORT CSV {table}.csv')">
            <el-icon><DocumentCopy /></el-icon>
            <span>CSV</span>
          </button>
          <button @click="runContext('EXPORT JSON {table}.json')">
            <el-icon><DocumentCopy /></el-icon>
            <span>JSON</span>
          </button>
        </div>
      </div>
      <div class="submenu-group">
        <span class="submenu-title">
          <el-icon><Tools /></el-icon>
          <span>表操作</span>
        </span>
        <div class="submenu-panel">
          <button @click="runContext('SHOW ATTR')">
            <el-icon><Tools /></el-icon>
            <span>显示属性</span>
          </button>
          <button @click="runContext('SET PRIMARY KEY(id)')">
            <el-icon><Key /></el-icon>
            <span>设置主键</span>
          </button>
          <button @click="runContext('DROP TABLE({table})')">
            <el-icon><Delete /></el-icon>
            <span>删除表</span>
          </button>
        </div>
      </div>
    </div>

    <el-dialog v-model="showHelp" class="help-modal" width="760px">
      <template #header>
        <h3>帮助（可搜索，高亮匹配）</h3>
      </template>
      <el-input v-model="helpKeyword" placeholder="搜索命令、语法、示例、说明..." />
      <div class="dialog-actions" style="margin-top: 8px">
        <el-button @click="expandAllHelp">全部展开</el-button>
        <el-button @click="collapseAllHelp">全部折叠</el-button>
      </div>
      <div class="help-list">
        <div v-for="(h, idx) in filteredHelp" :key="`${h.command}-${idx}`" class="help-item">
          <div class="help-header" @click="toggleHelpCard(idx)">
            <strong v-html="highlight(h.category)"></strong>
            <strong v-html="highlight(h.command)"></strong>
            <span>{{ helpExpanded[idx] ? "▾" : "▸" }}</span>
          </div>
          <div v-if="helpExpanded[idx]" class="help-body">
            <div>语法：<span v-html="highlight(h.syntax)"></span></div>
            <div v-if="h.overloads?.length">重载：<span v-html="highlight(h.overloads.join(' | '))"></span></div>
            <div>示例：<span v-html="highlight(h.example)"></span></div>
            <div>说明：<span v-html="highlight(h.desc)"></span></div>
            <div>详细：<span v-html="highlight(h.detail)"></span></div>
          </div>
        </div>
      </div>
      <template #footer>
        <el-button @click="showHelp = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showDllModal" width="560px">
      <template #header>
        <h3>DLL 状态</h3>
      </template>
      <div><strong>加载状态：</strong>{{ dll.loaded ? "已加载" : "未加载" }}</div>
      <div><strong>版本：</strong>{{ dll.version }}</div>
      <div><strong>路径：</strong>{{ dll.path }}</div>
      <div class="dll-message"><strong>详情：</strong>{{ dll.message }}</div>
      <template v-if="runtimeArtifacts">
        <div style="margin-top: 10px"><strong>GUI EXE：</strong>{{ runtimeArtifacts.guiExePath }} (mtime={{ runtimeArtifacts.guiExeModified }})</div>
        <div><strong>Demo EXE：</strong>{{ runtimeArtifacts.demoPath }} (mtime={{ runtimeArtifacts.demoModified }})</div>
        <div><strong>Perf EXE：</strong>{{ runtimeArtifacts.perfPath }} (mtime={{ runtimeArtifacts.perfModified }})</div>
        <div><strong>Runtime Report EXE：</strong>{{ runtimeArtifacts.runtimeReportPath }} (mtime={{ runtimeArtifacts.runtimeReportModified }})</div>
        <div><strong>Core DLL：</strong>{{ runtimeArtifacts.dllPath }} (mtime={{ runtimeArtifacts.dllModified }})</div>
      </template>
      <template #footer>
        <el-button @click="showDllModal = false">关闭</el-button>
        <el-button type="primary" @click="openDllStatusModal">刷新状态</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showWorkspaceWarning" width="620px">
      <template #header>
        <h3>Workspace 路径风险提示</h3>
      </template>
      <div class="dll-message">{{ workspaceWarningText }}</div>
      <template #footer>
        <el-button @click="showWorkspaceWarning = false">知道了</el-button>
        <el-button type="primary" @click="setWorkspace">立即修改路径</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showSettingsModal" class="settings-modal" width="700px">
      <template #header>
        <h3>Settings</h3>
      </template>
      <div class="settings-grid">
        <section class="settings-card">
          <div class="settings-card-title">主题与背景</div>
          <el-form label-width="120px">
            <el-form-item label="主题色">
              <div style="display:flex; gap:8px; align-items:center; width:100%;">
                <input type="color" v-model="settings.accent" />
                <el-input v-model="settings.accent" placeholder="#3b82f6" />
              </div>
            </el-form-item>
            <el-form-item label="背景模式">
              <el-select v-model="settings.bgMode">
                <el-option value="gradient" label="渐变背景" />
                <el-option value="image" label="背景图片" />
              </el-select>
            </el-form-item>
            <el-form-item label="背景图片 URL">
              <el-input v-model="settings.bgImageUrl" placeholder="https://.../wallpaper.jpg 或 file:///C:/tmp/bg.jpg" />
            </el-form-item>
            <el-form-item label="本地图片">
              <div style="display:flex; gap:8px; align-items:center;">
                <el-button @click="pickBackgroundFile">选择图片文件</el-button>
                <span style="font-size:12px; color:#93c5fd;">已自动转为本地内嵌背景</span>
                <input
                  ref="bgFileInput"
                  type="file"
                  accept="image/*"
                  style="display:none"
                  @change="onBackgroundFileChange"
                />
              </div>
            </el-form-item>
            <el-form-item label="背景透过度">
              <el-slider v-model="settings.bgImageOpacity" :min="0.05" :max="0.8" :step="0.01" />
            </el-form-item>
          </el-form>
        </section>
        <section class="settings-card">
          <div class="settings-card-title">布局与交互</div>
          <el-form label-width="120px">
            <el-form-item label="面板透明度">
              <el-slider v-model="settings.panelOpacity" :min="0.6" :max="1" :step="0.01" />
            </el-form-item>
            <el-form-item label="字体缩放">
              <el-slider v-model="settings.fontScale" :min="0.9" :max="1.2" :step="0.01" />
            </el-form-item>
            <el-form-item label="紧凑模式">
              <el-switch v-model="settings.denseMode" />
            </el-form-item>
            <el-form-item label="界面动画">
              <el-switch v-model="settings.animations" />
            </el-form-item>
          </el-form>
        </section>
      </div>
      <template #footer>
        <el-button @click="resetSettings">恢复默认</el-button>
        <el-button type="primary" @click="showSettingsModal = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showCreateTableWizard" class="settings-modal" width="760px">
      <template #header>
        <h3>创建表（仿 MySQL 向导）</h3>
      </template>
      <el-form label-width="120px">
        <el-form-item label="表名">
          <el-input v-model="createTableName" placeholder="schema.table 或 table" />
        </el-form-item>

        <el-form-item label="属性列表">
          <div style="display:flex; flex-direction:column; gap:8px; width:100%;">
            <div
              v-for="(c, idx) in createCols"
              :key="`col-${idx}`"
              style="display:flex; gap:8px; align-items:center;"
            >
              <el-input v-model="c.name" placeholder="列名" style="flex:1;" />
              <el-select v-model="c.type" style="width: 160px;">
                <el-option value="string" label="string" />
                <el-option value="int" label="int" />
                <el-option value="float" label="float" />
                <el-option value="double" label="double" />
                <el-option value="bool" label="bool" />
                <el-option value="date" label="date" />
                <el-option value="datetime" label="datetime" />
                <el-option value="timestamp" label="timestamp" />
                <el-option value="char" label="char" />
              </el-select>
              <el-button size="small" type="danger" plain @click="removeCreateCol(idx)">删除</el-button>
            </div>
            <div style="display:flex; gap:8px; align-items:center;">
              <el-button @click="addCreateCol">+ 添加属性</el-button>
            </div>
          </div>
        </el-form-item>

        <el-form-item label="主键">
          <el-input v-model="createPk" placeholder="id" style="width: 220px;" />
          <label style="display:flex; align-items:center; gap:6px; margin-left: 10px;">
            <el-switch v-model="createAlsoSetPk" />
            同时执行 SET PRIMARY KEY
          </label>
        </el-form-item>

        <el-form-item label="创建后">
          <label style="display:flex; align-items:center; gap:6px;">
            <el-switch v-model="createAutoUse" />
            自动 USE 该表
          </label>
        </el-form-item>
      </el-form>

      <div class="dialog-preview">{{ buildCreateTableScript() }}</div>
      <template #footer>
        <el-button @click="showCreateTableWizard = false">取消</el-button>
        <el-button type="primary" :disabled="busy" @click="submitCreateTableWizard">创建空表</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showDialog" width="560px">
      <template #header>
        <h3>{{ dialogTitle }}</h3>
      </template>
      <el-form label-width="120px">
        <el-form-item v-for="f in dialogFields" :key="f.key" :label="f.label">
          <el-input v-model="f.value" />
        </el-form-item>
      </el-form>
      <div class="dialog-preview">{{ buildDialogCommand() }}</div>
      <template #footer>
        <el-button @click="showDialog = false">取消</el-button>
        <el-button type="primary" @click="submitDialog">执行</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="showUiMessage"
      width="480px"
      :close-on-click-modal="false"
      @closed="handleUiMessageClosed"
    >
      <template #header>
        <h3>{{ uiMessageTitle }}</h3>
      </template>
      <div style="white-space: pre-wrap;">{{ uiMessageText }}</div>
      <el-input
        v-if="uiMessageKind === 'prompt'"
        v-model="uiMessageInput"
        style="margin-top: 10px"
        @keyup.enter="closeUiMessage(uiMessageInput)"
      />
      <template #footer>
        <el-button v-if="uiMessageKind !== 'alert'" @click="closeUiMessage(false)">取消</el-button>
        <el-button type="primary" @click="closeUiMessage(uiMessageKind === 'prompt' ? uiMessageInput : true)">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>
