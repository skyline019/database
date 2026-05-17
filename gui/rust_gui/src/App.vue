<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { WebviewWindow } from "@tauri-apps/api/webviewWindow";
import { shouldStopAndSkipHistory } from "./commandPolicy";
import { shouldApplyPageResult } from "./pagePolicy";
import { buildAddAttrCommand } from "./attrPolicy";
import {
  MDB_DEFATTR_TYPE_OPTIONS,
  isKnownDefattrType,
  validateMdbDefattrCell,
  type MdbKnownDefattrType
} from "./mdbDefattrTypes";
import {
  FolderOpened,
  InfoFilled,
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
  Delete,
  RefreshRight,
  CircleCheck,
  RefreshLeft,
  Flag,
  WarningFilled,
  Reading,
  Search
} from "@element-plus/icons-vue";

const LS_LOGS = "structdb_gui_logs";
const LS_LOGS_LEGACY = "newdb_gui_logs";
const LS_LOG_COLLAPSED = "structdb_gui_log_collapsed";
const LS_LOG_COLLAPSED_LEGACY = "newdb_gui_log_collapsed";

type PageResult = { headers: string[]; rows: string[][]; raw: string; columns?: { name: string; ty: string }[] };
type State = { dataDir: string; currentTable: string; pageSize: number };
type DllInfo = {
  loaded: boolean;
  version: string;
  path: string;
  message: string;
  apiVersionMajor?: number;
  apiVersionMinor?: number;
  apiVersionPatch?: number;
  abiVersion?: number;
};
type CommandExecResult = {
  ok: boolean;
  output: string;
  errorCode?: string | null;
  errorCodeNumeric?: number | null;
};
type ScriptExecResult = { ok: boolean; output: string; errorCode?: string | null; stopLine?: number | null; cancelled?: boolean };
type LongTaskProgress = {
  kind?: string;
  status?: string;
  unitsDone?: number;
  unitsTotal?: number;
  bytesDone?: number;
  bytesTotal?: number;
  detail?: string | null;
  taskId?: string | null;
};
type TimedExecResult = {
  ok: boolean;
  output: string;
  errorCode?: string | null;
  errorCodeNumeric?: number | null;
  elapsedMs: number;
};
type ExportBundleResult = { path: string };
type RuntimeArtifactInfo = {
  guiExePath: string;
  guiExeModified: string;
  demoPath: string;
  demoModified: string;
  dllPath: string;
  dllModified: string;
  runtimeStatsSchemaVersion?: string;
  backendGitCommit?: string;
  buildProfile?: string;
  /** `debug` | `release` — GUI/Tauri build kind from `runtime_artifact_info`. */
  guiPackageKind?: string;
  cApiVersionMajor?: number;
  cApiVersionMinor?: number;
  cApiVersionPatch?: number;
  cApiAbiVersion?: number;
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
  /** Primary body / title text (hex). */
  textMain: string;
  /** Secondary labels (hex). */
  textRegular: string;
  /** Muted hints / captions (hex). */
  textSoft: string;
  /** App / body base tone (hex); layered gradients mix from this. */
  pageBg: string;
  /** Main data grid surface (hex); el-table + .data-table-wrap. */
  tableBg: string;
  bgMode: "gradient" | "image" | "video";
  bgImageUrl: string;
  bgImageOpacity: number;
  bgVideoUrl: string;
  bgVideoOpacity: number;
  panelOpacity: number;
  tableViewOpacity: number;
  logPanelOpacity: number;
  fontScale: number;
  denseMode: boolean;
  animations: boolean;
  sidebarWidth: number;
  cornerScale: number;
  shadowScale: number;
  logFontScale: number;
  logLineHeight: number;
  borderContrast: number;
  panelBrightness: number;
  logHighlightIntensity: number;
};
type SettingsPreset = { key: string; label: string; settings: Partial<UiSettings> };
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
  | "txn_savepoint"
  | "txn_rollback_to"
  | "txn_release_savepoint"
  | "generic";
type UndoOp = {
  type: OperationType;
  title: string;
  forward: string;
  backward?: string;
  inverseSource?: string;
  table?: string;
  time: string;
  ok?: boolean;
};
type UndoUnitStatus = "open" | "ready" | "applied" | "failed" | "frozen";
type UndoUnit = {
  unitId: string;
  txnId: string;
  savepointName: string;
  tablesTouched: string[];
  ops: UndoOp[];
  createdAt: string;
  status: UndoUnitStatus;
  lastError?: string;
};
type TxnSessionRecorder = {
  active: boolean;
  txnId: string;
  currentSavepoint: string;
  pendingOps: UndoOp[];
};
type StackExecResult = {
  ok: boolean;
  appliedOps: number;
  failedOp?: string | null;
  executedCommands?: string[] | null;
  repairAction: string;
  elapsedMs: number;
  message: string;
};
type InverseInferResult = {
  backward?: string | null;
  source?: string;
};
type MenuAction =
  | { kind: "command"; command: string; opType?: OperationType; title?: string; reversible?: boolean }
  | { kind: "workspace" }
  | { kind: "help" }
  | { kind: "logWindow" }
  | { kind: "cliTerminalWindow" }
  | { kind: "txnRecovery" }
  | { kind: "walRecovery" }
  | { kind: "exportBundle" }
  | { kind: "backupStorageBundle" }
  | { kind: "recoverCheckpoint" }
  | { kind: "setDurability" }
  | { kind: "about" }
  | { kind: "settings" }
  | { kind: "createTableWizard" }
  | {
      kind: "dialog";
      opType: OperationType;
      title: string;
      template: string;
      fields: { key: string; label: string; value: string }[];
      reversible?: boolean;
    }
  | { kind: "applyPreset"; presetKey: string };
type MenuNode = {
  label: string;
  /** Non-clickable group title inside dropdown (classification). */
  section?: boolean;
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

const state = ref<State>({ dataDir: "", currentTable: "", pageSize: 12 });
const tables = ref<string[]>([]);
const tableSearch = ref("");
const tableTabs = ref<TableTabState[]>([]);
const activeTableKey = ref<string>("");
const command = ref("");
const commandInputEl = ref<HTMLInputElement | null>(null);
const scriptText = ref("# 每行一条命令\nLIST TABLES\n");
const scriptRunning = ref(false);
const scriptProgressLabel = ref("");
let scriptProgressUnlisten: (() => void) | null = null;
const logs = ref<string[]>([]);
const validationErrors = ref<Record<string, boolean>>({});
const activeTab = ref<"data" | "mdb">("data");
const busy = ref(false);
const dll = ref<DllInfo>({
  loaded: false,
  version: "n/a",
  path: "",
  message: "",
  apiVersionMajor: -1,
  apiVersionMinor: -1,
  apiVersionPatch: -1,
  abiVersion: -1
});
const runtimeArtifacts = ref<RuntimeArtifactInfo | null>(null);
/** Last SHOW PLAN / EXPLAIN WHERE raw output (for Export modal diagnostics). */
const lastShowPlanRaw = ref("");
const lastCommandErrorCodeNumeric = ref<number | null>(null);
const walRecoveryText = ref("");
const exportBundlePath = ref("");
const coldBackupPath = ref("");
const mdbDurabilityLevel = ref<number | null>(null);
const recoverCheckpointSeqInput = ref("1");
const undoStack = ref<UndoUnit[]>([]);
const redoStack = ref<UndoUnit[]>([]);
const txnRecorder = ref<TxnSessionRecorder>({
  active: false,
  txnId: "",
  currentSavepoint: "__autosave__",
  pendingOps: []
});
const stackUndoPage = ref(1);
const stackRedoPage = ref(1);
/** 事务栈列表：撤销 / 重做共用一个列表区，淡出切换。 */
const stackPanelTab = ref<"undo" | "redo">("undo");
const stackPageSize = 8;
const stackPreviewUnit = ref<UndoUnit | null>(null);
const selectedStackKey = ref("");
const showHelp = ref(false);
const showAboutModal = ref(false);
const showTxnRecoveryModal = ref(false);
const showWalRecoveryModal = ref(false);
const showExportModal = ref(false);
const showSettingsModal = ref(false);
const txnRecoverySavepointName = ref("sp1");
const settingsNav = ref<"theme" | "layout" | "module">("theme");
const showWorkspaceWarning = ref(false);
const showDialog = ref(false);
const showSidebar = ref(true);
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
const bgVideoFileInput = ref<HTMLInputElement | null>(null);
const settingsImportInput = ref<HTMLInputElement | null>(null);
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

/** 点击已打开表标签：切换 USE 与后端会话表；再次点击当前标签也会重新同步并刷新分页。 */
async function selectOpenTableTab(key: string) {
  if (activeTableKey.value === key) {
    await activateTableTabByKey(key);
    return;
  }
  activeTableKey.value = key;
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
type QuickRowField = { name: string; ty: string; value: string };
const showQuickRowModal = ref(false);
const quickRowNextId = ref("1");
const quickRowFields = ref<QuickRowField[]>([]);
const addAttrName = ref("");
const addAttrType = ref<MdbKnownDefattrType>("string");
const delAttrName = ref("");

type CreateCol = { name: string; type: MdbKnownDefattrType };
const createTableName = ref("hr.employees");
const createCols = ref<CreateCol[]>([
  { name: "id", type: "int" },
  { name: "name", type: "string" },
  { name: "dept", type: "string" },
  { name: "age", type: "int" }
]);
const createPk = ref("id");
const createAutoUse = ref(true);
const createAlsoSetPk = ref(true);
const defaultSettings: UiSettings = {
  accent: "#3b82f6",
  textMain: "#dbe7ff",
  textRegular: "#c7d2fe",
  textSoft: "#93c5fd",
  pageBg: "#080d18",
  tableBg: "#08121f",
  bgMode: "gradient",
  bgImageUrl: "",
  bgImageOpacity: 0.58,
  bgVideoUrl: "",
  bgVideoOpacity: 0.58,
  panelOpacity: 0.06,
  tableViewOpacity: 0.03,
  logPanelOpacity: 0.05,
  fontScale: 1,
  denseMode: false,
  animations: true,
  sidebarWidth: 260,
  cornerScale: 1,
  shadowScale: 1,
  logFontScale: 1,
  logLineHeight: 1.5,
  borderContrast: 1,
  panelBrightness: 1,
  logHighlightIntensity: 1
};
const settingsPresets: SettingsPreset[] = [
  {
    key: "default",
    label: "默认",
    settings: {
      accent: "#3b82f6",
      textMain: "#e8f0ff",
      textRegular: "#c7d2fe",
      textSoft: "#93c5fd",
      pageBg: "#080d18",
      tableBg: "#08121f",
      bgMode: "gradient",
      bgVideoUrl: "",
      bgVideoOpacity: 0.58,
      panelOpacity: 0.06,
      tableViewOpacity: 0.03,
      logPanelOpacity: 0.05,
      fontScale: 1,
      denseMode: false,
      animations: true,
      cornerScale: 1,
      shadowScale: 1,
      logFontScale: 1,
      logLineHeight: 1.5,
      borderContrast: 1,
      panelBrightness: 1,
      logHighlightIntensity: 1
    }
  },
  {
    key: "midnight",
    label: "午夜蓝",
    settings: {
      accent: "#60a5fa",
      textMain: "#e8ecff",
      textRegular: "#a5b4fc",
      textSoft: "#818cf8",
      pageBg: "#050914",
      tableBg: "#071424",
      bgMode: "gradient",
      bgVideoUrl: "",
      bgVideoOpacity: 0.58,
      panelOpacity: 0.92,
      tableViewOpacity: 0.72,
      logPanelOpacity: 0.9,
      fontScale: 1,
      denseMode: false,
      animations: true,
      cornerScale: 1.05,
      shadowScale: 1.15,
      logFontScale: 1,
      logLineHeight: 1.55,
      borderContrast: 1.08,
      panelBrightness: 0.96,
      logHighlightIntensity: 1.1
    }
  },
  {
    key: "mint",
    label: "薄荷绿",
    settings: {
      accent: "#34d399",
      textMain: "#ecfdf5",
      textRegular: "#a7f3d0",
      textSoft: "#6ee7b7",
      pageBg: "#02140e",
      tableBg: "#031c14",
      bgMode: "gradient",
      panelOpacity: 0.88,
      tableViewOpacity: 0.68,
      logPanelOpacity: 0.86,
      fontScale: 1,
      denseMode: false,
      animations: true,
      cornerScale: 1.1,
      shadowScale: 0.9,
      logFontScale: 1.02,
      logLineHeight: 1.55,
      borderContrast: 0.95,
      panelBrightness: 1.05,
      logHighlightIntensity: 0.95
    }
  },
  {
    key: "compact",
    label: "高密度",
    settings: {
      accent: "#818cf8",
      textMain: "#f1f5f9",
      textRegular: "#cbd5e1",
      textSoft: "#94a3b8",
      pageBg: "#090b10",
      tableBg: "#0c1018",
      bgMode: "gradient",
      panelOpacity: 0.95,
      tableViewOpacity: 0.78,
      logPanelOpacity: 0.96,
      fontScale: 0.96,
      denseMode: true,
      animations: false,
      sidebarWidth: 240,
      cornerScale: 0.9,
      shadowScale: 0.75,
      logFontScale: 0.94,
      logLineHeight: 1.4,
      borderContrast: 1.2,
      panelBrightness: 0.92,
      logHighlightIntensity: 1.2
    }
  },
  {
    key: "sunset",
    label: "日落橙",
    settings: {
      accent: "#f97316",
      textMain: "#fff7ed",
      textRegular: "#fed7aa",
      textSoft: "#fdba74",
      pageBg: "#140805",
      tableBg: "#1c0c08",
      bgMode: "gradient",
      panelOpacity: 0.9,
      tableViewOpacity: 0.73,
      logPanelOpacity: 0.9,
      fontScale: 1,
      denseMode: false,
      animations: true,
      cornerScale: 1.05,
      shadowScale: 1.05,
      logFontScale: 1,
      logLineHeight: 1.52,
      borderContrast: 1.02,
      panelBrightness: 1,
      logHighlightIntensity: 1.05
    }
  },
  {
    key: "violet",
    label: "紫罗兰",
    settings: {
      accent: "#a78bfa",
      textMain: "#faf5ff",
      textRegular: "#ddd6fe",
      textSoft: "#c4b5fd",
      pageBg: "#0c0618",
      tableBg: "#110b22",
      bgMode: "gradient",
      bgVideoUrl: "",
      bgVideoOpacity: 0.58,
      panelOpacity: 0.91,
      tableViewOpacity: 0.73,
      logPanelOpacity: 0.92,
      fontScale: 1,
      denseMode: false,
      animations: true,
      cornerScale: 1.08,
      shadowScale: 1.1,
      logFontScale: 1,
      logLineHeight: 1.52,
      borderContrast: 1.04,
      panelBrightness: 0.98,
      logHighlightIntensity: 1.08
    }
  },
  {
    key: "rose",
    label: "玫瑰粉",
    settings: {
      accent: "#fb7185",
      textMain: "#fff1f2",
      textRegular: "#fecdd3",
      textSoft: "#fda4af",
      pageBg: "#13060c",
      tableBg: "#1a0a12",
      bgMode: "gradient",
      panelOpacity: 0.9,
      tableViewOpacity: 0.73,
      logPanelOpacity: 0.9,
      fontScale: 1,
      denseMode: false,
      animations: true,
      cornerScale: 1.06,
      shadowScale: 1.02,
      logFontScale: 1,
      logLineHeight: 1.52,
      borderContrast: 1,
      panelBrightness: 1,
      logHighlightIntensity: 1.02
    }
  },
  {
    key: "cyber",
    label: "电青",
    settings: {
      accent: "#22d3ee",
      textMain: "#ecfeff",
      textRegular: "#a5f3fc",
      textSoft: "#67e8f9",
      pageBg: "#031218",
      tableBg: "#051a24",
      bgMode: "gradient",
      panelOpacity: 0.88,
      tableViewOpacity: 0.72,
      logPanelOpacity: 0.88,
      fontScale: 1,
      denseMode: false,
      animations: true,
      cornerScale: 1.02,
      shadowScale: 1.12,
      logFontScale: 1.02,
      logLineHeight: 1.55,
      borderContrast: 1.06,
      panelBrightness: 0.97,
      logHighlightIntensity: 1.12
    }
  },
  {
    key: "mono",
    label: "冷灰",
    settings: {
      accent: "#94a3b8",
      textMain: "#f8fafc",
      textRegular: "#e2e8f0",
      textSoft: "#cbd5e1",
      pageBg: "#08090b",
      tableBg: "#0b0d11",
      bgMode: "gradient",
      panelOpacity: 0.92,
      tableViewOpacity: 0.76,
      logPanelOpacity: 0.93,
      fontScale: 1,
      denseMode: false,
      animations: true,
      cornerScale: 0.98,
      shadowScale: 0.92,
      logFontScale: 1,
      logLineHeight: 1.48,
      borderContrast: 1.12,
      panelBrightness: 0.96,
      logHighlightIntensity: 1.05
    }
  }
];

/** 主菜单「主题预设」二级项（视图菜单与工具菜单共用同一份静态结构）。 */
const THEME_PRESET_MENU_CHILDREN: MenuNode[] = settingsPresets.map((p) => ({
  label: p.label,
  action: { kind: "applyPreset", presetKey: p.key } as MenuAction
}));

const settings = ref<UiSettings>({ ...defaultSettings });
let settingsPersistTimer: number | null = null;
const settingsDirty = ref(false);
const settingsLastSavedAt = ref("");
const settingsSyncing = ref(false);
const settingsSliderDragging = ref(false);
const sidebarResizing = ref(false);
let sidebarDragStartX = 0;
let sidebarDragStartWidth = 260;
const settingsSummary = computed(
  () =>
    `accent=${settings.value.accent} | text=${settings.value.textMain}/${settings.value.textSoft} | bg=${settings.value.pageBg}/${settings.value.tableBg} | panel=${settings.value.panelOpacity.toFixed(2)} | font=${settings.value.fontScale.toFixed(2)} | border=${settings.value.borderContrast.toFixed(2)} | bright=${settings.value.panelBrightness.toFixed(2)} | logFx=${settings.value.logHighlightIntensity.toFixed(2)}`
);

function normalizeHexColor(raw: string, fallback: string): string {
  const t = String(raw ?? "").trim();
  if (/^#[0-9a-f]{6}$/i.test(t)) return t.toLowerCase();
  if (/^#[0-9a-f]{3}$/i.test(t)) {
    const h = t.slice(1);
    return `#${h[0]}${h[0]}${h[1]}${h[1]}${h[2]}${h[2]}`.toLowerCase();
  }
  return fallback;
}

function previewPresetColors(preset: SettingsPreset): string[] {
  const u = sanitizeSettings({ ...defaultSettings, ...preset.settings });
  return [u.accent, u.textMain, u.textSoft];
}

function resetThemeTextColors() {
  settings.value.textMain = defaultSettings.textMain;
  settings.value.textRegular = defaultSettings.textRegular;
  settings.value.textSoft = defaultSettings.textSoft;
}

function resetCanvasBgColors() {
  settings.value.pageBg = defaultSettings.pageBg;
  settings.value.tableBg = defaultSettings.tableBg;
}

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

function buildConfirmReorderCommandTemplate(): string {
  const t = state.value.currentTable?.trim() || "";
  const tableJson = JSON.stringify(t);
  return `CONFIRM_REORDER({"table":${tableJson},"pairs":[["old_id","new_id"]]})`;
}

function handleGlobalHotkeys(ev: KeyboardEvent) {
  if (viewMode.value !== "main") return;
  if (showHelp.value || showAboutModal.value || showSettingsModal.value || showDialog.value || showCreateTableWizard.value || showUiMessage.value) {
    return;
  }
  const key = (ev.key || "").toLowerCase();
  const ctrl = ev.ctrlKey || ev.metaKey;
  if (ctrl && key === "l") {
    ev.preventDefault();
    logs.value = [];
    persistLogs();
    return;
  }
  if (ctrl && key === "k") {
    ev.preventDefault();
    commandInputEl.value?.focus();
    return;
  }
  // PHASE38：主键/行锚重排（CONFIRM_REORDER）；占位符可改为真实 old→new
  if (ctrl && ev.shiftKey && key === "r") {
    ev.preventDefault();
    command.value = buildConfirmReorderCommandTemplate();
    nextTick(() => {
      commandInputEl.value?.focus();
      const el = commandInputEl.value;
      if (el && typeof el.setSelectionRange === "function") {
        const s = command.value.indexOf("old_id");
        if (s >= 0) el.setSelectionRange(s, s + "old_id".length);
      }
    });
    return;
  }
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
  const headersFull = tableViewData.value.headers.map((h) => String(h ?? "").trim());
  const dataHeaders = headersFull.filter((h) => h && h !== "#");
  const colsFromPage = pageData.value.columns;
  if (Array.isArray(colsFromPage) && colsFromPage.length > 0) {
    if (colsFromPage.length === dataHeaders.length) {
      return colsFromPage;
    }
    const byName = new Map(
      colsFromPage.map((c) => [String(c.name ?? "").trim().toLowerCase(), c] as const)
    );
    return dataHeaders.map((name) => {
      const hit = byName.get(name.toLowerCase());
      if (hit) return hit;
      return { name, ty: name.toLowerCase() === "id" ? "int" : "string" };
    });
  }
  return dataHeaders.map((name) => ({
    name,
    ty: name.toLowerCase() === "id" ? "int" : "string"
  }));
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
const canUndo = computed(() => undoStack.value.length > 0);
const canRedo = computed(() => redoStack.value.length > 0);
const undoStackPaged = computed(() => {
  const start = Math.max(0, undoStack.value.length - stackUndoPage.value * stackPageSize);
  const end = undoStack.value.length - (stackUndoPage.value - 1) * stackPageSize;
  return undoStack.value.slice(start, end).reverse();
});
const redoStackPaged = computed(() => {
  const start = Math.max(0, redoStack.value.length - stackRedoPage.value * stackPageSize);
  const end = redoStack.value.length - (stackRedoPage.value - 1) * stackPageSize;
  return redoStack.value.slice(start, end).reverse();
});
type LogKind = "cmd" | "error" | "success" | "meta" | "session" | "normal";
type LogFilterKind = "all" | LogKind;
type RenderedLog = { text: string; kind: LogKind };

const logFilterKind = ref<LogFilterKind>("all");
const logKeyword = ref("");
const logAutoFollow = ref(true);
const logScrollPaneEl = ref<HTMLElement | null>(null);
const helpTotalCount = computed(() => helpEntries.length);
const helpMatchedCount = computed(() => filteredHelp.value.length);
const uiMessageIsLogReview = computed(
  () => uiMessageKind.value === "alert" && uiMessageTitle.value.startsWith("回看日志：")
);
const reviewLogPaneEl = ref<HTMLElement | null>(null);
const reviewLogRowEls = ref<Record<number, HTMLElement | null>>({});
const reviewHitCursor = ref(0);
const parsedReviewLog = computed(() => {
  if (!uiMessageIsLogReview.value) return null;
  const src = String(uiMessageText.value ?? "");
  const marker = "\n\n--- 日志上下文";
  const splitAt = src.indexOf(marker);
  const meta = splitAt >= 0 ? src.slice(0, splitAt) : src;
  const ctxBlock = splitAt >= 0 ? src.slice(splitAt + 2) : "";
  const cmdMatch = /命令：([^\n]+)/.exec(meta);
  const timeMatch = /时间：([^\n]+)/.exec(meta);
  const lineRangeMatch = /日志上下文（(\d+-\d+)）/.exec(ctxBlock);
  const body = ctxBlock.replace(/^---\s*日志上下文（\d+-\d+）---\n?/, "");
  return {
    command: cmdMatch ? cmdMatch[1].trim() : "",
    time: timeMatch ? timeMatch[1].trim() : "",
    lineRange: lineRangeMatch ? lineRangeMatch[1] : "",
    lines: body || "(日志为空)"
  };
});
const reviewLogRows = computed(() => {
  const raw = parsedReviewLog.value?.lines ?? "";
  return String(raw).split(/\r?\n/);
});
const reviewHitIndices = computed(() => {
  const cmd = String(parsedReviewLog.value?.command ?? "").trim();
  if (!cmd) return [];
  const needle = cmd.toLowerCase();
  const exact = `> ${cmd}`.toLowerCase();
  const hits: number[] = [];
  for (let i = 0; i < reviewLogRows.value.length; i += 1) {
    const line = String(reviewLogRows.value[i] ?? "").trim().toLowerCase();
    if (!line) continue;
    if (line === exact || line.includes(needle)) {
      hits.push(i);
    }
  }
  return hits;
});
const reviewActiveHitLine = computed(() => {
  if (!reviewHitIndices.value.length) return -1;
  const pos = Math.max(0, Math.min(reviewHitCursor.value, reviewHitIndices.value.length - 1));
  return reviewHitIndices.value[pos] ?? -1;
});

const renderedLogs = computed<RenderedLog[]>(() =>
  logs.value.map((line) => {
    const t = String(line ?? "").trim();
    let kind: LogKind = "normal";
    if (t.startsWith("> ")) {
      kind = "cmd";
    } else if (t.startsWith("[SESSION]")) {
      kind = "session";
    } else if (/\b(ERR|ERROR|FAILED|FAIL)\b/i.test(t) || t.includes("[CAPI_ERROR]")) {
      kind = "error";
    } else if (/\b(ok|passed|done|valid|success)\b/i.test(t) || t.includes("[INSERT] ok")) {
      kind = "success";
    } else if (t.startsWith("[") && t.includes("]")) {
      kind = "meta";
    }
    return { text: line, kind };
  })
);
const visibleLogs = computed<RenderedLog[]>(() => {
  const kind = logFilterKind.value;
  const keyword = logKeyword.value.trim().toLowerCase();
  return renderedLogs.value.filter((x) => {
    if (kind !== "all" && x.kind !== kind) return false;
    if (!keyword) return true;
    return String(x.text ?? "").toLowerCase().includes(keyword);
  });
});

function highlightLogKeyword(text: string) {
  const src = String(text ?? "");
  const kw = logKeyword.value.trim();
  const escapedSrc = escapeHtml(src);
  if (!kw) return escapedSrc;
  const escapedKw = kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  try {
    return escapedSrc.replace(new RegExp(escapedKw, "gi"), (m) => `<mark>${m}</mark>`);
  } catch {
    return escapedSrc;
  }
}
function classifyLogLine(text: string): LogKind {
  const t = String(text ?? "").trim();
  if (t.startsWith("> ")) return "cmd";
  if (t.startsWith("[SESSION]")) return "session";
  if (/\b(ERR|ERROR|FAILED|FAIL)\b/i.test(t) || t.includes("[CAPI_ERROR]")) return "error";
  if (/\b(ok|passed|done|valid|success)\b/i.test(t) || t.includes("[INSERT] ok")) return "success";
  if (t.startsWith("[") && t.includes("]")) return "meta";
  return "normal";
}
function highlightReviewLogLine(line: string) {
  return {
    text: line,
    kind: classifyLogLine(line)
  };
}
function setReviewLogRowRef(idx: number, el: Element | null) {
  if (!el || !(el instanceof HTMLElement)) return;
  reviewLogRowEls.value[idx] = el;
}
function scrollToReviewHitByCursor() {
  const lineIdx = reviewActiveHitLine.value;
  if (lineIdx < 0) return;
  const row = reviewLogRowEls.value[lineIdx];
  if (row) {
    row.scrollIntoView({ block: "center", behavior: "smooth" });
    return;
  }
  const pane = reviewLogPaneEl.value;
  if (!pane) return;
  const fallbackTop = Math.max(0, lineIdx * 24 - pane.clientHeight / 2);
  pane.scrollTo({ top: fallbackTop, behavior: "smooth" });
}
function jumpReviewHit(step: number) {
  const total = reviewHitIndices.value.length;
  if (!total) return;
  let next = reviewHitCursor.value + step;
  if (next < 0) next = total - 1;
  if (next >= total) next = 0;
  reviewHitCursor.value = next;
  nextTick(() => {
    scrollToReviewHitByCursor();
  });
}
function focusReviewHitByLine(lineIdx: number) {
  const pos = reviewHitIndices.value.indexOf(lineIdx);
  if (pos < 0) return;
  reviewHitCursor.value = pos;
}

function scrollLogsToBottom() {
  if (!logScrollPaneEl.value) return;
  logScrollPaneEl.value.scrollTop = logScrollPaneEl.value.scrollHeight;
}

function onLogScroll() {
  const el = logScrollPaneEl.value;
  if (!el) return;
  const distance = el.scrollHeight - el.scrollTop - el.clientHeight;
  logAutoFollow.value = distance < 18;
}

function toggleLogAutoFollow() {
  logAutoFollow.value = !logAutoFollow.value;
  if (logAutoFollow.value) {
    nextTick(() => {
      scrollLogsToBottom();
    });
  }
}

watch(
  visibleLogs,
  () => {
    if (!logAutoFollow.value) return;
    nextTick(() => {
      scrollLogsToBottom();
    });
  },
  { deep: false }
);
watch(
  [parsedReviewLog, uiMessageIsLogReview],
  () => {
    reviewLogRowEls.value = {};
    reviewHitCursor.value = 0;
    if (!uiMessageIsLogReview.value) return;
    if (!reviewHitIndices.value.length) return;
    nextTick(() => {
      scrollToReviewHitByCursor();
    });
  },
  { deep: false }
);
function buildLayoutGradient(page: string, accent: string): string {
  const p = page.trim() || defaultSettings.pageBg;
  const a = accent.trim() || defaultSettings.accent;
  return [
    `radial-gradient(ellipse 118% 88% at 50% -24%, color-mix(in srgb, ${a} 26%, ${p}) 0%, transparent 57%)`,
    `radial-gradient(ellipse 70% 52% at 100% 34%, color-mix(in srgb, ${a} 13%, ${p}) 0%, transparent 51%)`,
    `radial-gradient(ellipse 58% 44% at 0% 76%, color-mix(in srgb, ${a} 9%, ${p}) 0%, transparent 47%)`,
    `linear-gradient(188deg, color-mix(in srgb, ${p} 95%, #000) 0%, ${p} 40%, color-mix(in srgb, ${p} 97%, #030712) 100%)`
  ].join(", ");
}

const layoutStyle = computed(() => {
  const s = settings.value;
  const hasVideo = s.bgMode === "video" && s.bgVideoUrl.trim();
  const hasImage = s.bgMode === "image" && s.bgImageUrl.trim();
  const bg = hasVideo
    ? `linear-gradient(rgba(3,7,18,${1 - s.bgVideoOpacity}), rgba(3,7,18,${1 - s.bgVideoOpacity}))`
    : hasImage
      ? "none"
      : buildLayoutGradient(s.pageBg, s.accent);
  return {
    "--accent": s.accent,
    "--accent-soft": `color-mix(in srgb, ${s.accent} 45%, #9bf3ff)`,
    "--accent-hot": `color-mix(in srgb, ${s.accent} 72%, #ffffff)`,
    "--accent-deep": `color-mix(in srgb, ${s.accent} 28%, #050814)`,
    "--text-main": s.textMain,
    "--text-regular": s.textRegular,
    "--text-soft": s.textSoft,
    "--page-bg": s.pageBg,
    "--table-surface-bg": s.tableBg,
    "--panel-opacity": String(s.panelOpacity),
    "--table-view-opacity": String(s.tableViewOpacity),
    "--log-panel-opacity": String(s.logPanelOpacity),
    "--font-scale": String(s.fontScale),
    "--sidebar-width": `${Math.round(s.sidebarWidth)}px`,
    "--corner-scale": String(s.cornerScale),
    "--shadow-scale": String(s.shadowScale),
    "--log-font-scale": String(s.logFontScale),
    "--log-line-height": String(s.logLineHeight),
    "--border-contrast": String(s.borderContrast),
    "--panel-brightness": String(s.panelBrightness),
    "--log-highlight-intensity": String(s.logHighlightIntensity),
    "--wallpaper-opacity": String(s.bgMode === "video" ? s.bgVideoOpacity : s.bgImageOpacity),
    backgroundImage: bg,
    backgroundSize: "cover",
    backgroundPosition: "center center",
    backgroundRepeat: "no-repeat"
  } as Record<string, string>;
});

function syncThemeVarsToRoot() {
  if (typeof document === "undefined") return;
  const s = settings.value;
  const root = document.documentElement;
  root.style.setProperty("--accent", s.accent);
  root.style.setProperty("--accent-soft", `color-mix(in srgb, ${s.accent} 45%, #9bf3ff)`);
  root.style.setProperty("--accent-hot", `color-mix(in srgb, ${s.accent} 72%, #ffffff)`);
  root.style.setProperty("--accent-deep", `color-mix(in srgb, ${s.accent} 28%, #050814)`);
  root.style.setProperty("--text-main", s.textMain);
  root.style.setProperty("--text-regular", s.textRegular);
  root.style.setProperty("--text-soft", s.textSoft);
  root.style.setProperty("--page-bg", s.pageBg);
  root.style.setProperty("--table-surface-bg", s.tableBg);
  root.style.setProperty("--panel-opacity", String(s.panelOpacity));
  root.style.setProperty("--table-view-opacity", String(s.tableViewOpacity));
  root.style.setProperty("--log-panel-opacity", String(s.logPanelOpacity));
  root.style.setProperty("--font-scale", String(s.fontScale));
  root.style.setProperty("--sidebar-width", `${Math.round(s.sidebarWidth)}px`);
  root.style.setProperty("--corner-scale", String(s.cornerScale));
  root.style.setProperty("--shadow-scale", String(s.shadowScale));
  root.style.setProperty("--log-font-scale", String(s.logFontScale));
  root.style.setProperty("--log-line-height", String(s.logLineHeight));
  root.style.setProperty("--border-contrast", String(s.borderContrast));
  root.style.setProperty("--panel-brightness", String(s.panelBrightness));
  root.style.setProperty("--log-highlight-intensity", String(s.logHighlightIntensity));
  root.style.setProperty("--wallpaper-opacity", String(s.bgMode === "video" ? s.bgVideoOpacity : s.bgImageOpacity));
}

watch(
  settings,
  () => {
    syncThemeVarsToRoot();
  },
  { deep: true, immediate: true }
);
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
const filteredTableTree = computed<SchemaNode[]>(() => {
  const kw = tableSearch.value.trim().toLowerCase();
  if (!kw) return tableTree.value;
  const out: SchemaNode[] = [];
  for (const s of tableTree.value) {
    const schemaHit = s.schema.toLowerCase().includes(kw);
    if (schemaHit) {
      out.push({ schema: s.schema, tables: [...s.tables] });
      continue;
    }
    const tables = s.tables.filter((t) => t.toLowerCase().includes(kw) || `${s.schema}.${t}`.toLowerCase().includes(kw));
    if (tables.length) out.push({ schema: s.schema, tables });
  }
  return out;
});
const treeData = computed<UiTreeNode[]>(() =>
  filteredTableTree.value.map((s) => ({
    key: `schema:${s.schema}`,
    label: s.schema === "default" ? "default（保底）" : s.schema,
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
  {
    category: "参考",
    command: "语法权威",
    syntax: "Client/mdb + Docs/PHASE25.md",
    example: "HELP",
    desc: "与引擎一致的 MDB 子集",
    detail: "GUI 文案以 StructDB MDB 解析与 dispatch 为准；`AUTOVACUUM`/`WALSYNC`/`RECOVER TO` 等会返回 `[NOT_SUPPORTED]`（见 PHASE25G）。**`CONFIRM_REORDER`** 已在 MDB 侧实现并输出 `[REORDER_MAP_JSON]`，请以 PHASE38 / 引擎日志为准，勿与上述堆类动词混为一谈。"
  },
  { category: "表管理", command: "CREATE TABLE", syntax: "CREATE TABLE(name)", overloads: ["CREATE TABLE(users)", "CREATE TABLE(hr.employees)"], example: "CREATE TABLE(hr.employees)", desc: "创建空表", detail: "创建后需 `USE(name)` 再 `DEFATTR(...)`。表名即 KV catalog 中的逻辑名；已存在则报错。" },
  { category: "表管理", command: "DROP TABLE", syntax: "DROP TABLE(name)", overloads: ["DROP TABLE(users)", "DROP TABLE(hr.employees)"], example: "DROP TABLE(users)", desc: "删除表及 KV 中的行/索引键", detail: "删除 catalog、schema 快照与行数据等键空间条目（非 newdb 式 .bin/.attr 侧车）。删除前建议 `EXPORT JSON path` 备份。" },
  { category: "表管理", command: "USE", syntax: "USE(name)", overloads: ["USE(users)", "USE(hr.employees)"], example: "USE(hr.employees)", desc: "切换当前工作表", detail: "后续 INSERT/UPDATE/DELETE/PAGE 等均作用于当前 USE 表；GUI 分页与网格依赖会话中的当前表。" },
  { category: "表管理", command: "RENAME TABLE", syntax: "RENAME TABLE(new_name)", overloads: ["RENAME TABLE(users_v2)", "RENAME TABLE(hr.staff)"], example: "RENAME TABLE(hr.staff)", desc: "重命名当前已 USE 的表", detail: "重命名的是当前表，不是传入旧名。成功后建议刷新表树并重新打开标签。" },
  { category: "架构", command: "DEFATTR", syntax: "DEFATTR(name:type,...)", overloads: ["DEFATTR(name:string,age:int)", "DEFATTR(id:int,name:string)"], example: "DEFATTR(id:int,name:string,dept:string,salary:int)", desc: "定义列（仅首次，不可追加）", detail: "允许类型：int、string/varchar/text、char（空或单 ASCII）、float、double、datetime（YYYY-MM-DD 或含时间）、timestamp（整数或 datetime 格式）。schema 已存在则拒绝。追加列请用 `ADDATTR(col:type)`。" },
  { category: "架构", command: "ADDATTR", syntax: "ADDATTR([idx,]name:type)", overloads: ["ADDATTR(salary:int)", "ADDATTR(1,note:text)"], example: "ADDATTR(salary:float)", desc: "追加或插入一列（DEFATTR 之后）", detail: "无 idx 时列追加在末尾；有十进制 idx 时在 schema 该下标处插入（0..=当前列数）。已有行该列填类型默认值：数值/时间戳等为 0 或规范日期；string/varchar/text 为字面量 `0`（与快捷插入占位一致）；char 仍为空串。与 `DEFATTR` 互斥：空表须先 `DEFATTR`。" },
  { category: "架构", command: "SETATTR / RENATTR / DELATTR", syntax: "SETATTR(id,attr,val) / RENATTR(old,new) / DELATTR(attr)", overloads: ["SETATTR(1,dept,ENG)", "RENATTR(salary,total_salary)", "DELATTR(temp_col)"], example: "SETATTR(1,dept,FIN)", desc: "列级维护", detail: "SETATTR 更新单格；RENATTR 改列名；DELATTR 删列影响全表。GUI 撤销：`ADDATTR(idx,列:类型)` 的 idx 为 **完整 schema/SHOW ATTR 列序**（含 id 列）；再执行与列数一致的整行 `UPDATE` 恢复快照（超大表受 `NEWDB_GUI_DROP_SNAPSHOT_MAX_ROWS` 限制可能无法自动逆向）。" },
  { category: "架构", command: "SHOW ATTR / SHOW PRIMARY KEY", syntax: "SHOW ATTR | DESCRIBE | SHOW KEY | SHOW PRIMARY KEY", overloads: ["SHOW ATTR", "DESCRIBE", "SHOW KEY"], example: "SHOW ATTR", desc: "查看列与主键", detail: "输出在日志区；主键可由 `SET PRIMARY KEY(col)` 覆盖默认首列。" },
  { category: "架构", command: "SET PRIMARY KEY", syntax: "SET PRIMARY KEY(key)", overloads: ["SET PRIMARY KEY(id)", "SET PRIMARY KEY(emp_id)"], example: "SET PRIMARY KEY(id)", desc: "指定逻辑主键列", detail: "列须已在 DEFATTR 中；引擎会校验列值唯一。行 id（INSERT 首参）与 PK 列可不同；FINDPK 走逻辑 PK。" },
  { category: "数据", command: "INSERT / BULKINSERT / BULKINSERTFAST", syntax: "INSERT(row_id,v1..vN) / BULKINSERT(...)", overloads: ["INSERT(1,1,Alice,ENG,29)", "INSERT(2,2,Bob,FIN,33)"], example: "INSERT(1,1,Alice,ENG,29)", desc: "插入行", detail: "参数个数须为 1 + DEFATTR 列数：首参为行 id（rows 键），其后与 DEFATTR 顺序一一对应（若含 id 列则须再传一列 id 值，通常与首参相同）。BULKINSERTFAST 见 PHASE25C。" },
  { category: "数据", command: "UPDATE", syntax: "UPDATE(id,v1,v2,...)", overloads: ["UPDATE(1,Alice,ENG,30)"], example: "UPDATE(1,Alice,ENG,30,22000)", desc: "按行 id 覆盖整行", detail: "参数顺序与 DEFATTR 列顺序一致；网格编辑最终会生成 UPDATE。" },
  { category: "数据", command: "DELETE / DELETEPK / FIND / FINDPK", syntax: "DELETE(id) / DELETEPK(key) / FIND(id) / FINDPK(key)", overloads: ["DELETE(1)", "DELETEPK(1001)", "FIND(2)", "FINDPK(1001)"], example: "DELETE(3)", desc: "删除与按主键/行 id 查询", detail: "DELETE 按行 id；DELETEPK 按当前逻辑主键列值删除。无 newdb 式「重排连续 id」命令。" },
  { category: "数据", command: "CONFIRM_REORDER", syntax: "CONFIRM_REORDER({\"table\":\"T\",\"pairs\":[[\"old\",\"new\"],…]})", overloads: ["CONFIRM_REORDER({\"table\":\"users\",\"pairs\":[[\"1\",\"100\"]]})"], example: "CONFIRM_REORDER({\"table\":\"users\",\"pairs\":[[\"1\",\"100\"]]})", desc: "批量迁移行锚（当前 USE 表）", detail: "JSON 内 table 须与当前 USE 一致；非事务内执行；成功时引擎输出一行 [REORDER_MAP_JSON] 供 GUI 维护 id 重映射链。详见 Docs/phases/PHASE38.md。" },
  { category: "查询", command: "WHERE", syntax: "WHERE(attr,op,val[,AND|OR,...])", overloads: ["WHERE(age,>=,18)", "WHERE(dept,=,ENG,AND,salary,>,20000)"], example: "WHERE(dept,=,ENG,AND,age,>=,30)", desc: "条件筛选", detail: "支持常用比较符与 AND/OR 链；复杂条件可写在 .mdb 脚本多行维护。" },
  { category: "查询", command: "WHEREP", syntax: "WHEREP(proj_attr,WHERE,key_attr,=,key_value)", overloads: ["WHEREP(name,WHERE,dept,=,ENG)"], example: "WHEREP(salary,WHERE,dept,=,FIN)", desc: "等值过滤 + 单列投影", detail: "key 须为非 id 的等值条件；只读快速路径，输出有限行。" },
  { category: "查询", command: "PAGE", syntax: "PAGE(page,size,sort_col,asc|desc)", overloads: ["PAGE(1,12,id,asc)", "PAGE(2,50,salary,desc)"], example: "PAGE(1,25,join_date,desc)", desc: "分页 + 排序（恰好 4 个参数）", detail: "`sort_col` 可为列名或行 id 关键字 `id`；方向为 `asc` 或 `desc`（前缀匹配）。不支持第 5 参数 keyset。日志中列出本页行 id。" },
  { category: "查询", command: "EXPLAIN WHERE / SHOW PLAN", syntax: "EXPLAIN WHERE(...) / SHOW PLAN(...)", overloads: ["EXPLAIN WHERE(dept,=,ENG)", "SHOW PLAN(age,>=,30,AND,dept,=,FIN)"], example: "SHOW PLAN(dept,=,ENG)", desc: "谓词计划（文本）", detail: "与 WHERE 相同谓词语法；输出为 MDB 文本摘要，无 newdb 专有 JSON plan。" },
  { category: "查询", command: "COUNT / SUM / AVG / MIN / MAX / QBAL", syntax: "COUNT | COUNT(col,op,val) / SUM(attr) / ... / QBAL(col,min_int)", overloads: ["COUNT", "COUNT(age,>=,18)", "SUM(salary)", "QBAL(age,18)"], example: "SUM(salary)", desc: "聚合与 QBAL", detail: "无过滤统计用单独单词 `COUNT`；带谓词用 `COUNT(col,op,val)`。可先 WHERE 再聚合；结果在日志区。QBAL 为整列 >= 下限的计数与求和（PHASE25F）。" },
  { category: "导入导出", command: "IMPORTDIR / EXPORT", syntax: "IMPORTDIR(path) / EXPORT [JSON] path", overloads: ["IMPORTDIR(C:/tmp/mdb_scripts)", "EXPORT out.json", "EXPORT JSON out.json"], example: "EXPORT JSON backup.json", desc: "目录脚本导入与 JSON 导出", detail: "IMPORTDIR 按文件名顺序执行目录下 `*.mdb` 脚本。EXPORT 针对当前 USE 表，写入 JSON 数组；`JSON` 关键字可选，亦可 `EXPORT C:/path/file.json`。" },
  { category: "事务", command: "BEGIN / COMMIT / ROLLBACK", syntax: "BEGIN / COMMIT / ROLLBACK", overloads: ["BEGIN", "COMMIT", "ROLLBACK"], example: "BEGIN", desc: "事务控制", detail: "仅接受单独单词 `BEGIN`，无表名后缀。SAVEPOINT name、ROLLBACK TO SAVEPOINT name、RELEASE SAVEPOINT name（空格分隔，非括号形式）。" },
  { category: "维护", command: "VACUUM / SCAN / RESET / SHOWLOG", syntax: "VACUUM | SCAN | RESET | SHOWLOG", overloads: ["VACUUM", "SCAN", "RESET", "SHOWLOG"], example: "VACUUM", desc: "维护与诊断", detail: "VACUUM：flush_memtable + 受限 L0 drain（PHASE25C）。SCAN 最多扫描 5000 行。RESET 清空当前表数据与 schema（危险）。SHOWLOG 输出会话 journal 尾部。" },
  { category: "维护", command: "SHOW TUNING / SHOW STORAGE", syntax: "SHOW TUNING | SHOW TUNING JSON | SHOW STORAGE | SHOW STORAGE JSON", overloads: ["SHOW TUNING", "SHOW STORAGE JSON"], example: "SHOW TUNING JSON", desc: "引擎状态与存储压力", detail: "JSON 形态为 StructDB 自有结构；非 newdb runtime_stats。" },
  { category: "维护", command: "SET DURABILITY", syntax: "SET DURABILITY level", overloads: ["SET DURABILITY 0", "SET DURABILITY 1", "SET DURABILITY 2"], example: "SET DURABILITY 1", desc: "MDB 会话耐久档位（Wave 4）", detail: "0=最快（少 fsync）；1=默认；2=每批/关键写 fsync。GUI「工具」菜单可切换；亦可通过 C API `structdb_mdb_session_set_durability`。" },
  { category: "维护", command: "SHOW CHECKPOINTS", syntax: "SHOW CHECKPOINTS", overloads: ["SHOW CHECKPOINTS"], example: "SHOW CHECKPOINTS", desc: "列出 checkpoint 链摘要", detail: "输出 seq 与 wal 大小等，用于选择 `RECOVER TO CHECKPOINT_SEQ n` 或 GUI 冷恢复目标。" },
  { category: "维护", command: "RECOVER TO CHECKPOINT_SEQ", syntax: "RECOVER TO CHECKPOINT_SEQ n", overloads: ["RECOVER TO CHECKPOINT_SEQ 3"], example: "RECOVER TO CHECKPOINT_SEQ 3", desc: "按 checkpoint 序号回滚数据目录（PHASE43）", detail: "会截断 WAL/链至目标 seq；执行前须停写（GUI 冷恢复会自动关闭 embed）。详见 Docs/phases/PHASE43.md 与 BACKUP_RESTORE_RUNBOOK。" },
  { category: "索引", command: "CREATE INDEX / DROP INDEX", syntax: "CREATE [UNIQUE] INDEX name ON table(col) | DROP INDEX name ON table", overloads: ["CREATE INDEX idx_dept ON users(dept)", "CREATE UNIQUE INDEX u_k ON users(k)", "DROP INDEX idx_dept ON users"], example: "CREATE INDEX idx_dept ON users(dept)", desc: "命名二级索引（PHASE45）", detail: "UNIQUE 在定义中加 `u:` 前缀或 `CREATE UNIQUE INDEX`；重复键 INSERT 报错。`SCAN INDEX name` 按索引键扫描。" },
  { category: "查询", command: "GROUP BY / ORDER BY / PAGE_JSON", syntax: "GROUP BY(col,...) / ORDER BY(col,asc|desc) / PAGE_JSON(...)", overloads: ["GROUP BY(dept)", "ORDER BY(salary,desc)", "PAGE_JSON(1,10,id,asc)"], example: "GROUP BY(dept)", desc: "分组聚合与 JSON 分页（PHASE42）", detail: "与 WHERE 组合使用；PAGE_JSON 返回 JSON 行集，适合脚本与 GUI 扩展。" },
  { category: "查询", command: "SCAN INDEX", syntax: "SCAN INDEX index_name", overloads: ["SCAN INDEX idx_dept"], example: "SCAN INDEX idx_dept", desc: "按命名索引扫描", detail: "须已 CREATE INDEX；输出受 SCAN 行数上限约束。" },
  { category: "导入导出", command: "IMPORT SEGMENT", syntax: "IMPORT SEGMENT (token)", overloads: ["IMPORT SEGMENT (batch_a)", "IMPORT SEGMENT (batch_b)"], example: "IMPORT SEGMENT (seg1)", desc: "分块导入幂等边界（PHASE44）", detail: "换 token 时自动 persist 上一段；idem 键 `idem:import:<table>:seg:<token>`。大批量导入见 scripts/bench/import_segment_two_segments.mdb。" }
];

const topMenus: { label: string; key: string; items: MenuNode[] }[] = [
  {
    label: "文件(File)",
    key: "file",
    items: [
      { section: true, label: "工作区" },
      { label: "设置数据目录...", action: { kind: "workspace" } },
      { label: "导入目录...", action: { kind: "dialog", opType: "generic", title: "导入目录", template: "IMPORTDIR({path})", fields: [{ key: "path", label: "目录路径", value: "C:/tmp/structdb_import" }] } },
      { label: "IMPORT SEGMENT...", action: { kind: "dialog", opType: "generic", title: "IMPORT SEGMENT", template: "IMPORT SEGMENT ({token})", fields: [{ key: "token", label: "分段 token", value: "batch_a" }] } },
      { divider: true, label: "-" },
      { section: true, label: "导出当前表" },
      { label: "导出 JSON...", action: { kind: "dialog", opType: "generic", title: "导出 JSON", template: "EXPORT JSON {file}", fields: [{ key: "file", label: "导出文件", value: "out.json" }] } },
      { label: "导出（裸路径）...", action: { kind: "dialog", opType: "generic", title: "EXPORT 路径", template: "EXPORT {file}", fields: [{ key: "file", label: "路径（JSON）", value: "C:/backup/table_dump.json" }] } },
      { divider: true, label: "-" },
      { section: true, label: "存储冷备（C API 1.9）" },
      { label: "冷备 data_dir（backup_bundle）...", action: { kind: "backupStorageBundle" } }
    ]
  },
  {
    label: "编辑(Edit)",
    key: "edit",
    items: [
      { section: true, label: "撤销栈" },
      { label: "撤销", action: { kind: "command", command: "__UNDO__" } },
      { label: "重做", action: { kind: "command", command: "__REDO__" } }
    ]
  },
  {
    label: "视图(View)",
    key: "view",
    items: [
      { section: true, label: "界面与外观" },
      { label: "界面设置…", action: { kind: "settings" } },
      { label: "主题预设", children: THEME_PRESET_MENU_CHILDREN },
      { divider: true, label: "-" },
      { section: true, label: "独立窗口" },
      { label: "日志窗口", action: { kind: "logWindow" } },
      { label: "CLI 终端窗口", action: { kind: "cliTerminalWindow" } },
      { divider: true, label: "-" },
      { label: "关于…", action: { kind: "about" } }
    ]
  },
  {
    label: "表(Table)",
    key: "table",
    items: [
      { section: true, label: "表与空间" },
      { label: "刷新表列表", action: { kind: "command", command: "SHOW TABLES", opType: "generic", title: "刷新表列表" } },
      { label: "创建表(向导)...", action: { kind: "createTableWizard" } },
      { label: "创建表...", action: { kind: "dialog", opType: "table_create", title: "创建表", template: "CREATE TABLE({name})", fields: [{ key: "name", label: "表名", value: "users" }], reversible: true } },
      { label: "删除表...", action: { kind: "dialog", opType: "table_drop", title: "删除表", template: "DROP TABLE({name})", fields: [{ key: "name", label: "表名", value: "users" }] } },
      { label: "使用表", action: { kind: "dialog", opType: "table_use", title: "使用表", template: "USE({name})", fields: [{ key: "name", label: "表名", value: "users" }] } },
      { label: "重命名表...", action: { kind: "dialog", opType: "table_rename", title: "重命名表", template: "RENAME TABLE({newName})", fields: [{ key: "newName", label: "新表名", value: "users_new" }], reversible: true } },
      { divider: true, label: "-" },
      { section: true, label: "结构" },
      { label: "定义属性...", action: { kind: "dialog", opType: "schema_defattr", title: "定义属性", template: "DEFATTR({attrs})", fields: [{ key: "attrs", label: "属性定义", value: "name:string,age:int" }] } },
      { label: "显示属性", action: { kind: "command", command: "SHOW ATTR", opType: "generic", title: "显示属性" } },
      { label: "显示主键", action: { kind: "command", command: "SHOW PRIMARY KEY", opType: "generic", title: "显示主键" } },
      { label: "设置主键...", action: { kind: "dialog", opType: "schema_set_pk", title: "设置主键", template: "SET PRIMARY KEY({pk})", fields: [{ key: "pk", label: "主键字段", value: "id" }] } },
      { divider: true, label: "-" },
      { section: true, label: "表级维护" },
      { label: "清空表（RESET）", action: { kind: "command", command: "RESET", opType: "generic", title: "RESET" } },
      { label: "整理（VACUUM）", action: { kind: "command", command: "VACUUM", opType: "generic", title: "VACUUM" } },
      { label: "扫描（SCAN）", action: { kind: "command", command: "SCAN", opType: "generic", title: "SCAN" } },
      { divider: true, label: "-" },
      { section: true, label: "索引（PHASE45）" },
      { label: "创建索引...", action: { kind: "dialog", opType: "generic", title: "CREATE INDEX", template: "CREATE INDEX {name} ON {table}({col})", fields: [{ key: "name", label: "索引名", value: "idx_dept" }, { key: "table", label: "表名", value: "users" }, { key: "col", label: "列", value: "dept" }] } },
      { label: "创建唯一索引...", action: { kind: "dialog", opType: "generic", title: "CREATE UNIQUE INDEX", template: "CREATE UNIQUE INDEX {name} ON {table}({col})", fields: [{ key: "name", label: "索引名", value: "u_k" }, { key: "table", label: "表名", value: "users" }, { key: "col", label: "列", value: "k" }] } },
      { label: "删除索引...", action: { kind: "dialog", opType: "generic", title: "DROP INDEX", template: "DROP INDEX {name} ON {table}", fields: [{ key: "name", label: "索引名", value: "idx_dept" }, { key: "table", label: "表名", value: "users" }] } },
      { label: "扫描索引...", action: { kind: "dialog", opType: "generic", title: "SCAN INDEX", template: "SCAN INDEX {name}", fields: [{ key: "name", label: "索引名", value: "idx_dept" }] } }
    ]
  },
  {
    label: "数据(Data)",
    key: "data",
    items: [
      { section: true, label: "写入" },
      { label: "插入数据...", action: { kind: "dialog", opType: "data_insert", title: "插入数据", template: "INSERT({values})", fields: [{ key: "values", label: "参数(1+列数)", value: "1,1,alice,eng,29" }], reversible: true } },
      { label: "批量插入...", action: { kind: "dialog", opType: "data_insert", title: "批量插入", template: "BULKINSERT({start},{count})", fields: [{ key: "start", label: "起始ID", value: "100000" }, { key: "count", label: "条数", value: "5000" }] } },
      { label: "更新数据...", action: { kind: "dialog", opType: "data_update", title: "更新数据", template: "UPDATE({values})", fields: [{ key: "values", label: "参数", value: "1,alice,21" }] } },
      { label: "删除数据...", action: { kind: "dialog", opType: "data_delete", title: "删除数据", template: "DELETE({id})", fields: [{ key: "id", label: "ID", value: "1" }] } },
      { divider: true, label: "-" },
      { section: true, label: "查询与执行计划" },
      { label: "条件查询...", action: { kind: "dialog", opType: "query_where", title: "条件查询", template: "WHERE({expr})", fields: [{ key: "expr", label: "条件", value: "age,>=,18" }] } },
      { label: "条件投影(只读命中)...", action: { kind: "dialog", opType: "query_wherep", title: "条件投影", template: "WHEREP({proj},WHERE,{key},=,{value})", fields: [{ key: "proj", label: "投影字段", value: "name" }, { key: "key", label: "等值字段", value: "dept" }, { key: "value", label: "等值", value: "ENG" }] } },
      { label: "执行计划...", action: { kind: "dialog", opType: "generic", title: "SHOW PLAN", template: "SHOW PLAN({expr})", fields: [{ key: "expr", label: "条件(同 WHERE)", value: "dept,=,ENG" }] } },
      { label: "执行计划(EXPLAIN)...", action: { kind: "dialog", opType: "generic", title: "EXPLAIN WHERE", template: "EXPLAIN WHERE({expr})", fields: [{ key: "expr", label: "条件(同 WHERE)", value: "dept,=,ENG" }] } },
      { label: "分页查询...", action: { kind: "dialog", opType: "query_page", title: "分页查询", template: "PAGE({page},{size},{order},{dir})", fields: [{ key: "page", label: "页码", value: "1" }, { key: "size", label: "每页", value: "12" }, { key: "order", label: "排序列", value: "id" }, { key: "dir", label: "方向 asc|desc", value: "asc" }] } },
      { divider: true, label: "-" },
      { section: true, label: "聚合" },
      { label: "统计行数", action: { kind: "command", command: "COUNT", opType: "aggregate", title: "统计行数" } },
      { label: "求和...", action: { kind: "dialog", opType: "aggregate", title: "求和", template: "SUM({attr})", fields: [{ key: "attr", label: "字段", value: "age" }] } },
      { label: "平均值...", action: { kind: "dialog", opType: "aggregate", title: "平均值", template: "AVG({attr})", fields: [{ key: "attr", label: "字段", value: "age" }] } },
      { label: "最小值...", action: { kind: "dialog", opType: "aggregate", title: "最小值", template: "MIN({attr})", fields: [{ key: "attr", label: "字段", value: "age" }] } },
      { label: "最大值...", action: { kind: "dialog", opType: "aggregate", title: "最大值", template: "MAX({attr})", fields: [{ key: "attr", label: "字段", value: "age" }] } },
      { divider: true, label: "-" },
      { section: true, label: "分组与 JSON 分页" },
      { label: "GROUP BY...", action: { kind: "dialog", opType: "generic", title: "GROUP BY", template: "GROUP BY({cols})", fields: [{ key: "cols", label: "列（逗号分隔）", value: "dept" }] } },
      { label: "ORDER BY...", action: { kind: "dialog", opType: "generic", title: "ORDER BY", template: "ORDER BY({col},{dir})", fields: [{ key: "col", label: "列", value: "id" }, { key: "dir", label: "asc|desc", value: "asc" }] } },
      { label: "PAGE_JSON...", action: { kind: "dialog", opType: "query_page", title: "PAGE_JSON", template: "PAGE_JSON({page},{size},{order},{dir})", fields: [{ key: "page", label: "页码", value: "1" }, { key: "size", label: "每页", value: "12" }, { key: "order", label: "排序列", value: "id" }, { key: "dir", label: "方向", value: "asc" }] } }
    ]
  },
  {
    label: "事务(Transaction)",
    key: "txn",
    items: [
      { section: true, label: "会话事务" },
      { label: "开始事务", action: { kind: "command", command: "BEGIN", opType: "txn_begin", title: "开始事务" } },
      { label: "提交事务", action: { kind: "command", command: "COMMIT", opType: "txn_commit", title: "提交事务" } },
      { label: "回滚事务", action: { kind: "command", command: "ROLLBACK", opType: "txn_rollback", title: "回滚事务" } },
      { divider: true, label: "-" },
      { section: true, label: "恢复与检查点" },
      { label: "事务与 Savepoint 面板...", action: { kind: "txnRecovery" } },
      { label: "SHOW CHECKPOINTS", action: { kind: "command", command: "SHOW CHECKPOINTS", opType: "generic", title: "SHOW CHECKPOINTS" } },
      { label: "按 checkpoint 冷恢复...", action: { kind: "recoverCheckpoint" } }
    ]
  },
  {
    label: "工具",
    key: "tools",
    items: [
      { section: true, label: "运维与诊断" },
      { label: "SHOW TUNING 摘要...", action: { kind: "walRecovery" } },
      { label: "导出诊断包…", action: { kind: "exportBundle" } },
      { label: "冷备 data_dir…", action: { kind: "backupStorageBundle" } },
      { divider: true, label: "-" },
      { section: true, label: "会话调优" },
      { label: "MDB 耐久档位…", action: { kind: "setDurability" } },
      { label: "SET DURABILITY 0（最快）", action: { kind: "command", command: "SET DURABILITY 0", opType: "generic", title: "SET DURABILITY 0" } },
      { label: "SET DURABILITY 1（默认）", action: { kind: "command", command: "SET DURABILITY 1", opType: "generic", title: "SET DURABILITY 1" } },
      { label: "SET DURABILITY 2（最强）", action: { kind: "command", command: "SET DURABILITY 2", opType: "generic", title: "SET DURABILITY 2" } },
      { divider: true, label: "-" },
      { section: true, label: "引擎观测" },
      { label: "SHOW TUNING", action: { kind: "command", command: "SHOW TUNING", opType: "generic", title: "SHOW TUNING" } },
      { label: "SHOW TUNING JSON", action: { kind: "command", command: "SHOW TUNING JSON", opType: "generic", title: "SHOW TUNING JSON" } },
      { label: "SHOW STORAGE", action: { kind: "command", command: "SHOW STORAGE", opType: "generic", title: "SHOW STORAGE" } },
      { divider: true, label: "-" },
      { section: true, label: "其他" },
      { label: "清空日志", action: { kind: "command", command: "__CLEAR_LOG__", title: "清空日志" } },
      { label: "帮助", action: { kind: "help" } }
    ]
  }
];

function now() {
  return new Date().toLocaleTimeString();
}

function persistLogs() {
  localStorage.setItem(LS_LOGS, JSON.stringify(logs.value));
  try {
    localStorage.removeItem(LS_LOGS_LEGACY);
  } catch {
    /* ignore */
  }
}

function loadLogCollapsed() {
  const raw = localStorage.getItem(LS_LOG_COLLAPSED) ?? localStorage.getItem(LS_LOG_COLLAPSED_LEGACY);
  if (raw === null) {
    logCollapsed.value = true;
    return;
  }
  logCollapsed.value = raw === "1";
}

function persistLogCollapsed() {
  localStorage.setItem(LS_LOG_COLLAPSED, logCollapsed.value ? "1" : "0");
  try {
    localStorage.removeItem(LS_LOG_COLLAPSED_LEGACY);
  } catch {
    /* ignore */
  }
}

async function loadSettings() {
  settingsSyncing.value = true;
  try {
    const s = await invoke<UiSettings>("get_settings");
    settings.value = sanitizeSettings({ ...defaultSettings, ...(s as Partial<UiSettings>) });
  } catch {
    settings.value = { ...defaultSettings };
  }
  settingsDirty.value = false;
  window.setTimeout(() => {
    settingsSyncing.value = false;
  }, 0);
}

async function persistSettings() {
  settingsSyncing.value = true;
  try {
    const sanitized = sanitizeSettings(settings.value);
    if (!sameSettings(settings.value, sanitized)) {
      settings.value = sanitized;
    }
    await invoke("set_settings", { settings: sanitized });
    settingsDirty.value = false;
    settingsLastSavedAt.value = now();
  } finally {
    window.setTimeout(() => {
      settingsSyncing.value = false;
    }, 0);
  }
}

watch(
  settings,
  () => {
    if (settingsSyncing.value) return;
    settingsDirty.value = true;
    if (settingsSliderDragging.value) return;
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

function sanitizeSettings(input: Partial<UiSettings>): UiSettings {
  const rawAccent = String(input.accent ?? "").trim();
  const safeAccent = normalizeHexColor(rawAccent, defaultSettings.accent);
  const safeTextMain = normalizeHexColor(String(input.textMain ?? "").trim(), defaultSettings.textMain);
  const safeTextRegular = normalizeHexColor(String(input.textRegular ?? "").trim(), defaultSettings.textRegular);
  const safeTextSoft = normalizeHexColor(String(input.textSoft ?? "").trim(), defaultSettings.textSoft);
  const safePageBg = normalizeHexColor(String(input.pageBg ?? "").trim(), defaultSettings.pageBg);
  const safeTableBg = normalizeHexColor(String(input.tableBg ?? "").trim(), defaultSettings.tableBg);
  const bgMode: UiSettings["bgMode"] =
    input.bgMode === "video" ? "video" : input.bgMode === "image" ? "image" : "gradient";
  const clamp = (n: number, min: number, max: number) => Math.max(min, Math.min(max, n));
  const num = (v: unknown, fallback: number) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  };
  return {
    accent: safeAccent,
    textMain: safeTextMain,
    textRegular: safeTextRegular,
    textSoft: safeTextSoft,
    pageBg: safePageBg,
    tableBg: safeTableBg,
    bgMode,
    bgImageUrl: String(input.bgImageUrl ?? ""),
    bgImageOpacity: clamp(num(input.bgImageOpacity, defaultSettings.bgImageOpacity), 0.05, 0.8),
    bgVideoUrl: String(input.bgVideoUrl ?? ""),
    bgVideoOpacity: clamp(num(input.bgVideoOpacity, defaultSettings.bgVideoOpacity), 0.05, 0.8),
    panelOpacity: clamp(num(input.panelOpacity, defaultSettings.panelOpacity), 0.6, 1),
    tableViewOpacity: clamp(num(input.tableViewOpacity, defaultSettings.tableViewOpacity), 0.15, 1),
    logPanelOpacity: clamp(num(input.logPanelOpacity, defaultSettings.logPanelOpacity), 0.35, 1),
    fontScale: clamp(num(input.fontScale, defaultSettings.fontScale), 0.9, 1.2),
    denseMode: Boolean(input.denseMode),
    animations: Boolean(input.animations),
    sidebarWidth: clamp(num(input.sidebarWidth, defaultSettings.sidebarWidth), 200, 460),
    cornerScale: clamp(num(input.cornerScale, defaultSettings.cornerScale), 0.8, 1.35),
    shadowScale: clamp(num(input.shadowScale, defaultSettings.shadowScale), 0.6, 1.5),
    logFontScale: clamp(num(input.logFontScale, defaultSettings.logFontScale), 0.88, 1.25),
    logLineHeight: clamp(num(input.logLineHeight, defaultSettings.logLineHeight), 1.3, 1.9),
    borderContrast: clamp(num(input.borderContrast, defaultSettings.borderContrast), 0.75, 1.4),
    panelBrightness: clamp(num(input.panelBrightness, defaultSettings.panelBrightness), 0.5, 1.2),
    logHighlightIntensity: clamp(num(input.logHighlightIntensity, defaultSettings.logHighlightIntensity), 0.75, 1.4)
  };
}

function sameSettings(a: UiSettings, b: UiSettings) {
  return (
    a.accent === b.accent &&
    a.textMain === b.textMain &&
    a.textRegular === b.textRegular &&
    a.textSoft === b.textSoft &&
    a.pageBg === b.pageBg &&
    a.tableBg === b.tableBg &&
    a.bgMode === b.bgMode &&
    a.bgImageUrl === b.bgImageUrl &&
    a.bgVideoUrl === b.bgVideoUrl &&
    a.bgVideoOpacity === b.bgVideoOpacity &&
    a.bgImageOpacity === b.bgImageOpacity &&
    a.panelOpacity === b.panelOpacity &&
    a.tableViewOpacity === b.tableViewOpacity &&
    a.logPanelOpacity === b.logPanelOpacity &&
    a.fontScale === b.fontScale &&
    a.denseMode === b.denseMode &&
    a.animations === b.animations &&
    a.sidebarWidth === b.sidebarWidth &&
    a.cornerScale === b.cornerScale &&
    a.shadowScale === b.shadowScale &&
    a.logFontScale === b.logFontScale &&
    a.logLineHeight === b.logLineHeight &&
    a.borderContrast === b.borderContrast &&
    a.panelBrightness === b.panelBrightness &&
    a.logHighlightIntensity === b.logHighlightIntensity
  );
}

function onSettingsSliderInput() {
  settingsSliderDragging.value = true;
}

async function onSettingsSliderChange() {
  settingsSliderDragging.value = false;
  if (settingsPersistTimer !== null) {
    window.clearTimeout(settingsPersistTimer);
    settingsPersistTimer = null;
  }
  await persistSettings();
}

function onSidebarResizeMove(ev: MouseEvent) {
  if (!sidebarResizing.value) return;
  const next = Math.max(200, Math.min(460, sidebarDragStartWidth + (ev.clientX - sidebarDragStartX)));
  if (settings.value.sidebarWidth === next) return;
  settings.value.sidebarWidth = next;
}

function stopSidebarResize() {
  if (!sidebarResizing.value) return;
  sidebarResizing.value = false;
  window.removeEventListener("mousemove", onSidebarResizeMove);
  window.removeEventListener("mouseup", stopSidebarResize);
  document.body.style.cursor = "";
  document.body.style.userSelect = "";
}

function startSidebarResize(ev: MouseEvent) {
  if (!showSidebar.value) return;
  sidebarResizing.value = true;
  sidebarDragStartX = ev.clientX;
  sidebarDragStartWidth = settings.value.sidebarWidth;
  document.body.style.cursor = "col-resize";
  document.body.style.userSelect = "none";
  window.addEventListener("mousemove", onSidebarResizeMove);
  window.addEventListener("mouseup", stopSidebarResize);
}

function loadLogs() {
  const raw = localStorage.getItem(LS_LOGS) ?? localStorage.getItem(LS_LOGS_LEGACY);
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
      `C:/tmp/structdb_data\n` +
      `C:/tmp/structdb_gui_case`;
    showWorkspaceWarning.value = true;
  }
}

function logLine(line: string) {
  logs.value.push(line);
  if (logs.value.length > 500) logs.value.shift();
  persistLogs();
}

function appendSessionLogHeader() {
  const ts = new Date().toLocaleString();
  const mode = viewMode.value === "log" ? "LOG_WINDOW" : viewMode.value === "cli" ? "CLI_WINDOW" : "MAIN_WINDOW";
  logLine(`\n[SESSION] ${mode} started at ${ts}`);
}

function stackKey(item: UndoUnit) {
  return `${item.unitId}::${item.createdAt}`;
}

type StackExecTrace = { kind: "undo" | "redo"; at: string; commands: string[] };
const stackExecTraceByUnitId = ref<Record<string, StackExecTrace>>({});

function recordStackExecTrace(unit: UndoUnit, kind: "undo" | "redo", r: StackExecResult) {
  const cmds = (r.executedCommands ?? []).filter((x) => !!x);
  if (!cmds.length) return;
  stackExecTraceByUnitId.value[unit.unitId] = { kind, at: now(), commands: cmds };
}

function isReversible(item: UndoUnit) {
  return item.ops.some((x) => !!x.backward);
}

async function viewOperationLog(item: UndoUnit) {
  const all = logs.value.join("\n");
  const lines = all.split(/\r?\n/);
  const firstForward = item.ops[0]?.forward || "";
  const trace = stackExecTraceByUnitId.value[item.unitId];
  const primaryCommand = (trace?.commands?.[0] || firstForward || "").trim();
  const needle = `> ${firstForward}`.trim();
  let hit = -1;
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const t = (lines[i] ?? "").trim();
    if (!t) continue;
    if (t === needle) {
      hit = i;
      break;
    }
  }
  if (hit < 0 && firstForward && firstForward !== "[SCRIPT]") {
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      const t = (lines[i] ?? "").trim();
      if (t.includes(firstForward)) {
        hit = i;
        break;
      }
    }
  }
  const start = Math.max(0, (hit >= 0 ? hit : lines.length - 1) - 10);
  const end = Math.min(lines.length, (hit >= 0 ? hit : lines.length - 1) + 18);
  const snippet = lines.slice(start, end).join("\n");
  const traceText = trace
    ? `\n\n--- 最近一次 ${trace.kind === "undo" ? "回退" : "重做"} 追踪（${trace.at}）---\n${trace.commands
        .map((c) => `- ${c}`)
        .join("\n")}`
    : "";
  const inverseSource = item.ops[0]?.inverseSource?.trim() || "-";
  const isTruncatedSnapshot = /truncated\s*=\s*true/i.test(inverseSource);
  const snapshotWarn = isTruncatedSnapshot
    ? "\n⚠ 注意：当前逆向脚本来自截断快照（非全量），超出上限的数据不会被本次回退恢复。"
    : "";
  await openUiMessage(
    "alert",
    `回看日志：${item.savepointName}`,
    `unit=${item.unitId}\nops=${item.ops.length}\n命令：${primaryCommand || "-"}\n逆向来源：${inverseSource}${snapshotWarn}\n时间：${item.createdAt}${traceText}\n\n--- 日志上下文（${start + 1}-${end}）---\n${snippet || "(日志为空)"}`
  );
}

async function selectStackItem(item: UndoUnit) {
  selectedStackKey.value = stackKey(item);
  stackPreviewUnit.value = item;
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
    if (m) return `DELETE(${normalizeHeapRowIdForCmd(m[1] ?? "")})`;
  }
  if (type === "table_rename") {
    const m = /RENAME TABLE\(([^)]+)\)/i.exec(forward);
    if (m && state.value.currentTable) return `RENAME TABLE(${state.value.currentTable})`;
  }
  if (cmd === "BEGIN") return "ROLLBACK";
  return undefined;
}

/** Canonical decimal for integer heap `id` (matches Rust remap keys; avoids `03` vs `3`). */
function normalizeHeapRowIdForCmd(raw: string): string {
  const t = String(raw ?? "").trim();
  if (!t) return t;
  if (/^-?\d+$/.test(t)) {
    const n = Number(t);
    if (Number.isSafeInteger(n)) return String(n);
  }
  return t;
}

/** Grid cell vs UPDATE/DELETE id (PAGE_JSON may use number-like strings; avoid `===` misses). */
function gridRowMatchesId(row: string[], idColumnIdx: number, idArg: string): boolean {
  if (idColumnIdx < 0 || idColumnIdx >= row.length) return false;
  const a = String(row[idColumnIdx] ?? "").trim();
  const b = String(idArg ?? "").trim();
  if (a === b) return true;
  if (/^-?\d+$/.test(a) && /^-?\d+$/.test(b)) return Number(a) === Number(b);
  return false;
}

/** DEFATTR 列顺序下的单元格值（仅跳过「#」），含 `id` 列，与引擎 INSERT/UPDATE 的 `1 + schema.size()` 一致。 */
function rowValuesInSchemaOrder(hit: string[], viewHeaders: string[]): string[] {
  const idxByHeader = new Map<string, number>();
  for (let i = 0; i < viewHeaders.length; i += 1) {
    idxByHeader.set(viewHeaders[i]!.trim().toLowerCase(), i);
  }
  const out: string[] = [];
  for (const c of tableColumns.value) {
    const name = String(c.name ?? "").trim();
    if (!name || name === "#") continue;
    const idx = idxByHeader.get(name.toLowerCase());
    out.push(idx === undefined ? "" : `${hit[idx] ?? ""}`.trim());
  }
  return out;
}

function inferUpdateInverseFromCurrentView(forward: string): string | undefined {
  const m = /UPDATE\(([^)]*)\)/i.exec(forward);
  if (!m) return undefined;
  const args = m[1]
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x.length > 0);
  if (args.length < 1) return undefined;
  const id = normalizeHeapRowIdForCmd(args[0]!);
  const idIdx = idColumnIndex.value;
  if (idIdx < 0) return undefined;
  const hit = tableViewData.value.rows.find((row) => gridRowMatchesId(row, idIdx, id));
  if (!hit) return undefined;
  const oldValues = rowValuesInSchemaOrder(hit, tableViewData.value.headers);
  return `UPDATE(${[id, ...oldValues].join(",")})`;
}

function inferDeleteInverseFromCurrentView(forward: string): string | undefined {
  const m = /DELETE\(([^,)]+)\)/i.exec(forward);
  if (!m) return undefined;
  const id = normalizeHeapRowIdForCmd(m[1] ?? "");
  if (!id) return undefined;
  const idIdx = idColumnIndex.value;
  if (idIdx < 0) return undefined;
  const hit = tableViewData.value.rows.find((row) => gridRowMatchesId(row, idIdx, id));
  if (!hit) return undefined;
  const vals = rowValuesInSchemaOrder(hit, tableViewData.value.headers);
  return `INSERT(${[id, ...vals].join(",")})`;
}

function inferOpTypeFromCommand(command: string): OperationType {
  const up = normalizeCmd(command).toUpperCase();
  if (up.startsWith("INSERT(") || up.startsWith("BULKINSERT(")) return "data_insert";
  if (up.startsWith("UPDATE(")) return "data_update";
  if (up.startsWith("DELETE(")) return "data_delete";
  if (up.startsWith("CREATE TABLE(")) return "table_create";
  if (up.startsWith("DROP TABLE(")) return "table_drop";
  if (up.startsWith("RENAME TABLE(")) return "table_rename";
  if (up.startsWith("USE(") || up.startsWith("USE ")) return "table_use";
  if (up.startsWith("DEFATTR(")) return "schema_defattr";
  if (up.startsWith("ADDATTR(")) return "schema_defattr";
  if (up.startsWith("SET PRIMARY KEY(")) return "schema_set_pk";
  if (up === "BEGIN") return "txn_begin";
  if (up === "COMMIT") return "txn_commit";
  if (up === "ROLLBACK") return "txn_rollback";
  if (up.startsWith("SAVEPOINT ")) return "txn_savepoint";
  if (up.startsWith("ROLLBACK TO ")) return "txn_rollback_to";
  if (up.startsWith("RELEASE SAVEPOINT ")) return "txn_release_savepoint";
  return "generic";
}

function inferBackwardForCommand(command: string, opType: OperationType): string | undefined {
  const up = normalizeCmd(command).toUpperCase();
  const grid =
    up.startsWith("UPDATE(")
      ? inferUpdateInverseFromCurrentView(command)
      : up.startsWith("DELETE(")
        ? inferDeleteInverseFromCurrentView(command)
        : undefined;
  return inferInverse(opType, command) ?? grid;
}

function commandHasEmptyUpdateInsertArg(cmd: string | undefined): boolean {
  const t = String(cmd ?? "").trim();
  if (!t) return false;
  const m = /^(UPDATE|INSERT)\(([\s\S]*)\)$/i.exec(t);
  if (!m) return false;
  const inner = m[2] ?? "";
  const parts = inner.split(",").map((x) => x.trim());
  return parts.some((x) => x.length === 0);
}

async function inferBackwardPayloadByBackend(
  command: string,
  opType: OperationType
): Promise<{ backward?: string; source?: string }> {
  try {
    const r = await invoke<InverseInferResult>("infer_inverse_command", { command, opType });
    const backward = (r?.backward ?? "").trim() || undefined;
    const source = (r?.source ?? "").trim() || undefined;
    return { backward, source };
  } catch {
    return {};
  }
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

async function openAboutModal() {
  await refreshDllInfo();
  await refreshRuntimeArtifacts();
  showAboutModal.value = true;
}

function applySettingsPreset(preset: SettingsPreset) {
  settings.value = sanitizeSettings({ ...settings.value, ...preset.settings });
}

async function resetSettings() {
  const ok = await openUiMessage("confirm", "恢复默认设置", "确认恢复所有设置为默认值？");
  if (!ok) return;
  settings.value = { ...defaultSettings };
  await persistSettings();
}

function openSettingsImport() {
  settingsImportInput.value?.click();
}

function exportSettingsJson() {
  const payload = JSON.stringify(sanitizeSettings(settings.value), null, 2);
  const blob = new Blob([payload], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  a.href = url;
  a.download = `structdb-gui-settings-${ts}.json`;
  a.click();
  URL.revokeObjectURL(url);
}

function onSettingsImportChange(ev: Event) {
  const input = ev.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = async () => {
    try {
      const raw = JSON.parse(String(reader.result ?? "{}")) as Partial<UiSettings>;
      settings.value = sanitizeSettings({ ...settings.value, ...raw });
      await persistSettings();
      await openUiMessage("alert", "导入成功", "设置已导入并生效。");
    } catch (e) {
      await openUiMessage("alert", "导入失败", `设置 JSON 解析失败：${String(e)}`);
    }
  };
  reader.readAsText(file);
  input.value = "";
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

function pickBackgroundVideoFile() {
  bgVideoFileInput.value?.click();
}

function onBackgroundVideoFileChange(ev: Event) {
  const input = ev.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file) return;
  if (!file.type.startsWith("video/")) {
    void openUiMessage("alert", "文件类型错误", "请选择视频文件（mp4/webm/ogg 等）");
    input.value = "";
    return;
  }
  const reader = new FileReader();
  reader.onload = () => {
    settings.value.bgMode = "video";
    settings.value.bgVideoUrl = String(reader.result ?? "");
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
  return validateMdbDefattrCell(type, value);
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
}

async function computeNextRowId(): Promise<string> {
  try {
    const pr = await invoke<PageResult>("query_page", {
      pageNo: 1,
      pageSize: 1,
      orderKey: "id",
      descending: true
    });
    const tv = pr.headers.length === 1 && pr.headers[0] === "raw" ? tableViewData.value : pr;
    const idIdx = tv.headers.findIndex((h) => h.trim().toLowerCase() === "id");
    const top = idIdx >= 0 ? (tv.rows?.[0]?.[idIdx] ?? "") : "";
    const n = Number(String(top).trim());
    if (Number.isFinite(n) && n > 0) {
      return String(Math.floor(n) + 1);
    }
  } catch {
    // fallback below
  }
  const idIdx = idColumnIndex.value >= 0 ? idColumnIndex.value : 1;
  let maxId = 0;
  for (const row of tableViewData.value.rows) {
    const n = Number((row?.[idIdx] ?? "").trim());
    if (Number.isFinite(n) && n > maxId) {
      maxId = n;
    }
  }
  return String(maxId > 0 ? maxId + 1 : 1);
}

function buildQuickRowFieldsFromCurrentTable() {
  const rid = quickRowNextId.value.trim() || "1";
  const fields = tableColumns.value
    .map((c) => ({ name: String(c.name ?? "").trim(), ty: String(c.ty ?? "string").trim() || "string" }))
    .filter((c) => !!c.name && c.name !== "#")
    .map((c) => ({
      ...c,
      value: c.name.toLowerCase() === "id" ? rid : "0"
    }));
  quickRowFields.value = fields;
}

async function openQuickAddRowModal() {
  if (!state.value.currentTable.trim()) {
    await openUiMessage("alert", "未选择表", "请先在左侧选择当前表。");
    return;
  }
  cancelEditRow();
  quickRowNextId.value = await computeNextRowId();
  buildQuickRowFieldsFromCurrentTable();
  showQuickRowModal.value = true;
}

async function submitQuickAddRow() {
  const rowKey = quickRowNextId.value.trim() || "1";
  const vals = quickRowFields.value.map((f) => (f.value ?? "").trim() || "0");
  const cmd = `INSERT(${[rowKey, ...vals].join(",")})`;
  await runCommand(cmd, { opType: "data_insert", title: `新增行 id=${rowKey}` });
  showQuickRowModal.value = false;
}

function isEnterKey(ev: KeyboardEvent): boolean {
  return ev.key === "Enter" || ev.key === "NumpadEnter";
}

/** 弹窗内 Enter 触发主操作：避免与下拉、开关、按钮等抢键 */
function shouldModalEnterSubmit(ev: KeyboardEvent): boolean {
  const t = ev.target as HTMLElement | null;
  if (!t) return false;
  if (t.tagName === "TEXTAREA") return false;
  if (t.closest(".el-textarea__inner")) return false;
  if (t.closest(".el-select__popper") || t.closest(".el-select-dropdown__list")) return false;
  if (t.closest(".el-select")) return false;
  if (t.closest(".el-switch")) return false;
  if (t.closest("button, [role='button']")) return false;
  return true;
}

function onCreateTableWizardEnter(ev: KeyboardEvent) {
  if (!isEnterKey(ev) || !showCreateTableWizard.value || busy.value) return;
  if (!shouldModalEnterSubmit(ev)) return;
  ev.preventDefault();
  void submitCreateTableWizard();
}

function onGenericDialogEnter(ev: KeyboardEvent) {
  if (!isEnterKey(ev) || !showDialog.value || busy.value) return;
  if (!shouldModalEnterSubmit(ev)) return;
  ev.preventDefault();
  void submitDialog();
}

function handleQuickToolEnter(ev: KeyboardEvent, action: "addAttr" | "delAttr") {
  if (!isEnterKey(ev)) return;
  ev.preventDefault();
  ev.stopPropagation();
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
  if (!isKnownDefattrType(addAttrType.value)) {
    await openUiMessage("alert", "类型无效", "当前类型不是 StructDB DEFATTR 支持的类型。");
    return;
  }
  const cmd = buildAddAttrCommand(name, addAttrType.value);
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
  const cols = createCols.value
    .map((c) => ({ name: c.name.trim(), type: c.type.trim() }))
    .filter((c) => !!c.name && !!c.type);
  const seenNames = new Set<string>();
  for (const c of cols) {
    const key = c.name.toLowerCase();
    if (seenNames.has(key)) {
      await openUiMessage("alert", "列名重复", `列「${c.name}」重复，请修改后再创建。`);
      return;
    }
    seenNames.add(key);
    if (!isKnownDefattrType(c.type)) {
      await openUiMessage(
        "alert",
        "不支持的类型",
        `列「${c.name}」的类型「${c.type}」不是 StructDB DEFATTR 支持的类型（与引擎 known_logical_type 一致）。`
      );
      return;
    }
  }
  if (createAlsoSetPk.value && createPk.value.trim()) {
    const pk = createPk.value.trim();
    const names = new Set(cols.map((c) => c.name.toLowerCase()));
    if (!names.has(pk.toLowerCase())) {
      await openUiMessage(
        "alert",
        "主键列不在 DEFATTR 中",
        `SET PRIMARY KEY(${pk}) 要求该列已出现在属性列表中。请添加列「${pk}」或修改主键名，或关闭「同时执行 SET PRIMARY KEY」。`
      );
      return;
    }
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
  const id = normalizeHeapRowIdForCmd(draft[idIdx] ?? "");
  if (!id) {
    await openUiMessage("alert", "更新失败", "当前行缺少 id，无法更新");
    focusCellEditor(rowIndex, idIdx);
    return;
  }

  for (let i = 0; i < headers.length; i += 1) {
    const err = validateCellValue(cols[i]?.ty ?? "string", draft[i] ?? "");
    if (err) {
      await openUiMessage("alert", "更新失败", `${headers[i]} ${err}`);
      setCellInvalid(rowIndex, i);
      focusCellEditor(rowIndex, i);
      return;
    }
  }

  const idxByHeader = new Map(headers.map((h, i) => [h.trim().toLowerCase(), i]));
  const values = tableColumns.value
    .map((c) => String(c.name ?? "").trim())
    .filter((n) => n && n !== "#")
    .map((name) => {
      const colIdx = idxByHeader.get(name.toLowerCase());
      return colIdx === undefined ? "" : (draft[colIdx] ?? "").trim();
    });
  const schemaColCount = tableColumns.value.filter((c) => {
    const n = String(c.name ?? "").trim();
    return n && n !== "#";
  }).length;
  while (values.length < schemaColCount) {
    values.push("");
  }
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

/** 分页拉取当前表、当前排序下的全部行 id（用于整表快捷重排）。 */
const kReorderScanPageSize = 500;
const kReorderMaxRows = 50_000;

function findIdColumnIndexInHeaders(headers: string[]): number {
  return headers.findIndex((h) => h.trim().toLowerCase() === "id");
}

async function collectAllRowIdsForCurrentTableSorted(): Promise<string[]> {
  const tab = activeTableTab.value;
  if (!tab?.table?.trim()) {
    throw new Error("未选择当前表");
  }
  const tname = tab.table.trim();
  if (state.value.currentTable !== tname) {
    state.value.currentTable = tname;
    await invoke("set_current_table", { table: tname });
  }
  const orderKey = tab.orderKey.trim() || "id";
  const descending = tab.desc;
  const ids: string[] = [];
  const seen = new Set<string>();
  let pageNo = 1;
  for (;;) {
    const pr = await invoke<PageResult>("query_page", {
      pageNo,
      pageSize: kReorderScanPageSize,
      orderKey,
      descending
    });
    if (!shouldApplyPageResult(pr)) {
      throw new Error("分页结果无效或含错误，已中止收集行 id");
    }
    const idIdx = findIdColumnIndexInHeaders(pr.headers);
    if (idIdx < 0) {
      throw new Error("结果中无名为 id 的列，无法重排行锚");
    }
    const rows = pr.rows ?? [];
    if (!rows.length) break;
    for (const row of rows) {
      const id = String(row[idIdx] ?? "").trim();
      if (!id) continue;
      if (seen.has(id)) {
        throw new Error(`发现重复行 id：${id}`);
      }
      seen.add(id);
      ids.push(id);
    }
    if (rows.length < kReorderScanPageSize) break;
    pageNo += 1;
    if (ids.length > kReorderMaxRows) {
      throw new Error(`行数超过 ${kReorderMaxRows}，请分批处理或联系管理员扩展上限`);
    }
  }
  return ids;
}

/** 按当前排序将全表行 id 重编号为 1…n（一条 CONFIRM_REORDER）。 */
async function quickReorderWholeTableIds() {
  if (!activeTableTab.value?.table?.trim()) {
    await openUiMessage("alert", "无当前表", "请先打开或 USE 一张表");
    return;
  }
  if (txnRecorder.value.active) {
    await openUiMessage("alert", "事务中", "请先 COMMIT 或 ROLLBACK 后再快捷重排");
    return;
  }
  const table = activeTableTab.value.table.trim();
  let oldIds: string[] = [];
  busy.value = true;
  try {
    oldIds = await collectAllRowIdsForCurrentTableSorted();
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    await openUiMessage("alert", "收集行 id 失败", msg);
    return;
  } finally {
    busy.value = false;
  }
  if (!oldIds.length) {
    await openUiMessage("alert", "无数据", "当前表没有可重排的行 id");
    return;
  }
  const newIds = oldIds.map((_, i) => String(i + 1));
  let anyChange = false;
  for (let i = 0; i < oldIds.length; i++) {
    if (oldIds[i] !== newIds[i]) anyChange = true;
  }
  if (!anyChange) {
    await openUiMessage("alert", "无需重排", `按当前排序键，id 已是 1…${oldIds.length}，无需提交。`);
    return;
  }
  const pairs: [string, string][] = oldIds.map((o, i) => [o, newIds[i]!]);
  const ok = await openUiMessage(
    "confirm",
    "快捷重排 id",
    `表「${table}」共 ${oldIds.length} 行：按当前排序（${activeTableTab.value.orderKey || "id"}，${
      activeTableTab.value.desc ? "降序" : "升序"
    }）将行锚重编号为 1…${oldIds.length}。\n此操作会执行 CONFIRM_REORDER，失败则整批不生效。是否继续？`
  );
  if (!ok) return;
  const json = JSON.stringify({ table, pairs });
  const cmd = `CONFIRM_REORDER(${json})`;
  const backwardPairs: [string, string][] = pairs.map(([o, n]) => [n, o]);
  const backwardCmd = `CONFIRM_REORDER(${JSON.stringify({ table, pairs: backwardPairs })})`;
  page.value = 1;
  await runCommand(cmd, {
    opType: "generic",
    title: `快捷重排 id（${table}，${oldIds.length} 行）`,
    backward: backwardCmd
  });
}

async function onPageSizeChange(v: number | undefined) {
  const n = Number(v ?? state.value.pageSize ?? 12);
  state.value.pageSize = Math.max(1, Math.min(5000, Number.isFinite(n) ? Math.floor(n) : 12));
  page.value = 1;
  await refreshPage();
}

/** 是否存在可执行的逆向脚本（有则视为可独立单步撤销）。 */
function undoPayloadPresent(op: UndoOp): boolean {
  return typeof op.backward === "string" && op.backward.trim().length > 0;
}

/** 是否写入撤销栈：仅「表数据变更」与「表/列结构」相关命令；其余（查询、聚合、SHOW/SCAN、USE、事务以外的杂项等）跳过，避免回退链被只读命令污染。 */
function shouldRecordOpInUndoChain(op: UndoOp): boolean {
  const fwd = normalizeCmd(op.forward);
  const up = fwd.toUpperCase();
  if (up === "[SCRIPT]") return false;
  if (op.type.startsWith("txn_")) return true;
  const tableDataOrSchema = new Set<OperationType>([
    "data_insert",
    "data_delete",
    "data_update",
    "table_create",
    "table_drop",
    "table_rename",
    "schema_defattr",
    "schema_set_pk"
  ]);
  if (tableDataOrSchema.has(op.type)) return true;
  const inferred = inferOpTypeFromCommand(op.forward);
  if (tableDataOrSchema.has(inferred)) return true;
  if (up.startsWith("DELATTR(")) return true;
  if (up.startsWith("ADDATTR(")) return true;
  if (up === "RESET" || up.startsWith("RESET ")) return true;
  if (up.startsWith("CONFIRM_REORDER(")) return true;
  if (up.startsWith("BULKINSERTFAST(")) return true;
  return false;
}

/** 事务栈列表展示用（内部 savepoint 名对用户可读化）。 */
function stackSavepointDisplay(name: string): string {
  switch (name) {
    case "__single__":
      return "单步";
    case "__txn_irrev__":
      return "连带批次（无逆向）";
    case "__commit__":
      return "提交前连带";
    case "__script__":
      return "脚本";
    default:
      return name;
  }
}

/** 将事务中累积的「无逆向」操作打成一批（连带撤销）；名称须以 `__` 开头以免触发错误的 ROLLBACK TO。 */
function flushTxnPendingOpsAsUnit(savepointName: string) {
  const chunk = txnRecorder.value.pendingOps.splice(0);
  if (chunk.length > 0) {
    pushHistoryUnit(buildUndoUnit(savepointName, chunk));
  }
}

function buildUndoUnit(
  savepointName: string,
  ops: UndoOp[],
  status: UndoUnitStatus = "ready"
): UndoUnit {
  const unitId = `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const tables = Array.from(new Set(ops.map((x) => x.table || state.value.currentTable).filter((x) => !!x)));
  return {
    unitId,
    txnId: txnRecorder.value.txnId || `txn-${Date.now()}`,
    savepointName,
    tablesTouched: tables as string[],
    ops,
    createdAt: now(),
    status
  };
}

/** 新历史：仅压入撤销栈；重做栈保留，可随时重做（与当前数据冲突时由后端中断）。 */
function pushHistoryUnit(unit: UndoUnit) {
  undoStack.value.push(unit);
  if (undoStack.value.length > 1000) undoStack.value.shift();
}

async function persistStackState() {
  try {
    await invoke("save_stack_units", {
      undoUnitsJson: JSON.stringify(undoStack.value),
      redoUnitsJson: JSON.stringify(redoStack.value)
    });
  } catch (e) {
    logLine(`[STACK][WARN] persist failed: ${String(e)}`);
  }
}

async function loadStackState() {
  try {
    const raw = await invoke<string>("load_stack_units");
    const v = JSON.parse(raw || "{}");
    undoStack.value = Array.isArray(v.undo_units) ? (v.undo_units as UndoUnit[]) : [];
    let redo = Array.isArray(v.redo_units) ? (v.redo_units as UndoUnit[]) : [];
    const legacySus = Array.isArray(v.legacy_suspended_redo_units)
      ? (v.legacy_suspended_redo_units as UndoUnit[])
      : Array.isArray(v.suspended_redo_units)
        ? (v.suspended_redo_units as UndoUnit[])
        : [];
    if (legacySus.length > 0) {
      redo = [...legacySus, ...redo];
      logLine(`[STACK] 已将旧版暂存重做 (${legacySus.length}) 并入重做栈`);
    }
    redoStack.value = redo;
    const warnings: string[] = Array.isArray(v.warnings) ? v.warnings : [];
    for (const w of warnings) {
      logLine(`[STACK][WARN] ${w}`);
    }
  } catch (e) {
    logLine(`[STACK][WARN] load failed: ${String(e)}`);
  }
}

function trackTxnCommand(op: UndoOp) {
  const cmd = normalizeCmd(op.forward).toUpperCase();
  if (cmd.startsWith("BEGIN")) {
    txnRecorder.value.active = true;
    txnRecorder.value.txnId = `txn-${Date.now()}`;
    txnRecorder.value.currentSavepoint = "__autosave__";
    txnRecorder.value.pendingOps = [];
    return;
  }
  if (!txnRecorder.value.active) {
    if (!shouldRecordOpInUndoChain(op)) return;
    const unit = buildUndoUnit("__single__", [op]);
    pushHistoryUnit(unit);
    return;
  }
  if (cmd.startsWith("SAVEPOINT ")) {
    const sp = op.forward.replace(/^SAVEPOINT\s+/i, "").trim() || "__savepoint__";
    flushTxnPendingOpsAsUnit(sp);
    txnRecorder.value.currentSavepoint = sp;
    return;
  }
  if (cmd.startsWith("COMMIT")) {
    flushTxnPendingOpsAsUnit("__commit__");
    txnRecorder.value.active = false;
    return;
  }
  if (cmd.startsWith("ROLLBACK")) {
    txnRecorder.value.pendingOps = [];
    txnRecorder.value.active = false;
    return;
  }
  // 事务内：可逆命令各自成栈项；仅无可逆载荷的命令累积，在 COMMIT/SAVEPOINT/下一条可逆前打成一批（连带）。
  if (undoPayloadPresent(op)) {
    flushTxnPendingOpsAsUnit("__txn_irrev__");
    if (!shouldRecordOpInUndoChain(op)) return;
    pushHistoryUnit(buildUndoUnit("__single__", [op]));
    return;
  }
  if (!shouldRecordOpInUndoChain(op)) return;
  txnRecorder.value.pendingOps.push(op);
}

async function runCommand(
  text: string,
  opts?: { skipHistory?: boolean; opType?: OperationType; title?: string; backward?: string }
) {
  if (!text.trim()) return;
  const opType = opts?.opType ?? "generic";
  const up = normalizeCmd(text).toUpperCase();
  const gridBackward =
    up.startsWith("UPDATE(")
      ? inferUpdateInverseFromCurrentView(text)
      : up.startsWith("DELETE(")
        ? inferDeleteInverseFromCurrentView(text)
        : undefined;
  // Prefer current grid row over backend snapshot: backend query_page/SHOW ATTR can disagree with on-screen cells (e.g. schema.table, sidecar timing).
  let backendInfer: { backward?: string; source?: string };
  if (gridBackward !== undefined) {
    backendInfer = {};
  } else {
    backendInfer = await inferBackwardPayloadByBackend(text, opType);
  }
  const backendBackward = backendInfer.backward;
  const presetBackward =
    opts?.backward ?? gridBackward ?? backendBackward ?? inferInverse(opType, text) ?? undefined;
  busy.value = true;
  try {
    const exec = await invoke<CommandExecResult>("execute_command_ex", { command: text });
    const result = exec.output ?? "";
    {
      const u = normalizeCmd(text).toUpperCase();
      if (u.includes("SHOW PLAN") || u.startsWith("EXPLAIN WHERE")) {
        lastShowPlanRaw.value = result;
        lastCommandErrorCodeNumeric.value =
          exec.errorCodeNumeric === undefined || exec.errorCodeNumeric === null ? null : exec.errorCodeNumeric;
      }
    }
    logLine(`> ${text}`);
    logLine(result);
    const failed = shouldStopAndSkipHistory(text, result, exec.errorCode ?? null);
    if (!failed) {
      applyStateSideEffects(text);
      if (up.startsWith("CONFIRM_REORDER(")) {
        page.value = 1;
      }
      if (!opts?.skipHistory) {
        const op: UndoOp = {
          type: opType,
          title: opts?.title ?? text,
          forward: text,
          backward: presetBackward,
          inverseSource:
            backendInfer.source ||
            (gridBackward !== undefined ? "frontend:grid_view" : presetBackward ? "frontend:heuristic" : undefined),
          table: state.value.currentTable || "",
          time: now(),
          ok: true
        };
        trackTxnCommand(op);
        await persistStackState();
      }
    } else {
      logLine("[CMD] insert/update stopped due to command error.");
      await openUiMessage("alert", "命令已中断", "INSERT/UPDATE 执行失败，本次不计入历史。");
    }
    await refreshTables();
    await refreshPage();
    return !failed;
  } catch (e: unknown) {
    const raw =
      typeof e === "string"
        ? e
        : e && typeof e === "object" && "message" in e
          ? String((e as { message?: string }).message ?? e)
          : String(e);
    logLine(`[CMD][ERROR] ${raw}`);
    await openUiMessage(
      "alert",
      "命令执行失败",
      raw.trim().slice(0, 1200) || "invoke 返回错误，详情见日志。"
    );
    return false;
  } finally {
    busy.value = false;
  }
}

async function runTimedOp(
  kind:
    | "txn_begin"
    | "txn_commit"
    | "txn_rollback"
    | "txn_savepoint"
    | "txn_rollback_to"
    | "txn_release_savepoint",
  payload: Record<string, any>,
  forwardCmd: string,
  title: string
) {
  busy.value = true;
  try {
    const map: Record<string, string> = {
      txn_begin: "txn_begin",
      txn_commit: "txn_commit",
      txn_rollback: "txn_rollback",
      txn_savepoint: "txn_savepoint",
      txn_rollback_to: "txn_rollback_to",
      txn_release_savepoint: "txn_release_savepoint"
    };
    const cmdName = map[kind];
    const exec = await invoke<TimedExecResult>(cmdName, payload);
    logLine(`> ${forwardCmd}`);
    logLine(exec.output ?? "");
    if (exec.ok) {
      const op: UndoOp = {
        type: kind as OperationType,
        title,
        forward: forwardCmd,
        table: state.value.currentTable || "",
        time: now(),
        ok: true
      };
      trackTxnCommand(op);
      await persistStackState();
    } else {
      await openUiMessage("alert", "操作失败", exec.output || exec.errorCode || "unknown error");
    }
    const st = await invoke<State>("get_state");
    state.value = st;
    await refreshTables();
    await refreshPage();
    return exec.ok;
  } finally {
    busy.value = false;
  }
}

async function refreshWalRecoverySummary() {
  // Minimal recovery summary: reuse CLI command output (keeps backend changes small).
  const ok = await runCommand("SHOW TUNING", { skipHistory: true, opType: "generic", title: "SHOW TUNING 摘要" });
  if (!ok) {
    walRecoveryText.value = "[SHOW TUNING] failed";
    return;
  }
  // Use the latest logs slice as the modal content.
  walRecoveryText.value = logs.value.slice(0, 60).join("\n");
}

async function exportBundle() {
  busy.value = true;
  try {
    const path = await invoke<string>("export_bundle", { destZip: null });
    exportBundlePath.value = path;
    logs.value.unshift(`[${now()}] export bundle\n${path}`);
    persistLogs();
    await openUiMessage("alert", "导出完成", `诊断包已生成：\n${path}`);
  } catch (e: any) {
    await openUiMessage("alert", "导出失败", String(e));
  } finally {
    busy.value = false;
  }
}

async function loadMdbDurability() {
  try {
    const lvl = await invoke<number | null>("get_mdb_durability");
    mdbDurabilityLevel.value = lvl;
  } catch {
    mdbDurabilityLevel.value = null;
  }
}

async function backupStorageBundle() {
  if (!state.value.dataDir?.trim()) {
    await openUiMessage("alert", "冷备失败", "请先设置工作区 data_dir。");
    return;
  }
  const dest = await openUiMessage(
    "prompt",
    "冷备 data_dir",
    "目标目录（structdb_backup_bundle；执行前会关闭当前 embed 会话）",
    "C:/backup/structdb_cold"
  );
  if (typeof dest !== "string" || !dest.trim()) return;
  const ok = await openUiMessage(
    "confirm",
    "确认冷备",
    `将把工作区复制/打包到：\n${dest.trim()}\n\n继续？`
  );
  if (!ok) return;
  busy.value = true;
  try {
    const path = await invoke<string>("backup_storage_bundle", { destDir: dest.trim() });
    coldBackupPath.value = path;
    logs.value.unshift(`[${now()}] backup_storage_bundle\n${path}`);
    persistLogs();
    await loadMdbDurability();
    await refreshTables();
    await openUiMessage("alert", "冷备完成", `已写入：\n${path}\n\nembed 已关闭，下一条 MDB 将自动重连。`);
  } catch (e: any) {
    await openUiMessage("alert", "冷备失败", String(e));
  } finally {
    busy.value = false;
  }
}

async function recoverToCheckpointSeq() {
  if (!state.value.dataDir?.trim()) {
    await openUiMessage("alert", "恢复失败", "请先设置工作区 data_dir。");
    return;
  }
  const raw = await openUiMessage(
    "prompt",
    "按 checkpoint 冷恢复",
    "输入 checkpoint_seq（> 0）。可先运行 SHOW CHECKPOINTS 查看序号。",
    recoverCheckpointSeqInput.value
  );
  if (typeof raw !== "string" || !raw.trim()) return;
  const seq = Number.parseInt(raw.trim(), 10);
  if (!Number.isFinite(seq) || seq <= 0) {
    await openUiMessage("alert", "参数错误", "checkpoint_seq 须为正整数。");
    return;
  }
  recoverCheckpointSeqInput.value = String(seq);
  const ok = await openUiMessage(
    "confirm",
    "确认冷恢复",
    `将把工作区\n${state.value.dataDir}\n恢复到 checkpoint_seq=${seq}（截断 WAL/链）。此操作不可撤销，继续？`
  );
  if (!ok) return;
  busy.value = true;
  try {
    const msg = await invoke<string>("recover_to_checkpoint_seq", { checkpointSeq: seq });
    logs.value.unshift(`[${now()}] recover_to_checkpoint_seq ${seq}\n${msg}`);
    persistLogs();
    await loadMdbDurability();
    await refreshTables();
    await refreshPage();
    await openUiMessage("alert", "恢复完成", msg);
  } catch (e: any) {
    await openUiMessage("alert", "恢复失败", String(e));
  } finally {
    busy.value = false;
  }
}

async function setDurabilityPrompt() {
  const raw = await openUiMessage(
    "prompt",
    "MDB 耐久档位",
    "0=最快（少 fsync）  1=默认  2=最强（关键写 fsync）",
    String(mdbDurabilityLevel.value ?? 1)
  );
  if (typeof raw !== "string" || !raw.trim()) return;
  const level = Number.parseInt(raw.trim(), 10);
  if (![0, 1, 2].includes(level)) {
    await openUiMessage("alert", "参数错误", "耐久档位须为 0、1 或 2。");
    return;
  }
  busy.value = true;
  try {
    await invoke("set_mdb_durability", { level });
    mdbDurabilityLevel.value = level;
    logs.value.unshift(`[${now()}] SET DURABILITY ${level}`);
    persistLogs();
    await openUiMessage("alert", "已设置", `MDB 耐久档位 = ${level}`);
  } catch (e: any) {
    await openUiMessage("alert", "设置失败", String(e));
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

async function cancelMdbScript() {
  try {
    await invoke("cancel_long_task");
  } catch (e: any) {
    logLine(`[MDB] cancel request failed: ${String(e)}`);
  }
}

function formatLongTaskProgressLabel(p: LongTaskProgress): string {
  const kind = p.kind ?? "unknown";
  if (kind === "mdbScript") {
    return `${p.unitsDone ?? 0}/${p.unitsTotal ?? "?"}`;
  }
  if (kind === "compactionMerge") {
    const phase = p.unitsTotal ? `${p.unitsDone ?? 0}/${p.unitsTotal}` : "";
    const bytes = p.bytesDone ? ` ${p.bytesDone} B` : "";
    const detail = p.detail?.trim();
    return `${detail ? detail + " " : "Compaction "}${phase}${bytes}`.trim();
  }
  return `${kind} ${p.status ?? ""}`.trim();
}

async function subscribeLongTaskProgress(onUpdate: (label: string) => void) {
  return listen<LongTaskProgress>("long-task-progress", (ev) => {
    onUpdate(formatLongTaskProgressLabel(ev.payload));
  });
}

async function runScriptText(script: string) {
  busy.value = true;
  scriptRunning.value = true;
  scriptProgressLabel.value = "";
  scriptProgressUnlisten = await subscribeLongTaskProgress((label) => {
    scriptProgressLabel.value = label;
  });
  try {
    const exec = await invoke<ScriptExecResult>("run_script_ex", { script });
    const result = exec.output ?? "";
    logLine("[MDB] script executed");
    logLine(result);
    if (exec.cancelled) {
      logLine("[MDB] script cancelled");
      await openUiMessage("alert", "脚本已停止", "已收到停止请求，后续行未执行。");
    }
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
    scriptProgressUnlisten?.();
    scriptProgressUnlisten = null;
    scriptRunning.value = false;
    scriptProgressLabel.value = "";
    busy.value = false;
  }
}

async function runScript() {
  busy.value = true;
  scriptRunning.value = true;
  scriptProgressLabel.value = "";
  scriptProgressUnlisten = await subscribeLongTaskProgress((label) => {
    scriptProgressLabel.value = label;
  });
  try {
    const exec = await invoke<ScriptExecResult>("run_script_ex", { script: scriptText.value });
    const result = exec.output ?? "";
    logLine("[MDB] script executed");
    logLine(result);
    if (exec.cancelled) {
      logLine("[MDB] script cancelled");
      await openUiMessage("alert", "脚本已停止", "已收到停止请求，后续行未执行。");
    }
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
    scriptProgressUnlisten?.();
    scriptProgressUnlisten = null;
    scriptRunning.value = false;
    scriptProgressLabel.value = "";
    busy.value = false;
  }
}

async function refreshAfterStackExec() {
  await refreshTables();
  await nextTick();
  await refreshPage();
}

function undoGlobalIndexFromPaged(idx: number): number {
  return undoStack.value.length - 1 - (stackUndoPage.value - 1) * stackPageSize - idx;
}

function redoGlobalIndexFromPaged(idx: number): number {
  return redoStack.value.length - 1 - (stackRedoPage.value - 1) * stackPageSize - idx;
}

async function undoToIndex(targetIndex: number) {
  if (busy.value) return;
  if (targetIndex < 0 || targetIndex >= undoStack.value.length) return;
  const steps = undoStack.value.length - targetIndex;
  const item = undoStack.value[targetIndex];
  if (!item) return;
  const okConfirm = await openUiMessage(
    "confirm",
    "确认撤销到此处",
    `将撤销 ${steps} 个单元（包含目标单元）\n目标：${item.savepointName}\nops=${item.ops.length}\ntables=${item.tablesTouched.join(", ") || "n/a"}`
  );
  if (!okConfirm) return;
  busy.value = true;
  try {
    while (undoStack.value.length - 1 >= targetIndex) {
      const top = undoStack.value[undoStack.value.length - 1];
      if (!top) break;
      const repairedOps: UndoOp[] = [];
      for (const op of top.ops) {
        let backward = op.backward;
        if (commandHasEmptyUpdateInsertArg(backward)) {
          const repaired = await inferBackwardPayloadByBackend(op.forward, op.type);
          if (repaired.backward && !commandHasEmptyUpdateInsertArg(repaired.backward)) {
            backward = repaired.backward;
          }
        }
        repairedOps.push({ ...op, backward });
      }
      const safeUnit = { ...top, ops: repairedOps };
      const r = await invoke<StackExecResult>("stack_undo_unit", { unit: safeUnit });
      recordStackExecTrace(top, "undo", r);
      logLine(`[UNDO][${r.repairAction}] ${top.savepointName} applied=${r.appliedOps} ok=${r.ok}`);
      if (r.ok) {
        undoStack.value.pop();
        redoStack.value.push({ ...top, status: "applied" });
      } else {
        top.status = r.repairAction === "hard_fail_rollback" ? "frozen" : "failed";
        top.lastError = r.failedOp || r.message;
        await openUiMessage("alert", "UNDO 失败", `${r.message}\nfailed=${r.failedOp ?? "n/a"}`);
        break;
      }
    }
    await persistStackState();
    await refreshAfterStackExec();
  } finally {
    busy.value = false;
  }
}

async function redoToIndex(targetIndex: number, editable: boolean) {
  if (busy.value) return;
  if (targetIndex < 0 || targetIndex >= redoStack.value.length) return;
  const steps = redoStack.value.length - targetIndex;
  const item = redoStack.value[targetIndex];
  if (!item) return;
  const okConfirm = await openUiMessage(
    "confirm",
    "确认重做到此处",
    `将重做 ${steps} 个单元（包含目标单元）\n目标：${item.savepointName}\nops=${item.ops.length}\ntables=${item.tablesTouched.join(", ") || "n/a"}\n\n任一步与当前数据冲突将中断；未完成条目仍留在重做栈。`
  );
  if (!okConfirm) return;
  busy.value = true;
  try {
    while (redoStack.value.length - 1 >= targetIndex) {
      const topIndex = redoStack.value.length - 1;
      const top = redoStack.value[topIndex];
      if (!top) break;
      // Editable only applies to the final target unit, to keep batch deterministic.
      const wantEdit = editable && topIndex === targetIndex;
      if (wantEdit) {
        // Reuse existing single-item path (supports edited ops + backward inference).
        await redoFromStack(topIndex, true);
        break;
      }
      const r = await invoke<StackExecResult>("stack_redo_unit", { unit: top });
      recordStackExecTrace(top, "redo", r);
      logLine(`[REDO][${r.repairAction}] ${top.savepointName} applied=${r.appliedOps} ok=${r.ok}`);
      if (r.ok) {
        redoStack.value.pop();
        undoStack.value.push({ ...top, status: "ready" });
      } else {
        top.status = "failed";
        top.lastError = r.failedOp || r.message;
        await openUiMessage(
          "alert",
          "REDO 失败",
          `${r.message}\nfailed=${r.failedOp ?? "n/a"}\n\n本条仍留在重做栈，可先撤销或修正数据后再试。`
        );
        break;
      }
    }
    await persistStackState();
    await refreshAfterStackExec();
  } finally {
    busy.value = false;
  }
}

async function undo() {
  const item = undoStack.value[undoStack.value.length - 1];
  if (!item) return;
  stackPreviewUnit.value = item;
  const okConfirm = await openUiMessage(
    "confirm",
    "确认撤销单元",
    `savepoint=${item.savepointName}\nops=${item.ops.length}\ntables=${item.tablesTouched.join(", ") || "n/a"}`
  );
  if (!okConfirm) return;
  busy.value = true;
  try {
    const repairedOps: UndoOp[] = [];
    for (const op of item.ops) {
      let backward = op.backward;
      if (commandHasEmptyUpdateInsertArg(backward)) {
        const repaired = await inferBackwardPayloadByBackend(op.forward, op.type);
        if (repaired.backward && !commandHasEmptyUpdateInsertArg(repaired.backward)) {
          backward = repaired.backward;
        }
      }
      repairedOps.push({ ...op, backward });
    }
    const safeUnit = { ...item, ops: repairedOps };
    const r = await invoke<StackExecResult>("stack_undo_unit", { unit: safeUnit });
    recordStackExecTrace(item, "undo", r);
    logLine(`[UNDO][${r.repairAction}] ${item.savepointName} applied=${r.appliedOps} ok=${r.ok}`);
    if (r.ok) {
      undoStack.value.pop();
      redoStack.value.push({ ...item, status: "applied" });
    } else {
      item.status = r.repairAction === "hard_fail_rollback" ? "frozen" : "failed";
      item.lastError = r.failedOp || r.message;
      await openUiMessage("alert", "UNDO 失败", `${r.message}\nfailed=${r.failedOp ?? "n/a"}`);
    }
    await persistStackState();
    await refreshAfterStackExec();
  } finally {
    busy.value = false;
  }
}

async function redo() {
  if (!canRedo.value) return;
  const last = redoStack.value[redoStack.value.length - 1];
  if (!last) return;
  stackPreviewUnit.value = last;
  const okConfirm = await openUiMessage(
    "confirm",
    "确认重做单元",
    `savepoint=${last.savepointName}\nops=${last.ops.length}\ntables=${last.tablesTouched.join(", ") || "n/a"}\n\n若与当前数据不一致可能失败，失败将中断并保持本条在重做栈。`
  );
  if (!okConfirm) return;
  busy.value = true;
  try {
    const r = await invoke<StackExecResult>("stack_redo_unit", { unit: last });
    recordStackExecTrace(last, "redo", r);
    logLine(`[REDO][${r.repairAction}] ${last.savepointName} applied=${r.appliedOps} ok=${r.ok}`);
    if (r.ok) {
      redoStack.value.pop();
      undoStack.value.push({ ...last, status: "ready" });
    } else {
      last.status = "failed";
      last.lastError = r.failedOp || r.message;
      await openUiMessage(
        "alert",
        "REDO 失败",
        `${r.message}\nfailed=${r.failedOp ?? "n/a"}\n\n本条仍留在重做栈。`
      );
    }
    await persistStackState();
    await refreshAfterStackExec();
  } finally {
    busy.value = false;
  }
}

async function redoFromStack(stackIndex: number, editable: boolean) {
  const item = redoStack.value[stackIndex];
  if (!item) return;
  if (busy.value) return;
  let command = item.ops.map((x) => x.forward).join("\n");
  let edited = false;
  if (editable) {
    const next = await openUiMessage("prompt", "编辑重做命令", "重做前可编辑命令", command);
    if (typeof next !== "string") return;
    if (!next) return;
    edited = next !== command;
    command = next;
  }
  logLine(`[REDO] ${item.savepointName}${editable ? " (edited)" : ""}`);
  let editedUnit: UndoUnit;
  if (edited) {
    const ops = command
      .split(/\r?\n/)
      .map((x) => x.trim())
      .filter((x) => !!x)
      .map((cmd, idx) => {
        const prev = item.ops[idx];
        const sameAsPrev = prev ? normalizeCmd(prev.forward) === normalizeCmd(cmd) : false;
        const opType = sameAsPrev && prev ? prev.type : inferOpTypeFromCommand(cmd);
        const backward =
          (sameAsPrev && prev?.backward ? prev.backward : undefined) ?? inferBackwardForCommand(cmd, opType);
        return {
          type: opType,
          title: cmd,
          forward: cmd,
          backward,
          table: prev?.table || state.value.currentTable || "",
          time: now()
        };
      });
    editedUnit = { ...item, ops, savepointName: item.savepointName + " (edited)" };
  } else {
    // Preserve original backward commands when no edit is applied.
    editedUnit = { ...item };
  }
  const r = await invoke<StackExecResult>("stack_redo_unit", { unit: editedUnit });
  recordStackExecTrace(editedUnit, "redo", r);
  if (r.ok) {
    redoStack.value.splice(stackIndex, 1);
    undoStack.value.push(editedUnit);
    await persistStackState();
    await refreshAfterStackExec();
  } else {
    await openUiMessage("alert", "重做失败", `${r.message}\nfailed=${r.failedOp ?? "n/a"}\n\n本条仍留在重做栈。`);
    await refreshAfterStackExec();
  }
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
  await loadMdbDurability();
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
  const label = "structdb-cli-terminal";
  const exists = await WebviewWindow.getByLabel(label);
  if (exists) {
    await exists.show();
    await exists.setFocus();
    return;
  }
  const w = new WebviewWindow(label, {
    title: "StructDB CLI 终端",
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
  const label = "structdb-log-window";
  const exists = await WebviewWindow.getByLabel(label);
  if (exists) {
    await exists.show();
    await exists.setFocus();
    return;
  }
  const w = new WebviewWindow(label, {
    title: "StructDB 日志窗口",
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
  if (action.kind === "applyPreset") {
    const preset = settingsPresets.find((p) => p.key === action.presetKey);
    if (preset) {
      applySettingsPreset(preset);
    }
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
  if (action.kind === "about") {
    await openAboutModal();
    return;
  }
  if (action.kind === "txnRecovery") {
    showTxnRecoveryModal.value = true;
    return;
  }
  if (action.kind === "walRecovery") {
    showWalRecoveryModal.value = true;
    await refreshWalRecoverySummary();
    return;
  }
  if (action.kind === "exportBundle") {
    showExportModal.value = true;
    return;
  }
  if (action.kind === "backupStorageBundle") {
    await backupStorageBundle();
    return;
  }
  if (action.kind === "recoverCheckpoint") {
    await recoverToCheckpointSeq();
    return;
  }
  if (action.kind === "setDurability") {
    await setDurabilityPrompt();
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

function onTreeNodeContextMenu(ev: MouseEvent | Event, data: UiTreeNode) {
  if (data.type !== "table" || !data.fullName) return;
  onTableContextMenu(ev as MouseEvent, data.fullName);
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
  appendSessionLogHeader();
  loadLogCollapsed();
  if (viewMode.value === "cli") {
    await mountCliTerminal();
    return;
  }
  if (viewMode.value === "log") {
    setInterval(loadLogs, 1000);
    nextTick(() => {
      scrollLogsToBottom();
    });
    return;
  }
  state.value = await invoke<State>("get_state");
  checkWorkspacePathOrWarn(state.value.dataDir);
  try {
    const rt = await invoke<RuntimeArtifactInfo>("runtime_artifact_info");
    runtimeArtifacts.value = rt;
    logLine(`[RUNTIME] capi_dll=${rt.dllPath}`);
    logLine(`[RUNTIME] structdb_app=${rt.demoPath}`);
    if (rt.runtimeStatsSchemaVersion) {
      logLine(`[RUNTIME] runtime_stats_schema=${rt.runtimeStatsSchemaVersion}`);
    }
  } catch (e) {
    logLine(`[RUNTIME][ERROR] ${String(e)}`);
  }
  await loadStackState();
  await refreshTables();
  if (state.value.currentTable?.trim()) {
    const tab = ensureTableTab(state.value.currentTable.trim());
    activeTableKey.value = tab.key;
  }
  await refreshPage();
  await loadMdbDurability();
  window.addEventListener("click", () => {
    hideContextMenu();
  });
  window.addEventListener("keydown", handleGlobalHotkeys);
  nextTick(() => {
    scrollLogsToBottom();
  });
});

onUnmounted(() => {
  if (viewMode.value === "cli") {
    void teardownCliTerminal();
  }
  stopSidebarResize();
  window.removeEventListener("keydown", handleGlobalHotkeys);
});
</script>

<template>
  <div v-if="viewMode === 'log'" class="log-only">
    <header class="log-only-header">
      <h2 class="log-only-title">独立日志窗口</h2>
      <div class="log-tools">
        <el-select v-model="logFilterKind" size="small" style="width: 120px">
          <el-option label="全部" value="all" />
          <el-option label="命令" value="cmd" />
          <el-option label="错误" value="error" />
          <el-option label="成功" value="success" />
          <el-option label="状态" value="meta" />
        </el-select>
        <el-input v-model="logKeyword" size="small" clearable placeholder="筛选关键词" />
        <el-button size="small" :type="logAutoFollow ? 'primary' : undefined" @click="toggleLogAutoFollow">
          {{ logAutoFollow ? "跟随中" : "已暂停" }}
        </el-button>
      </div>
    </header>
    <div ref="logScrollPaneEl" class="logs command-output-box" @scroll="onLogScroll">
      <div
        v-for="(ln, idx) in visibleLogs"
        :key="`logonly-${idx}`"
        class="log-line"
        :class="`log-${ln.kind}`"
        v-html="highlightLogKeyword(ln.text)"
      >
      </div>
    </div>
  </div>

  <div v-else-if="viewMode === 'cli'" class="cli-only">
    <header class="cli-only-header">
      <h2 class="cli-only-title">StructDB 交互式 CLI（structdb_app，独立进程）</h2>
    </header>
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
    :class="{ dense: settings.denseMode, 'no-anim': !settings.animations, 'sidebar-collapsed': !showSidebar }"
    :style="layoutStyle"
  >
    <img
      v-if="settings.bgMode === 'image' && settings.bgImageUrl.trim()"
      class="layout-image-wallpaper"
      :src="settings.bgImageUrl"
      alt="wallpaper"
      draggable="false"
      decoding="async"
      fetchpriority="high"
    />
    <video
      v-if="settings.bgMode === 'video' && settings.bgVideoUrl.trim()"
      class="layout-video-wallpaper"
      :src="settings.bgVideoUrl"
      autoplay
      muted
      loop
      playsinline
    />
    <aside class="sidebar">
      <div class="sidebar-brand">
        <span class="sidebar-brand-mark">StructDB</span>
        <span class="sidebar-brand-sub">控制台</span>
      </div>
      <div class="sidebar-section">
        <div class="sidebar-header">
          <div class="sidebar-title">架构 / 表</div>
          <div class="sidebar-actions">
            <el-button size="small" plain @click="showSidebar = false">收起</el-button>
            <el-button size="small" plain @click="refreshTables">刷新</el-button>
          </div>
        </div>
        <div class="sidebar-subtitle">
          <span class="mono">workspace:</span>
          <span class="sidebar-subtitle-path">{{ state.dataDir || "(未设置)" }}</span>
        </div>
        <div class="v-spacer-8" />
        <el-input
          v-model="tableSearch"
          size="small"
          clearable
          placeholder="搜索 schema / table，例如 users 或 hr."
        />
      </div>
      <el-tree
        :data="treeData"
        node-key="key"
        default-expand-all
        :expand-on-click-node="false"
        class="table-tree"
        @node-click="onTreeNodeClick"
        @node-contextmenu="onTreeNodeContextMenu"
      >
        <template #default="{ data }">
          <div
            class="table-tree-node"
            :class="{
              active:
                data.type === 'table' &&
                (state.currentTable === data.fullName || state.currentTable === data.label)
            }"
            @contextmenu.prevent.stop="data.type === 'table' && data.fullName && onTableContextMenu($event, data.fullName)"
          >
            <span class="tree-icon" :class="{ schema: data.type === 'schema', table: data.type === 'table' }">
              {{ data.type === "schema" ? "🗂" : "▦" }}
            </span>
            <span class="tree-label">{{ data.label }}</span>
          </div>
        </template>
      </el-tree>
      <div class="sidebar-section sidebar-quicktools">
        <div class="fold-header" @click="showRowAttrTools = !showRowAttrTools">
          <strong>行 / 属性快捷操作</strong>
          <span>{{ showRowAttrTools ? "▾" : "▸" }}</span>
        </div>
        <div v-if="showRowAttrTools" class="fold-body">
          <div class="tool-grid">
            <div class="tool-card">
              <div class="tool-title">新增行</div>
              <el-form label-width="0">
                <el-form-item>
                  <div class="tool-row sidebar-tool-row">
                    <el-button type="primary" :disabled="busy" @click="openQuickAddRowModal">新增（弹窗）</el-button>
                  </div>
                </el-form-item>
              </el-form>
            </div>
            <div class="tool-card">
              <div class="tool-title">属性管理</div>
              <el-form label-width="0">
                <el-form-item>
                  <div class="tool-row sidebar-tool-row">
                    <el-input
                      v-model="addAttrName"
                      placeholder="属性名，如 salary"
                      @keydown="handleQuickToolEnter($event, 'addAttr')"
                    />
                    <el-select v-model="addAttrType">
                      <el-option v-for="opt in MDB_DEFATTR_TYPE_OPTIONS" :key="opt" :value="opt" :label="opt" />
                    </el-select>
                    <el-button type="primary" :disabled="busy" @click="addAttribute">新增属性</el-button>
                  </div>
                </el-form-item>
                <el-form-item>
                  <div class="tool-row sidebar-tool-row">
                    <el-select
                      v-model="delAttrName"
                      placeholder="选择要删除的属性"
                      @keydown="handleQuickToolEnter($event, 'delAttr')"
                    >
                      <el-option
                        v-for="h in sortableAttrs.filter((x) => x !== 'id')"
                        :key="`attr-sidebar-${h}`"
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
      <div class="stack-panel">
        <div class="stack-header">
          <div class="stack-title">事务栈</div>
          <div class="stack-header-right">
            <div class="stack-tab-switch" role="tablist" aria-label="撤销与重做列表切换">
              <button
                type="button"
                class="stack-tab"
                role="tab"
                :aria-selected="stackPanelTab === 'undo'"
                :class="{ 'stack-tab--active': stackPanelTab === 'undo' }"
                @click="stackPanelTab = 'undo'"
              >
                撤销 <span class="stack-tab-count">{{ undoStack.length }}</span>
              </button>
              <button
                type="button"
                class="stack-tab"
                role="tab"
                :aria-selected="stackPanelTab === 'redo'"
                :class="{ 'stack-tab--active': stackPanelTab === 'redo' }"
                @click="stackPanelTab = 'redo'"
              >
                重做 <span class="stack-tab-count">{{ redoStack.length }}</span>
              </button>
            </div>
          </div>
        </div>
        <div class="stack-panel-interactive">
        <div class="stack-actions">
          <button class="secondary" :disabled="!canUndo || busy" @click="undo">撤销</button>
          <button class="secondary" :disabled="!canRedo || busy" @click="redo">重做</button>
        </div>
        <div class="stack-list stack-list--shared">
          <Transition name="stack-fade" mode="out-in">
            <div v-if="stackPanelTab === 'undo'" key="stack-pane-undo" class="stack-list-pane">
              <template v-if="undoStack.length === 0">
                <div class="stack-empty">暂无撤销记录</div>
              </template>
              <template v-else>
                <div
                  v-for="(item, idx) in undoStackPaged"
                  :key="`u2-${item.unitId}-${idx}`"
                  class="stack-item clickable"
                  :class="{ selected: selectedStackKey === stackKey(item) }"
                  @click="selectStackItem(item)"
                >
                  <span class="stack-badge">U</span>
                  <span class="stack-item-text">
                    <span class="mono">{{ item.createdAt }}</span>
                    <span class="stack-item-title">
                      {{ stackSavepointDisplay(item.savepointName) }} (ops={{ item.ops.length }})
                      <el-tag
                        v-if="!isReversible(item)"
                        size="small"
                        type="warning"
                        effect="dark"
                        style="margin-left: 6px;"
                      >
                        不可逆
                      </el-tag>
                      <el-tag
                        v-if="item.tablesTouched.length > 1"
                        size="small"
                        type="info"
                        effect="dark"
                        style="margin-left: 6px;"
                      >
                        跨表
                      </el-tag>
                    </span>
                  </span>
                  <div class="stack-row-actions">
                    <button class="secondary mini" :disabled="busy" @click.stop="viewOperationLog(item)">回看</button>
                    <button
                      class="secondary mini"
                      :disabled="busy"
                      @click.stop="undoToIndex(undoGlobalIndexFromPaged(idx))"
                    >
                      撤销到此
                    </button>
                  </div>
                </div>
              </template>
            </div>
            <div v-else key="stack-pane-redo" class="stack-list-pane">
              <template v-if="redoStack.length === 0">
                <div class="stack-empty">暂无重做记录</div>
              </template>
              <template v-else>
                <div
                  v-for="(item, idx) in redoStackPaged"
                  :key="`r-${item.unitId}-${idx}`"
                  class="stack-item redo clickable"
                  :class="{ selected: selectedStackKey === stackKey(item) }"
                  @click="selectStackItem(item)"
                >
                  <span class="stack-badge">R</span>
                  <span class="stack-item-text">
                    <span class="mono">{{ item.createdAt }}</span>
                    <span class="stack-item-title">{{ stackSavepointDisplay(item.savepointName) }} (ops={{ item.ops.length }})</span>
                  </span>
                  <div class="stack-row-actions">
                    <button class="secondary mini" :disabled="busy" @click.stop="viewOperationLog(item)">回看</button>
                    <button class="secondary mini" :disabled="busy" @click.stop="redoToIndex(redoGlobalIndexFromPaged(idx), false)">重做到此</button>
                    <button class="secondary mini" :disabled="busy" @click.stop="redoFromStack(redoGlobalIndexFromPaged(idx), false)">重做</button>
                    <button class="secondary mini" :disabled="busy" @click.stop="redoFromStack(redoGlobalIndexFromPaged(idx), true)">编辑</button>
                  </div>
                </div>
              </template>
            </div>
          </Transition>
        </div>
        <div class="stack-actions stack-actions--pager" style="margin-top:8px;">
          <template v-if="stackPanelTab === 'undo'">
            <button class="secondary mini" :disabled="stackUndoPage <= 1" @click="stackUndoPage -= 1">上一页</button>
            <button class="secondary mini" :disabled="undoStack.length <= stackUndoPage * stackPageSize" @click="stackUndoPage += 1">下一页</button>
          </template>
          <template v-else>
            <button class="secondary mini" :disabled="stackRedoPage <= 1" @click="stackRedoPage -= 1">上一页</button>
            <button class="secondary mini" :disabled="redoStack.length <= stackRedoPage * stackPageSize" @click="stackRedoPage += 1">下一页</button>
          </template>
        </div>
        <div v-if="stackPreviewUnit" class="stack-empty" style="margin-top:8px;">
          预览：{{ stackSavepointDisplay(stackPreviewUnit.savepointName) }} | tables={{ stackPreviewUnit.tablesTouched.join(',') || 'n/a' }} | status={{ stackPreviewUnit.status }}
        </div>
        </div>
      </div>
    </aside>
    <div
      v-if="showSidebar"
      class="sidebar-resizer"
      :class="{ active: sidebarResizing }"
      title="拖拽调整侧栏宽度"
      @mousedown.prevent="startSidebarResize"
    />

    <section class="content" :class="{ 'console-collapsed': logCollapsed }">
      <div class="menu-bar">
        <div class="menu-bar-brand" aria-hidden="true">
          <span class="menu-bar-brand-mark">StructDB</span>
        </div>
        <div class="menu-bar-menus">
        <el-dropdown v-for="menu in topMenus" :key="menu.key" trigger="click">
          <el-button class="menu-title" text>{{ menu.label }}</el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <template v-for="(item, menuIdx) in menu.items" :key="`${menu.key}-${menuIdx}`">
                <el-dropdown-item
                  v-if="item.children?.length"
                  class="menu-dropdown-submenu-host"
                  :divided="!!item.divider"
                  @click.stop
                >
                  <el-dropdown trigger="click" placement="right-start" :teleported="true">
                    <span class="menu-dropdown-submenu-title">
                      {{ item.label }}
                      <span class="menu-dropdown-submenu-chevron" aria-hidden="true">›</span>
                    </span>
                    <template #dropdown>
                      <el-dropdown-menu>
                        <el-dropdown-item
                          v-for="(sub, subIdx) in item.children"
                          :key="`${menu.key}-${menuIdx}-sub-${subIdx}`"
                          @click="sub.action && runMenuAction(sub.action)"
                        >
                          {{ sub.label }}
                        </el-dropdown-item>
                      </el-dropdown-menu>
                    </template>
                  </el-dropdown>
                </el-dropdown-item>
                <el-dropdown-item
                  v-else
                  :divided="!!item.divider"
                  :disabled="!!item.section"
                  :class="{ 'menu-dropdown-section': item.section }"
                  @click="!item.section && item.action && runMenuAction(item.action)"
                >
                  <span v-if="!item.divider">{{ item.label }}</span>
                </el-dropdown-item>
              </template>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        </div>
      </div>

      <div class="toolbar">
        <div class="toolbar-group">
          <span class="toolbar-group-title">高频</span>
          <el-button plain @click="showSidebar = !showSidebar">{{ showSidebar ? "隐藏侧栏" : "显示侧栏" }}</el-button>
          <el-button type="primary" :icon="FolderOpened" @click="setWorkspace">数据目录</el-button>
          <el-button type="primary" :icon="Plus" @click="openCreateTableWizard">创建表</el-button>
          <el-button :icon="Setting" @click="showSettingsModal = true">设置</el-button>
        </div>
        <div class="toolbar-group toolbar-group-low">
          <span class="toolbar-group-title">低频</span>
          <el-button plain :icon="InfoFilled" @click="openAboutModal">关于</el-button>
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
          <div class="open-table-tabs-stack">
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
                  @click="selectOpenTableTab(t.key)"
                >
                  {{ t.table }}
                  <span class="tab-pill-close" title="关闭标签" @click.stop="closeTableTab(t.key)">×</span>
                </el-button>
              </div>
              <div class="open-table-tabs-right" v-else>
                <span class="mini-tip">从左侧选择表</span>
              </div>
            </div>
            <div class="app-status-bar" role="status" aria-live="polite">
              <span class="app-status-bar-label">USE</span>
              <span class="app-status-bar-value mono">{{ state.currentTable?.trim() || "（未选择）" }}</span>
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
          <div class="mdb-editor-panel">
            <textarea v-model="scriptText" class="mdb-textarea" placeholder="在此输入 MDB 脚本…" />
            <div class="v-spacer-8" />
            <div class="mdb-script-actions" style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
              <el-button type="primary" :icon="VideoPlay" @click="runScript" :disabled="busy">执行脚本</el-button>
              <el-button type="danger" plain @click="cancelMdbScript" :disabled="!scriptRunning">停止脚本</el-button>
              <span v-if="scriptProgressLabel" class="mdb-script-progress">{{ scriptProgressLabel }}</span>
            </div>
          </div>
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
        <el-tooltip
          content="按当前排序键分页读取全表，将行 id 重编号为 1…n（CONFIRM_REORDER）；事务中不可用"
          placement="top"
        >
          <el-button
            type="primary"
            :icon="RefreshRight"
            :disabled="busy || !String(state.currentTable ?? '').trim()"
            @click="quickReorderWholeTableIds"
          >
            快捷重排 id
          </el-button>
        </el-tooltip>
        <span class="status">当前页行数: {{ totalRowsHint }}</span>
      </div>

      <div class="console">
        <div class="console-header">
          <div class="console-header-left">
            <span>命令与日志</span>
            <el-tag size="small" effect="plain" :type="logAutoFollow ? 'success' : 'warning'">
              {{ logAutoFollow ? "自动跟随" : "已暂停跟随" }}
            </el-tag>
            <span class="console-shortcuts">
              <span class="kbd-chip"><kbd>Enter</kbd> 执行</span>
              <span class="kbd-chip"><kbd>Ctrl</kbd>+<kbd>L</kbd> 清空</span>
              <span class="kbd-chip"><kbd>Ctrl</kbd>+<kbd>K</kbd> 聚焦输入</span>
              <span class="kbd-chip"><kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>R</kbd> 重排</span>
            </span>
          </div>
          <el-button text @click="toggleLogCollapsed">{{ logCollapsed ? "展开" : "折叠" }}</el-button>
        </div>
        <template v-if="!logCollapsed">
          <div class="console-input">
          <input
            ref="commandInputEl"
            v-model="command"
            placeholder="输入命令，如 INSERT(1,1,alice,eng,29)"
            style="flex: 1"
            @keyup.enter="runCommand(command); command=''"
          />
          <el-button type="primary" :icon="VideoPlay" @click="runCommand(command); command=''" :disabled="busy">执行命令</el-button>
          </div>
          <div class="log-tools">
            <el-select v-model="logFilterKind" size="small" style="width: 120px">
              <el-option label="全部" value="all" />
              <el-option label="命令" value="cmd" />
              <el-option label="错误" value="error" />
              <el-option label="成功" value="success" />
              <el-option label="状态" value="meta" />
            </el-select>
            <el-input v-model="logKeyword" size="small" clearable placeholder="筛选关键词" />
            <el-button size="small" :type="logAutoFollow ? 'primary' : undefined" @click="toggleLogAutoFollow">
              {{ logAutoFollow ? "跟随中" : "已暂停" }}
            </el-button>
            <span class="status">显示 {{ visibleLogs.length }} / {{ renderedLogs.length }}</span>
          </div>
          <div ref="logScrollPaneEl" class="logs command-output-box" @scroll="onLogScroll">
            <div
              v-for="(ln, idx) in visibleLogs"
              :key="`mainlog-${idx}`"
              class="log-line"
              :class="`log-${ln.kind}`"
              v-html="highlightLogKeyword(ln.text)"
            >
            </div>
          </div>
        </template>
      </div>
    </section>

    <div
      v-if="contextMenu.visible"
      class="context-menu"
      :style="{ left: `${contextMenu.x}px`, top: `${contextMenu.y}px` }"
      @click.stop
    >
      <button @click="runContext('USE({table})')">
        <el-icon><Select /></el-icon>
        <span>使用表</span>
      </button>
      <div class="submenu-group">
        <span class="submenu-title">
          <el-icon><UploadFilled /></el-icon>
          <span>导出 JSON</span>
        </span>
        <div class="submenu-panel">
          <button @click="runContext('EXPORT JSON {table}.json')">
            <el-icon><DocumentCopy /></el-icon>
            <span>EXPORT JSON …</span>
          </button>
          <button @click="runContext('EXPORT {table}.json')">
            <el-icon><DocumentCopy /></el-icon>
            <span>EXPORT 裸路径 …</span>
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

    <el-dialog
      v-model="showHelp"
      class="help-modal help-center-modal"
      width="800px"
      align-center
      destroy-on-close
    >
      <template #header>
        <div class="help-shell-header">
          <div class="help-shell-brand" aria-hidden="true">
            <el-icon :size="22"><Reading /></el-icon>
          </div>
          <div class="help-shell-header-text">
            <div class="help-shell-kicker">StructDB · MDB</div>
            <h3 class="help-shell-title">帮助中心</h3>
            <p class="help-shell-lead">命令语法、重载、示例与说明 — 支持全文筛选与批量展开</p>
          </div>
          <div class="help-shell-stats" role="status">
            <el-tag class="help-stat-tag" size="small" effect="plain" round>总计 {{ helpTotalCount }}</el-tag>
            <el-tag class="help-stat-tag help-stat-tag--hit" size="small" type="success" effect="plain" round>
              命中 {{ helpMatchedCount }}
            </el-tag>
          </div>
        </div>
      </template>

      <div class="help-shell-body">
        <div class="help-toolbar-card">
          <el-input
            v-model="helpKeyword"
            class="help-search-input"
            :prefix-icon="Search"
            clearable
            placeholder="搜索命令、分类、语法、示例、说明…"
          />
          <div class="help-toolbar-actions">
            <el-button size="small" text type="primary" @click="helpKeyword = ''">清空</el-button>
            <el-divider direction="vertical" class="help-toolbar-divider" />
            <el-button size="small" @click="expandAllHelp">全部展开</el-button>
            <el-button size="small" @click="collapseAllHelp">全部折叠</el-button>
          </div>
        </div>

        <div class="help-list" role="list">
          <div v-if="filteredHelp.length === 0" class="help-empty">
            <span class="help-empty-icon" aria-hidden="true">⌕</span>
            <div>
              <div class="help-empty-title">未找到匹配项</div>
              <p class="help-empty-hint">试试 <span class="mono">PAGE</span>、<span class="mono">EXPORT</span>、<span class="mono">SHOW TUNING</span>、<span class="mono">DEFATTR</span> 等关键词</p>
            </div>
          </div>
          <div
            v-for="(h, idx) in filteredHelp"
            :key="`${h.command}-${idx}`"
            class="help-item"
            :class="{ 'help-item--open': helpExpanded[idx] }"
            role="listitem"
          >
            <div class="help-header" @click="toggleHelpCard(idx)">
              <span class="help-category" v-html="highlight(h.category)"></span>
              <div class="help-header-main">
                <strong class="help-command mono" v-html="highlight(h.command)"></strong>
              </div>
              <span class="help-chevron" :class="{ 'is-open': helpExpanded[idx] }" aria-hidden="true">
                <el-icon><ArrowRight /></el-icon>
              </span>
            </div>
            <Transition name="help-fold">
              <div v-show="helpExpanded[idx]" class="help-body">
                <div class="help-kv-row">
                  <span class="help-kv-key">语法</span>
                  <div class="help-kv-val help-kv-val--code mono" v-html="highlight(h.syntax)"></div>
                </div>
                <div v-if="h.overloads?.length" class="help-kv-row">
                  <span class="help-kv-key">重载</span>
                  <div class="help-kv-val help-kv-val--code mono" v-html="highlight(h.overloads.join(' | '))"></div>
                </div>
                <div class="help-kv-row">
                  <span class="help-kv-key">示例</span>
                  <div class="help-kv-val help-kv-val--code mono" v-html="highlight(h.example)"></div>
                </div>
                <div class="help-kv-row">
                  <span class="help-kv-key">说明</span>
                  <div class="help-kv-val" v-html="highlight(h.desc)"></div>
                </div>
                <div class="help-kv-row help-kv-row--detail">
                  <span class="help-kv-key">详细</span>
                  <div class="help-kv-val help-kv-val--detail" v-html="highlight(h.detail)"></div>
                </div>
              </div>
            </Transition>
          </div>
        </div>
      </div>

      <template #footer>
        <div class="help-footer">
          <span class="help-footer-hint">提示：点击卡片标题可展开 / 折叠</span>
          <el-button type="primary" plain @click="showHelp = false">关闭</el-button>
        </div>
      </template>
    </el-dialog>

    <el-dialog
      v-model="showAboutModal"
      class="help-modal help-center-modal help-center-modal--compact"
      width="520px"
      align-center
      destroy-on-close
    >
      <template #header>
        <div class="help-shell-header">
          <div class="help-shell-brand" aria-hidden="true">
            <el-icon :size="22"><InfoFilled /></el-icon>
          </div>
          <div class="help-shell-header-text">
            <div class="help-shell-kicker">StructDB · Desktop</div>
            <h3 class="help-shell-title">关于本客户端</h3>
            <p class="help-shell-lead">
              进程内加载 <span class="mono">structdb_capi_shared</span> 执行 MDB；可选拉起 <span class="mono">structdb_app</span> CLI。
            </p>
          </div>
        </div>
      </template>

      <div class="help-shell-body about-shell-body">
        <div class="about-kv-card">
          <div class="about-kv-row">
            <span class="about-kv-key">引擎 DLL</span>
            <span class="about-kv-val">{{ dll.loaded ? "已加载" : "未加载" }}</span>
          </div>
          <div class="about-kv-row">
            <span class="about-kv-key">版本字符串</span>
            <span class="about-kv-val mono">{{ dll.version || "—" }}</span>
          </div>
          <div v-if="dll.loaded && dll.apiVersionMajor != null && dll.apiVersionMajor >= 0" class="about-kv-row">
            <span class="about-kv-key">C API</span>
            <span class="about-kv-val mono">
              {{ dll.apiVersionMajor }}.{{ dll.apiVersionMinor }}.{{ dll.apiVersionPatch }}（ABI {{ dll.abiVersion }}）
            </span>
          </div>
          <div class="about-kv-row about-kv-row--path">
            <span class="about-kv-key">DLL 路径</span>
            <span class="about-kv-val mono about-kv-val--wrap">{{ dll.path || "—" }}</span>
          </div>
          <div v-if="!dll.loaded" class="about-dll-message">
            <strong>说明</strong>
            <span>{{ dll.message }}</span>
          </div>
        </div>

        <div v-if="runtimeArtifacts" class="about-kv-card about-kv-card--muted">
          <div class="about-kv-row about-kv-row--path">
            <span class="about-kv-key">structdb_app</span>
            <span class="about-kv-val mono about-kv-val--wrap">{{ runtimeArtifacts.demoPath }}</span>
          </div>
        </div>
      </div>

      <template #footer>
        <div class="help-footer">
          <span class="help-footer-hint">版本信息来自当前已加载的 C API 动态库</span>
          <div class="about-footer-actions">
            <el-button @click="openAboutModal">刷新</el-button>
            <el-button type="primary" plain @click="showAboutModal = false">关闭</el-button>
          </div>
        </div>
      </template>
    </el-dialog>

    <el-dialog
      v-model="showTxnRecoveryModal"
      class="settings-modal txn-panel-modal"
      width="640px"
      align-center
      destroy-on-close
    >
      <template #header>
        <div class="txn-panel-header">
          <div class="txn-panel-header-brand" aria-hidden="true">
            <span class="txn-panel-glyph">◇</span>
          </div>
          <div class="txn-panel-header-text">
            <div class="txn-panel-kicker">StructDB · MDB</div>
            <h3 class="txn-panel-heading">事务与 Savepoint</h3>
            <p class="txn-panel-lead">
              会话级 <span class="mono">BEGIN</span> / <span class="mono">COMMIT</span> / <span class="mono">ROLLBACK</span>
              · 命名保存点与释放
            </p>
          </div>
        </div>
      </template>

      <div class="txn-panel-body">
        <div class="txn-context-strip" role="status">
          <div class="txn-context-strip-inner">
            <span class="txn-context-label">当前 USE 表</span>
            <code class="txn-context-table mono">{{ state.currentTable || "—" }}</code>
          </div>
          <span class="txn-context-hint">由左侧表树或 <span class="mono">USE</span> 决定；<span class="mono">BEGIN</span> 无表名后缀</span>
        </div>

        <section class="txn-section txn-section--accent">
          <div class="txn-section-head">
            <span class="txn-section-icon-wrap" aria-hidden="true">
              <el-icon><VideoPlay /></el-icon>
            </span>
            <div>
              <h4 class="txn-section-title">会话事务</h4>
              <p class="txn-section-desc">控制当前嵌入会话的事务边界。</p>
            </div>
          </div>
          <div class="txn-btn-grid txn-btn-grid--3">
            <el-button
              class="txn-action-btn"
              :disabled="busy"
              @click="runTimedOp('txn_begin', {}, 'BEGIN', '开始事务')"
            >
              <span class="txn-action-btn-inner">
                <el-icon><VideoPlay /></el-icon>
                <span class="txn-action-label">BEGIN</span>
                <span class="txn-action-sub">开始</span>
              </span>
            </el-button>
            <el-button
              class="txn-action-btn"
              type="primary"
              :disabled="busy"
              @click="runTimedOp('txn_commit', {}, 'COMMIT', '提交事务')"
            >
              <span class="txn-action-btn-inner">
                <el-icon><CircleCheck /></el-icon>
                <span class="txn-action-label">COMMIT</span>
                <span class="txn-action-sub">提交</span>
              </span>
            </el-button>
            <el-button
              class="txn-action-btn"
              type="danger"
              plain
              :disabled="busy"
              @click="runTimedOp('txn_rollback', {}, 'ROLLBACK', '回滚事务')"
            >
              <span class="txn-action-btn-inner">
                <el-icon><RefreshLeft /></el-icon>
                <span class="txn-action-label">ROLLBACK</span>
                <span class="txn-action-sub">回滚</span>
              </span>
            </el-button>
          </div>
        </section>

        <section class="txn-section">
          <div class="txn-section-head">
            <span class="txn-section-icon-wrap txn-section-icon-wrap--muted" aria-hidden="true">
              <el-icon><Flag /></el-icon>
            </span>
            <div>
              <h4 class="txn-section-title">Savepoint</h4>
              <p class="txn-section-desc">
                命令须显式包含 <span class="mono">SAVEPOINT</span> 关键字，例如
                <span class="mono">ROLLBACK TO SAVEPOINT sp1</span>。
              </p>
            </div>
          </div>
          <div class="txn-sp-input-row">
            <span class="txn-sp-label">名称</span>
            <el-input
              v-model="txnRecoverySavepointName"
              class="txn-sp-input"
              placeholder="例如 sp1"
              clearable
              @keyup.enter="
                runTimedOp(
                  'txn_savepoint',
                  { name: txnRecoverySavepointName },
                  `SAVEPOINT ${txnRecoverySavepointName}`,
                  'SAVEPOINT'
                )
              "
            />
          </div>
          <div class="txn-btn-grid txn-btn-grid--3 txn-sp-actions">
            <el-button
              :disabled="busy"
              @click="
                runTimedOp(
                  'txn_savepoint',
                  { name: txnRecoverySavepointName },
                  `SAVEPOINT ${txnRecoverySavepointName}`,
                  'SAVEPOINT'
                )
              "
            >
              SAVEPOINT
            </el-button>
            <el-button
              :disabled="busy"
              @click="
                runTimedOp(
                  'txn_rollback_to',
                  { name: txnRecoverySavepointName },
                  `ROLLBACK TO SAVEPOINT ${txnRecoverySavepointName}`,
                  'ROLLBACK TO SAVEPOINT'
                )
              "
            >
              ROLLBACK TO
            </el-button>
            <el-button
              :disabled="busy"
              @click="
                runTimedOp(
                  'txn_release_savepoint',
                  { name: txnRecoverySavepointName },
                  `RELEASE SAVEPOINT ${txnRecoverySavepointName}`,
                  'RELEASE SAVEPOINT'
                )
              "
            >
              RELEASE
            </el-button>
          </div>
        </section>

        <section class="txn-section">
          <div class="txn-section-head">
            <span class="txn-section-icon-wrap txn-section-icon-wrap--muted" aria-hidden="true">
              <el-icon><Flag /></el-icon>
            </span>
            <div>
              <h4 class="txn-section-title">Checkpoint 链（PHASE43）</h4>
              <p class="txn-section-desc">
                列出链上检查点，或按序号冷恢复整个 <span class="mono">data_dir</span>（会关闭 embed）。
              </p>
            </div>
          </div>
          <div class="txn-btn-grid txn-btn-grid--2">
            <el-button :disabled="busy" @click="runCommand('SHOW CHECKPOINTS', { opType: 'generic', title: 'SHOW CHECKPOINTS' })">
              SHOW CHECKPOINTS
            </el-button>
            <el-button type="warning" plain :disabled="busy" @click="recoverToCheckpointSeq()">
              按 checkpoint 冷恢复…
            </el-button>
          </div>
          <p class="txn-context-hint" style="margin-top: 8px">
            当前 MDB 耐久档位：<span class="mono">{{ mdbDurabilityLevel ?? "未设置" }}</span>
            · <el-button link type="primary" :disabled="busy" @click="setDurabilityPrompt()">更改…</el-button>
          </p>
        </section>

        <div class="txn-callout txn-callout--note">
          <el-icon class="txn-callout-icon"><WarningFilled /></el-icon>
          <div>
            <div class="txn-callout-title">关于检查点恢复（PHASE43）</div>
            <p class="txn-callout-body">
              不支持 <span class="mono">RECOVER TO LSN/TIME</span>。可按 checkpoint 链回滚数据目录：
              先 <span class="mono">SHOW CHECKPOINTS</span>，再
              <span class="mono">RECOVER TO CHECKPOINT_SEQ n</span> 或使用「按 checkpoint 冷恢复」
              （C API <span class="mono">structdb_recover_data_dir_to_checkpoint_seq</span>，执行前会关闭 embed）。
              表级导出仍用 <span class="mono">EXPORT</span>；整库冷备见「工具 → 冷备 data_dir」与
              <span class="mono">Docs/BACKUP_RESTORE_RUNBOOK.md</span>。
            </p>
          </div>
        </div>
      </div>

      <template #footer>
        <div class="txn-panel-footer">
          <span class="txn-panel-footer-hint">操作耗时可在日志区查看</span>
          <el-button type="primary" plain @click="showTxnRecoveryModal = false">关闭</el-button>
        </div>
      </template>
    </el-dialog>

    <el-dialog v-model="showWalRecoveryModal" class="dashboard-modal" width="860px">
      <template #header>
        <div class="dashboard-modal-header">
          <h3 style="margin:0;">SHOW TUNING 摘要</h3>
          <div class="dashboard-modal-header-right">
            <el-button size="small" plain :disabled="busy" @click="refreshWalRecoverySummary">刷新</el-button>
          </div>
        </div>
      </template>
      <div class="tool-card">
        <div class="tool-title">最近 SHOW TUNING 相关日志（截取）</div>
        <pre class="mono" style="white-space: pre-wrap; margin: 0">{{ walRecoveryText || "n/a" }}</pre>
      </div>
      <template #footer>
        <el-button @click="showWalRecoveryModal = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showExportModal" class="dashboard-modal" width="860px">
      <template #header>
        <div class="dashboard-modal-header">
          <h3 style="margin:0;">导出诊断包</h3>
          <div class="dashboard-modal-header-right">
            <el-button size="small" plain :disabled="busy" @click="refreshRuntimeArtifacts">刷新版本</el-button>
          </div>
        </div>
      </template>
      <div class="tool-grid">
        <div class="tool-card">
          <div class="tool-title">存储冷备（C API backup_bundle）</div>
          <div class="mini-tip">
            复制工作区到目标目录并写入 backup_manifest.json；执行前会关闭 embed（与诊断 zip 不同）。
          </div>
          <div class="tool-row" style="margin-top: 8px">
            <el-button size="small" type="warning" plain :disabled="busy" @click="backupStorageBundle">冷备…</el-button>
            <span class="mini-tip mono" v-if="coldBackupPath">dest={{ coldBackupPath }}</span>
          </div>
        </div>
        <div class="tool-card">
          <div class="tool-title">一键导出诊断包（zip）</div>
          <div class="mini-tip">包含：scripts/results、工作区与 manifest.json（版本/路径快照，尽力而为）。</div>
          <div class="tool-row" style="margin-top: 8px">
            <el-button size="small" type="primary" :disabled="busy" @click="exportBundle">生成</el-button>
            <span class="mini-tip mono" v-if="exportBundlePath">path={{ exportBundlePath }}</span>
          </div>
        </div>
        <div class="tool-card">
          <div class="tool-title">运行环境摘要</div>
          <div v-if="runtimeArtifacts" class="mini-tip mono">
            gui={{ runtimeArtifacts.guiExePath }}\n
            capi_dll={{ runtimeArtifacts.dllPath }}\n
            structdb_app={{ runtimeArtifacts.demoPath }}\n
            capi={{ runtimeArtifacts.cApiVersionMajor ?? "" }}.{{ runtimeArtifacts.cApiVersionMinor ?? "" }}.{{
              runtimeArtifacts.cApiVersionPatch ?? ""
            }}/abi={{ runtimeArtifacts.cApiAbiVersion ?? "" }}
          </div>
          <div v-else class="mini-tip">未加载环境信息（点击「刷新版本」）</div>
        </div>
        <div class="tool-card">
          <div class="tool-title">执行计划（最近一次）</div>
          <div class="mini-tip">最近一次 SHOW PLAN 或 EXPLAIN WHERE 的原始输出；errorCodeNumeric 取自该次命令结果。</div>
          <div class="mini-tip mono">errorCodeNumeric={{ lastCommandErrorCodeNumeric ?? "n/a" }}</div>
          <pre class="mono" style="white-space: pre-wrap; max-height: 220px; overflow: auto; margin: 8px 0 0">{{
            lastShowPlanRaw || "（尚未运行 SHOW PLAN / EXPLAIN WHERE）"
          }}</pre>
        </div>
      </div>
      <template #footer>
        <el-button @click="showExportModal = false">关闭</el-button>
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
        <div class="settings-header">
          <h3 style="margin: 0">Settings</h3>
          <span class="settings-summary mono">{{ settingsSummary }}</span>
        </div>
      </template>
      <div class="settings-preset-row">
        <span class="settings-preset-label">预设</span>
        <div class="settings-preset-grid">
          <el-button
            v-for="preset in settingsPresets"
            :key="preset.key"
            size="small"
            plain
            class="settings-preset-btn"
            @click="applySettingsPreset(preset)"
          >
            <span class="preset-swatch-strip" aria-hidden="true">
              <span
                v-for="(c, si) in previewPresetColors(preset)"
                :key="`${preset.key}-${si}`"
                class="ps"
                :style="{ background: c }"
              />
            </span>
            {{ preset.label }}
          </el-button>
        </div>
      </div>
      <div class="settings-tools-row">
        <el-button size="small" @click="openSettingsImport">导入 JSON</el-button>
        <el-button size="small" @click="exportSettingsJson">导出 JSON</el-button>
        <el-tag size="small" :type="settingsDirty ? 'warning' : 'success'" effect="plain">
          {{ settingsDirty ? "未保存" : "已保存" }}
        </el-tag>
        <span class="mini-tip">last saved: {{ settingsLastSavedAt || "n/a" }}</span>
        <input
          ref="settingsImportInput"
          type="file"
          accept="application/json,.json"
          style="display:none"
          @change="onSettingsImportChange"
        />
      </div>
      <div class="settings-shell">
        <aside class="settings-side-menu">
          <button class="settings-menu-item" :class="{ active: settingsNav === 'theme' }" @click="settingsNav = 'theme'">
            主题与背景
          </button>
          <button class="settings-menu-item" :class="{ active: settingsNav === 'layout' }" @click="settingsNav = 'layout'">
            布局与交互
          </button>
          <button class="settings-menu-item" :class="{ active: settingsNav === 'module' }" @click="settingsNav = 'module'">
            模块化样式
          </button>
        </aside>
        <div class="settings-main">
        <section v-if="settingsNav === 'theme'" class="settings-card">
          <div class="settings-card-title">主题与背景</div>
          <el-form label-width="120px">
            <el-form-item label="主题色">
              <div class="settings-color-row">
                <input type="color" v-model="settings.accent" aria-label="主题色" />
                <el-input v-model="settings.accent" placeholder="#3b82f6" />
              </div>
            </el-form-item>
            <el-form-item label="界面文字">
              <div class="settings-color-grid">
                <div class="settings-color-field">
                  <label>主文字</label>
                  <div class="settings-color-row">
                    <input type="color" v-model="settings.textMain" aria-label="主文字色" />
                    <el-input v-model="settings.textMain" placeholder="#dbe7ff" />
                  </div>
                </div>
                <div class="settings-color-field">
                  <label>次要文字</label>
                  <div class="settings-color-row">
                    <input type="color" v-model="settings.textRegular" aria-label="次要文字色" />
                    <el-input v-model="settings.textRegular" placeholder="#c7d2fe" />
                  </div>
                </div>
                <div class="settings-color-field">
                  <label>弱化 / 提示</label>
                  <div class="settings-color-row">
                    <input type="color" v-model="settings.textSoft" aria-label="弱化文字色" />
                    <el-input v-model="settings.textSoft" placeholder="#93c5fd" />
                  </div>
                </div>
              </div>
              <div style="margin-top: 10px;">
                <el-button size="small" plain @click="resetThemeTextColors">文字色恢复默认</el-button>
                <span class="mini-tip" style="margin-left: 8px;">支持 #RGB / #RRGGBB；与主题色可任意组合</span>
              </div>
            </el-form-item>
            <el-form-item label="整体背景">
              <div class="settings-color-grid">
                <div class="settings-color-field" style="grid-column: 1 / -1">
                  <label>页面基色（侧栏外区域、body 叠层渐变锚点）</label>
                  <div class="settings-color-row">
                    <input type="color" v-model="settings.pageBg" aria-label="整体背景色" />
                    <el-input v-model="settings.pageBg" placeholder="#080d18" />
                  </div>
                </div>
                <div class="settings-color-field" style="grid-column: 1 / -1">
                  <label>主表格区底色（数据表 / Element 表格）</label>
                  <div class="settings-color-row">
                    <input type="color" v-model="settings.tableBg" aria-label="主表格背景色" />
                    <el-input v-model="settings.tableBg" placeholder="#08121f" />
                  </div>
                </div>
              </div>
              <div style="margin-top: 10px;">
                <el-button size="small" plain @click="resetCanvasBgColors">背景与表格恢复默认</el-button>
                <span class="mini-tip" style="margin-left: 8px;">「渐变背景」下主内容区叠层会随基色与主题色混合；透明度在「布局」里可调</span>
              </div>
            </el-form-item>
            <el-form-item label="背景模式">
              <el-select v-model="settings.bgMode">
                <el-option value="gradient" label="渐变背景" />
                <el-option value="image" label="背景图片" />
                <el-option value="video" label="动态壁纸" />
              </el-select>
            </el-form-item>
            <el-form-item label="背景图片 URL">
              <el-input v-model="settings.bgImageUrl" placeholder="https://.../wallpaper.jpg 或 file:///C:/tmp/bg.jpg" />
            </el-form-item>
            <el-form-item label="动态壁纸 URL">
              <el-input v-model="settings.bgVideoUrl" placeholder="https://.../wallpaper.mp4 或 file:///C:/tmp/bg.mp4" />
            </el-form-item>
            <el-form-item label="动态壁纸透过度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.bgVideoOpacity"
                  :min="0.05"
                  :max="0.8"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.bgVideoOpacity.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="本地图片">
              <div style="display:flex; gap:8px; align-items:center;">
                <el-button @click="pickBackgroundFile">选择图片文件</el-button>
                <el-button @click="pickBackgroundVideoFile">选择视频文件</el-button>
                <span class="mini-tip">已自动转为本地内嵌背景</span>
                <input
                  ref="bgFileInput"
                  type="file"
                  accept="image/*"
                  style="display:none"
                  @change="onBackgroundFileChange"
                />
                <input
                  ref="bgVideoFileInput"
                  type="file"
                  accept="video/*"
                  style="display:none"
                  @change="onBackgroundVideoFileChange"
                />
              </div>
            </el-form-item>
            <el-form-item label="背景透过度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.bgImageOpacity"
                  :min="0.05"
                  :max="0.8"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.bgImageOpacity.toFixed(2) }}</span>
              </div>
            </el-form-item>
          </el-form>
        </section>
        <section v-if="settingsNav === 'layout'" class="settings-card">
          <div class="settings-card-title">布局与交互</div>
          <el-form label-width="120px">
            <el-form-item label="面板透明度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.panelOpacity"
                  :min="0.6"
                  :max="1"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.panelOpacity.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="表格区不透明度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.tableViewOpacity"
                  :min="0.15"
                  :max="1"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.tableViewOpacity.toFixed(2) }}</span>
              </div>
              <div class="mini-tip" style="margin-top: 6px; line-height: 1.4">
                数值越低表格越「透」，底下渐变或壁纸越明显；默认已略调低便于看背景。
              </div>
            </el-form-item>
            <el-form-item label="日志面板透明度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.logPanelOpacity"
                  :min="0.35"
                  :max="1"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.logPanelOpacity.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="字体缩放">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.fontScale"
                  :min="0.9"
                  :max="1.2"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.fontScale.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="侧栏宽度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.sidebarWidth"
                  :min="200"
                  :max="460"
                  :step="1"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ Math.round(settings.sidebarWidth) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="圆角尺度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.cornerScale"
                  :min="0.8"
                  :max="1.35"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.cornerScale.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="阴影强度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.shadowScale"
                  :min="0.6"
                  :max="1.5"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.shadowScale.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="日志字号">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.logFontScale"
                  :min="0.88"
                  :max="1.25"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.logFontScale.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="日志行高">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.logLineHeight"
                  :min="1.3"
                  :max="1.9"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.logLineHeight.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="紧凑模式">
              <el-switch v-model="settings.denseMode" />
            </el-form-item>
            <el-form-item label="界面动画">
              <el-switch v-model="settings.animations" />
            </el-form-item>
          </el-form>
        </section>
        <section v-if="settingsNav === 'module'" class="settings-card">
          <div class="settings-card-title">模块化样式覆盖</div>
          <el-form label-width="120px">
            <el-form-item label="边框对比度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.borderContrast"
                  :min="0.75"
                  :max="1.4"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.borderContrast.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="主面板亮度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.panelBrightness"
                  :min="0.5"
                  :max="1.2"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.panelBrightness.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <el-form-item label="日志高亮强度">
              <div class="settings-slider-row">
                <el-slider
                  v-model="settings.logHighlightIntensity"
                  :min="0.75"
                  :max="1.4"
                  :step="0.01"
                  @input="onSettingsSliderInput"
                  @change="onSettingsSliderChange"
                />
                <span class="settings-slider-value mono">{{ settings.logHighlightIntensity.toFixed(2) }}</span>
              </div>
            </el-form-item>
            <div class="mini-tip">按模块影响：侧栏/主面板边框、主窗口亮度、日志高亮层级。</div>
          </el-form>
        </section>
        </div>
      </div>
      <template #footer>
        <el-button @click="persistSettings">立即保存</el-button>
        <el-button @click="resetSettings">恢复默认</el-button>
        <el-button type="primary" @click="showSettingsModal = false">关闭</el-button>
      </template>
    </el-dialog>

    <el-dialog v-model="showCreateTableWizard" class="settings-modal" width="760px">
      <template #header>
        <h3>创建表向导（DEFATTR）</h3>
      </template>
      <el-form label-width="120px" @keydown.enter="onCreateTableWizardEnter">
        <el-form-item label="表名">
          <el-input v-model="createTableName" placeholder="schema.table 或 table" />
        </el-form-item>

        <el-form-item label="属性列表">
          <p class="mini-tip" style="margin: 0 0 8px">
            类型与引擎 <span class="mono">known_logical_type</span> 一致：int、string、varchar、text、char、float、double、datetime、timestamp（无 bool/date）。
          </p>
          <div style="display:flex; flex-direction:column; gap:8px; width:100%;">
            <div
              v-for="(c, idx) in createCols"
              :key="`col-${idx}`"
              style="display:flex; gap:8px; align-items:center;"
            >
              <el-input v-model="c.name" placeholder="列名" style="flex:1;" />
              <el-select v-model="c.type" style="width: 160px;">
                <el-option v-for="opt in MDB_DEFATTR_TYPE_OPTIONS" :key="opt" :value="opt" :label="opt" />
              </el-select>
              <el-button size="small" type="danger" plain @click="removeCreateCol(idx)">删除</el-button>
            </div>
            <div style="display:flex; gap:8px; align-items:center;">
              <el-button size="small" type="primary" @click="addCreateCol">+ 添加属性</el-button>
            </div>
          </div>
        </el-form-item>

        <el-form-item label="主键">
          <el-input v-model="createPk" placeholder="id" style="width: 220px;" />
          <label style="display:flex; align-items:center; gap:6px; margin-left: 10px;">
            <el-switch v-model="createAlsoSetPk" />
            同时执行 SET PRIMARY KEY
          </label>
          <div class="mini-tip" style="margin-top: 6px;">
            主键列名须与上方属性列表中的某一列完全一致（例如默认已含 <span class="mono">id:int</span> 方可设主键为 id）。
          </div>
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
      <el-form label-width="120px" @keydown.enter="onGenericDialogEnter">
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

    <el-dialog v-model="showQuickRowModal" width="620px" :close-on-click-modal="false">
      <template #header>
        <h3>新增行（快捷）</h3>
      </template>
      <el-form label-width="120px">
        <el-form-item label="INSERT 行键">
          <el-input v-model="quickRowNextId" disabled />
        </el-form-item>
        <p class="mini-tip" style="margin: 0 0 8px">
          引擎要求参数个数为 <span class="mono">1 + DEFATTR 列数</span>：首参为行 id；下列各框与 DEFATTR 顺序一致（若含 <span class="mono">id</span> 列，默认与行键相同）。
        </p>
        <el-form-item v-for="(f, idx) in quickRowFields" :key="`quick-row-${f.name}-${idx}`" :label="`${f.name} (${f.ty})`">
          <el-input v-model="f.value" placeholder="默认 0" @keyup.enter="submitQuickAddRow" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="showQuickRowModal = false">取消</el-button>
        <el-button type="primary" :disabled="busy" @click="submitQuickAddRow">执行新增</el-button>
      </template>
    </el-dialog>

    <el-dialog
      v-model="showUiMessage"
      :width="uiMessageIsLogReview ? '760px' : '480px'"
      class="ui-message-modal"
      :close-on-click-modal="false"
      @closed="handleUiMessageClosed"
    >
      <template #header>
        <h3>{{ uiMessageTitle }}</h3>
      </template>
      <template v-if="uiMessageIsLogReview && parsedReviewLog">
        <div class="review-log-meta">
          <div><strong>命令</strong><span class="mono">{{ parsedReviewLog.command || "-" }}</span></div>
          <div><strong>时间</strong><span class="mono">{{ parsedReviewLog.time || "-" }}</span></div>
          <div><strong>区间</strong><span class="mono">{{ parsedReviewLog.lineRange || "-" }}</span></div>
        </div>
        <div class="review-log-toolbar">
          <el-tag size="small" effect="plain" :type="reviewHitIndices.length ? 'success' : 'info'">
            命中 {{ reviewHitIndices.length }}
          </el-tag>
          <span v-if="reviewHitIndices.length" class="review-hit-pos">
            第 {{ reviewHitCursor + 1 }} / {{ reviewHitIndices.length }} 条
          </span>
          <span v-else class="review-hit-pos">未找到命中行</span>
          <el-button size="small" :disabled="!reviewHitIndices.length" @click="jumpReviewHit(-1)">上一条</el-button>
          <el-button size="small" :disabled="!reviewHitIndices.length" @click="jumpReviewHit(1)">下一条</el-button>
        </div>
        <div ref="reviewLogPaneEl" class="review-log-pane">
          <div
            v-for="(line, idx) in reviewLogRows"
            :key="`review-log-${idx}`"
            class="log-line"
            :class="[
              `log-${highlightReviewLogLine(line).kind}`,
              { 'review-log-hit': reviewHitIndices.includes(idx), 'review-log-hit-active': reviewActiveHitLine === idx }
            ]"
            @click="focusReviewHitByLine(idx)"
            :ref="(el) => setReviewLogRowRef(idx, el as Element | null)"
          >
            <span class="review-log-line-no">{{ idx + 1 }}</span>
            <span>{{ line }}</span>
          </div>
        </div>
      </template>
      <div v-else style="white-space: pre-wrap;">{{ uiMessageText }}</div>
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

<style scoped>
.settings-modal :deep(.el-dialog),
.ui-message-modal :deep(.el-dialog),
.txn-panel-modal :deep(.el-dialog) {
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 22px;
  overflow: hidden;
  backdrop-filter: none;
  box-shadow: 0 18px 36px rgba(0, 0, 0, 0.22);
}

.settings-modal :deep(.el-dialog__header),
.ui-message-modal :deep(.el-dialog__header),
.txn-panel-modal :deep(.el-dialog__header) {
  padding: 18px 22px 14px;
  background: linear-gradient(135deg, rgba(90, 130, 255, 0.08), rgba(19, 25, 40, 0.02));
  border-bottom: 1px solid rgba(255, 255, 255, 0.05);
}

.settings-modal :deep(.el-dialog__body),
.ui-message-modal :deep(.el-dialog__body),
.txn-panel-modal :deep(.el-dialog__body) {
  padding: 18px 22px 20px;
}

.settings-modal :deep(.el-dialog__footer),
.ui-message-modal :deep(.el-dialog__footer),
.txn-panel-modal :deep(.el-dialog__footer) {
  padding: 14px 22px 18px;
  border-top: 1px solid rgba(255, 255, 255, 0.04);
  background: rgba(255, 255, 255, 0.001);
}

.settings-card {
  padding: 18px;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.001);
  border: 1px solid rgba(255, 255, 255, 0.01);
  box-shadow: none;
}

.settings-card + .settings-card {
  margin-top: 14px;
}

.settings-card-title {
  margin-bottom: 14px;
  font-size: 15px;
  font-weight: 700;
  letter-spacing: 0.02em;
}

.settings-slider-row {
  display: flex;
  align-items: center;
  gap: 12px;
  width: 100%;
}

.settings-slider-row :deep(.el-slider) {
  flex: 1;
}

.settings-slider-value {
  min-width: 52px;
  text-align: right;
  color: var(--text-soft);
}

.dialog-preview,
.review-log-pane {
  margin-top: 14px;
  padding: 14px;
  border-radius: 16px;
  background: rgba(10, 14, 24, 0.01);
  border: 1px solid rgba(255, 255, 255, 0.01);
  box-shadow: none;
}

.dialog-preview {
  font-family: ui-monospace, SFMono-Regular, Consolas, "Cascadia Mono", monospace;
  font-size: 12px;
  line-height: 1.6;
  color: var(--text-main);
  white-space: pre-wrap;
  word-break: break-word;
}

.review-log-meta {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}

.review-log-meta > div {
  padding: 10px 12px;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.001);
  border: 1px solid rgba(255, 255, 255, 0.01);
}

.review-log-meta strong {
  display: block;
  margin-bottom: 4px;
  font-size: 12px;
  color: var(--text-soft);
}

.review-log-toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
}

.review-hit-pos,
.mini-tip {
  color: var(--text-soft);
}

.review-log-pane {
  max-height: 56vh;
  overflow: auto;
  font-family: ui-monospace, SFMono-Regular, Consolas, "Cascadia Mono", monospace;
}

.review-log-pane .log-line {
  display: flex;
  gap: 12px;
  padding: 8px 10px;
  border-radius: 12px;
  transition: background 0.18s ease, transform 0.18s ease, box-shadow 0.18s ease;
}

.review-log-pane .log-line:hover {
  background: rgba(255, 255, 255, 0.05);
  transform: translateX(2px);
}

.review-log-line-no {
  min-width: 42px;
  color: var(--text-soft);
  text-align: right;
}

.review-log-hit {
  background: rgba(84, 189, 255, 0.14);
  box-shadow: inset 0 0 0 1px rgba(84, 189, 255, 0.18);
}

.review-log-hit-active {
  background: linear-gradient(90deg, rgba(84, 189, 255, 0.26), rgba(84, 189, 255, 0.16));
  box-shadow: inset 0 0 0 1px rgba(84, 189, 255, 0.38), 0 0 18px rgba(84, 189, 255, 0.1);
}

.mini-tip {
  font-size: 12px;
  line-height: 1.5;
}
.layout-image-wallpaper,
.layout-video-wallpaper {
  position: fixed;
  inset: 0;
  width: 100vw;
  height: 100vh;
  object-fit: cover;
  object-position: center center;
  opacity: var(--wallpaper-opacity, 0.58);
  filter: none;
  image-rendering: auto;
  transform: translateZ(0);
  pointer-events: none;
  z-index: 0;
}

.layout-image-wallpaper {
  filter: none;
  background: #000;
}

.layout > :not(.layout-image-wallpaper):not(.layout-video-wallpaper):not(.context-menu) {
  position: relative;
  z-index: 1;
}
:deep(.el-input__wrapper),
:deep(.el-select__wrapper) {
  border-radius: 12px;
}

:deep(.el-button) {
  border-radius: 12px;
}

.layout {
  --glass-surface: rgba(6, 10, 16, 0.01);
  --glass-surface-strong: rgba(8, 12, 20, 0.02);
  --glass-border: rgba(123, 198, 255, 0.02);
  --glass-border-strong: rgba(132, 231, 255, 0.03);
  min-height: 100vh;
  background: transparent;
  background-attachment: fixed;
  background-repeat: no-repeat;
  background-size: cover;
  background-position: center center;
  background-color: transparent;
}

.layout::after {
  content: "";
  position: fixed;
  inset: 0;
  pointer-events: none;
  background: none;
}

.layout::before {
  content: "";
  position: fixed;
  inset: 0;
  pointer-events: none;
  background: none;
  opacity: 0;
  mask-image: none;
}

.sidebar,
.content,
.log-only,
.console,
.main-panel,
.stack-panel,
.sidebar-section,
.tool-card,
.toolbar,
.pager,
.breadcrumb-bar,
.open-table-tabs,
.open-table-tabs-stack,
.data-table-wrap,
.console-header,
.console-input,
.log-tools,
.stack-item,
.stack-list-pane,
.mdb-editor-panel,
.mdb-textarea {
  border: 1px solid rgba(123, 198, 255, 0.01);
  background: rgba(6, 10, 16, 0.01);
  box-shadow: none;
  backdrop-filter: none;
}

.sidebar,
.content,
.log-only {
  background: transparent;
}

.sidebar {
  border-right-color: var(--glass-border-strong);
}

.sidebar-brand,
.menu-bar,
.toolbar,
.console-header,
.pager,
.stack-header,
.breadcrumb-bar,
.open-table-tabs,
.app-status-bar {
  background: rgba(255, 255, 255, 0.002);
  border: 1px solid rgba(120, 186, 255, 0.01);
  border-radius: 18px;
}

.sidebar-brand,
.menu-bar,
.toolbar,
.console-header,
.pager,
.stack-header,
.breadcrumb-bar,
.open-table-tabs {
  padding: 14px 16px;
}

.sidebar-brand-mark,
.menu-bar-brand-mark {
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.sidebar-section,
.tool-card,
.stack-item,
.stack-list-pane,
.data-table-wrap,
.mdb-editor-panel {
  border-radius: 18px;
}

.sidebar-section,
.tool-card,
.stack-list-pane,
.mdb-editor-panel {
  padding: 14px;
}

.table-tree-node {
  border-radius: 12px;
  transition: background 0.18s ease, transform 0.18s ease;
}

.table-tree-node:hover {
  background: linear-gradient(90deg, rgba(120, 203, 255, 0.12), rgba(86, 166, 255, 0.06));
  transform: translateX(3px);
  box-shadow: inset 0 0 0 1px rgba(120, 203, 255, 0.1);
}

.table-tree-node.active {
  background: linear-gradient(90deg, rgba(84, 189, 255, 0.22), rgba(84, 189, 255, 0.08));
  box-shadow: inset 0 0 0 1px rgba(84, 189, 255, 0.2), 0 0 24px rgba(84, 189, 255, 0.08);
}

.toolbar,
.pager,
.console-header,
.stack-header,
.open-table-tabs,
.breadcrumb-bar,
.app-status-bar {
  gap: 12px;
}

.toolbar-group,
.log-tools,
.console-input,
.stack-actions,
.stack-row-actions,
.tool-row,
.settings-slider-row,
.review-log-toolbar {
  gap: 10px;
}

.content {
  padding: 16px;
}

.main-panel,
.console,
.stack-panel,
.log-only {
  border-radius: 22px;
}

.data-table-wrap :deep(.el-table),
.data-table-wrap :deep(.el-table__header-wrapper),
.data-table-wrap :deep(.el-table__body-wrapper) {
  background: transparent;
}

.data-table-wrap :deep(.el-table tr),
.data-table-wrap :deep(.el-table th.el-table__cell),
.data-table-wrap :deep(.el-table td.el-table__cell) {
  background-color: rgba(255, 255, 255, 0.03) !important;
  border-color: rgba(255, 255, 255, 0.06) !important;
}

.data-table-wrap :deep(.el-table th.el-table__cell) {
  font-weight: 700;
  color: var(--text-main);
}

.data-table-wrap :deep(.el-table__row:hover > td.el-table__cell) {
  background-color: rgba(86, 166, 255, 0.1) !important;
}

.data-table-wrap :deep(.el-table__row.current-row > td.el-table__cell) {
  background-color: rgba(84, 189, 255, 0.14) !important;
}

.data-table-wrap :deep(.el-table__inner-wrapper::before) {
  display: none;
}

.empty-table-hint,
.stack-empty {
  color: var(--text-soft);
  background: rgba(255, 255, 255, 0.04);
  border: 1px dashed rgba(255, 255, 255, 0.1);
  border-radius: 14px;
}

.stack-item.selected {
  box-shadow: inset 0 0 0 1px rgba(86, 166, 255, 0.3), 0 0 28px rgba(86, 166, 255, 0.1);
}

.secondary,
.secondary.mini,
.menu-title,
:deep(.el-button.is-plain) {
  border-color: rgba(255, 255, 255, 0.12);
}

.secondary:hover,
.secondary.mini:hover,
:deep(.el-button.is-plain:hover) {
  border-color: rgba(86, 166, 255, 0.4);
  background: rgba(86, 166, 255, 0.14);
  box-shadow: 0 0 0 1px rgba(86, 166, 255, 0.08);
}

.log-line {
  border-radius: 10px;
}

.log-cmd,
.log-success,
.log-error,
.log-warning,
.log-meta {
  border-left: 3px solid transparent;
}

.log-cmd { border-left-color: color-mix(in srgb, var(--accent) 60%, #7dd3fc); }
.log-success { border-left-color: #57d38c; }
.log-error { border-left-color: #ff6b7a; }
.log-warning { border-left-color: #f0b84c; }
.log-meta { border-left-color: #6ea8ff; }

@media (max-width: 1200px) {
  .review-log-meta {
    grid-template-columns: 1fr;
  }

  .toolbar,
  .pager,
  .console-header,
  .stack-header,
  .breadcrumb-bar,
  .open-table-tabs {
    border-radius: 16px;
  }
}
</style>
