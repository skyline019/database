use serde::{Deserialize, Serialize};
use libloading::{Library, Symbol};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, OsStr};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::io::{Read, Write};
use std::process::Stdio;
use std::thread;
use walkdir::WalkDir;
use zip::write::FileOptions;
use tauri::{Emitter, Manager, State};
#[cfg(windows)]
use std::os::windows::process::CommandExt;
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;

/// Must match [`src/c_api/include/structdb_capi.h`](../../../../src/c_api/include/structdb_capi.h).
const STRUCTDB_MDB_RUN_OPTIONS_SIZE_V2: u32 = 56;
const STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3: u32 = 64;
const STRUCTDB_CAPI_OK: i32 = 0;
const STRUCTDB_CAPI_ERR_CANCELLED: i32 = 6;
/// Hard cap for `PAGE_JSON` page size from the GUI (align with MDB / engine tolerance).
const MDB_PAGE_JSON_MAX_PAGE_SIZE: usize = 500;

#[repr(C)]
struct StructdbMdbRunOptions {
    struct_size: u32,
    reserved_flags: u32,
    fsync_each_batch: i32,
    fsync_each_session_txn_op: i32,
    fail_if_unclosed_txn: i32,
    allow_persist_while_txn_active_experimental: i32,
    log_file_path: *const c_char,
    log_line_cb: Option<unsafe extern "C" fn(*mut c_void, *const c_char)>,
    log_user_data: *mut c_void,
    repl_exit_requested_out: *mut i32,
    long_task_control: *mut c_void,
}

#[repr(C)]
struct StructdbLongTaskProgress {
    kind: *const c_char,
    status: *const c_char,
    units_done: u64,
    units_total: u64,
    bytes_done: u64,
    bytes_total: u64,
    detail: *const c_char,
    task_id: *const c_char,
}

struct LongTaskCbCtx {
    app: tauri::AppHandle,
}

unsafe extern "C" fn cap_long_task_progress_cb(user: *mut c_void, progress: *const StructdbLongTaskProgress) {
    if user.is_null() || progress.is_null() {
        return;
    }
    let ctx = &*(user as *const LongTaskCbCtx);
    let p = &*progress;
    let kind = if p.kind.is_null() {
        "unknown".to_string()
    } else {
        CStr::from_ptr(p.kind).to_string_lossy().into_owned()
    };
    let status = if p.status.is_null() {
        "running".to_string()
    } else {
        CStr::from_ptr(p.status).to_string_lossy().into_owned()
    };
    let detail = if p.detail.is_null() {
        None
    } else {
        Some(CStr::from_ptr(p.detail).to_string_lossy().into_owned())
    };
    let task_id = if p.task_id.is_null() {
        None
    } else {
        Some(CStr::from_ptr(p.task_id).to_string_lossy().into_owned())
    };
    emit_long_task_progress(
        &ctx.app,
        LongTaskProgressPayload {
            kind,
            status,
            units_done: p.units_done,
            units_total: p.units_total,
            bytes_done: p.bytes_done,
            bytes_total: p.bytes_total,
            detail,
            task_id,
        },
    );
}

unsafe extern "C" fn structdb_log_line_cb(user: *mut c_void, line: *const c_char) {
    if user.is_null() || line.is_null() {
        return;
    }
    let sink = &mut *(user as *mut Vec<String>);
    let s = CStr::from_ptr(line).to_string_lossy().to_string();
    sink.push(s);
}

#[derive(Clone, Default)]
struct SessionState {
    data_dir: String,
    current_table: String,
    page_size: usize,
}

#[derive(Default)]
struct CapSessionState {
    engine: Option<usize>,
    embed: Option<usize>,
    mdb_session: Option<usize>,
    data_dir: String,
}

/// One `CONFIRM_REORDER` step: old row id -> new row id for `table` (session table name, may include schema).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct IdRemapLayer {
    table: String,
    map: HashMap<String, String>,
}

/// Opaque C API `structdb_long_task_control*`; only used from the Tauri command thread.
struct LongTaskCtrlPtr(*mut c_void);
unsafe impl Send for LongTaskCtrlPtr {}
unsafe impl Sync for LongTaskCtrlPtr {}

struct AppState {
    st: Mutex<SessionState>,
    cap: Mutex<CapSessionState>,
    id_remap_chain: Mutex<Vec<IdRemapLayer>>,
    /// Active C API `structdb_long_task_control*` during script/export/bench (if any).
    active_long_task: Mutex<Option<LongTaskCtrlPtr>>,
    /// Demo-path script cancel when C API batch is not used.
    script_cancel: std::sync::Arc<AtomicBool>,
}

struct CapApi {
    _lib: Library,
    capi_version: unsafe extern "C" fn() -> u32,
    version_string: unsafe extern "C" fn() -> *const c_char,
    engine_open_ex: unsafe extern "C" fn(*const c_char, u32, *mut c_char, usize) -> *mut c_void,
    engine_shutdown: unsafe extern "C" fn(*mut c_void),
    embed_open: unsafe extern "C" fn(*mut c_void, *const c_char, *mut c_char, usize) -> *mut c_void,
    embed_close: unsafe extern "C" fn(*mut c_void),
    mdb_session_create: unsafe extern "C" fn() -> *mut c_void,
    mdb_session_destroy: unsafe extern "C" fn(*mut c_void),
    mdb_execute_line_ex: unsafe extern "C" fn(
        *mut c_void,
        *mut c_void,
        *mut c_void,
        *const c_char,
        *mut c_char,
        usize,
        *const StructdbMdbRunOptions,
    ) -> i32,
    long_task_control_create: unsafe extern "C" fn() -> *mut c_void,
    long_task_control_destroy: unsafe extern "C" fn(*mut c_void),
    long_task_control_request_cancel: unsafe extern "C" fn(*mut c_void),
    long_task_control_cancel_requested: unsafe extern "C" fn(*const c_void) -> i32,
    long_task_control_set_progress_callback: unsafe extern "C" fn(
        *mut c_void,
        Option<unsafe extern "C" fn(*mut c_void, *const StructdbLongTaskProgress)>,
        *mut c_void,
    ),
    engine_begin_mdb_script_batch: unsafe extern "C" fn(*mut c_void, *mut c_void, u64),
    engine_end_mdb_script_batch: unsafe extern "C" fn(*mut c_void),
}

static CAP_API: OnceCell<CapApi> = OnceCell::new();

struct CliTermProcess {
    child: std::process::Child,
    stdin: Mutex<std::process::ChildStdin>,
}

/// 独立 `structdb_app` 子进程，或与主窗口 **共用 C API 引擎**（避免同一 `data_dir` 下 WAL 二次打开失败）。
enum CliSession {
    Child(CliTermProcess),
    InProcess,
}

static CLI_TERM_PROC: Mutex<Option<CliSession>> = Mutex::new(None);

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CliTermChunk {
    chunk: String,
    stream: String,
}

fn emit_cli_output(app: &tauri::AppHandle, stream: &str, chunk: &str) {
    let _ = app.emit(
        "cli-term-chunk",
        CliTermChunk {
            chunk: chunk.to_string(),
            stream: stream.to_string(),
        },
    );
}

#[cfg(windows)]
fn normalize_dir_compare_token(s: &str) -> String {
    s.trim().replace('/', "\\").to_lowercase()
}

#[cfg(not(windows))]
fn normalize_dir_compare_token(s: &str) -> String {
    s.trim().to_string()
}

fn dirs_logical_equal(a: &str, b: &str) -> bool {
    let a = a.trim();
    let b = b.trim();
    if a.is_empty() || b.is_empty() {
        return false;
    }
    if let (Ok(ca), Ok(cb)) = (fs::canonicalize(a), fs::canonicalize(b)) {
        return ca == cb;
    }
    normalize_dir_compare_token(a) == normalize_dir_compare_token(b)
}

/// 主窗口已通过 C API 打开 `embed` 且 `data_dir` 与 CLI 请求的工作区相同（此时再起 `structdb_app` 会因 WAL 独占打开失败）。
fn cap_holds_embed_for_workspace(app: &AppState, workspace_dir: &str) -> bool {
    if load_cap_api().is_err() {
        return false;
    }
    let cap = match app.cap.lock() {
        Ok(c) => c,
        Err(_) => return false,
    };
    cap.embed.is_some() && dirs_logical_equal(&cap.data_dir, workspace_dir)
}

fn spawn_cli_pipe_reader<R: Read + Send + 'static>(app: tauri::AppHandle, pipe: R, stream: &'static str) {
    thread::spawn(move || {
        let mut reader = pipe;
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let chunk = String::from_utf8_lossy(&buf[..n]).to_string();
                    let _ = app.emit(
                        "cli-term-chunk",
                        CliTermChunk {
                            chunk,
                            stream: stream.to_string(),
                        },
                    );
                }
                Err(_) => break,
            }
        }
    });
}

fn cli_terminal_stop_inner() -> Result<(), String> {
    let mut guard = CLI_TERM_PROC
        .lock()
        .map_err(|_| "cli term mutex poisoned".to_string())?;
    if let Some(sess) = guard.take() {
        if let CliSession::Child(mut p) = sess {
            let _ = p.child.kill();
            let _ = p.child.wait();
        }
    }
    Ok(())
}

#[tauri::command]
fn cli_terminal_start(
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    data_dir: Option<String>,
    table: Option<String>,
) -> Result<(), String> {
    configure_runtime_loader_paths();
    cli_terminal_stop_inner()?;
    let (dir, tbl) = {
        let st = state
            .inner()
            .st
            .lock()
            .map_err(|_| "state lock poisoned".to_string())?;
        let dir = data_dir
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| st.data_dir.clone());
        let tbl = table.unwrap_or_else(|| st.current_table.clone());
        (dir, tbl)
    };

    if !dir.trim().is_empty() && cap_holds_embed_for_workspace(state.inner(), &dir) {
        emit_cli_output(
            &app,
            "stdout",
            "StructDB REPL（进程内，与主窗口共用引擎 / WAL；无法另起 structdb_app 子进程）\r\n\
             空行：提示；输入 EXIT 结束 MDB 提示；执行 MDB 命令与主窗口同一会话。\r\n\
             StructDB REPL (empty line shows hint; EXIT for MDB exit log; same session as main window).\r\n",
        );
        if !tbl.trim().is_empty() {
            let use_line = format!("USE({})", tbl.trim());
            match dispatch_mdb_command(state.inner(), use_line) {
                Ok(out) => {
                    if !out.is_empty() {
                        emit_cli_output(&app, "stdout", &format!("{out}\r\n"));
                    }
                }
                Err(e) => emit_cli_output(&app, "stderr", &format!("{e}\r\n")),
            }
        }
        let mut guard = CLI_TERM_PROC
            .lock()
            .map_err(|_| "cli term mutex poisoned".to_string())?;
        *guard = Some(CliSession::InProcess);
        return Ok(());
    }

    let app_bin = resolve_structdb_app_bin()?;
    let mut cmd = Command::new(&app_bin);
    if !dir.trim().is_empty() {
        if !Path::new(&dir).exists() {
            fs::create_dir_all(&dir)
                .map_err(|e| format!("failed to create workspace {}: {e}", dir))?;
        }
        cmd.arg("--data-dir").arg(&dir);
        let sd = Path::new(&dir).join("embed_session");
        cmd.arg("--session-dir").arg(sd.to_string_lossy().as_ref());
    }
    cmd.arg("--repl");

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        cmd.creation_flags(0x08000000);
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn structdb_app: {e}"))?;
    let stdin = child.stdin.take().ok_or("structdb_app stdin unavailable")?;
    let stdout = child
        .stdout
        .take()
        .ok_or("structdb_app stdout unavailable")?;
    let stderr = child
        .stderr
        .take()
        .ok_or("structdb_app stderr unavailable")?;

    spawn_cli_pipe_reader(app.clone(), stdout, "stdout");
    spawn_cli_pipe_reader(app.clone(), stderr, "stderr");

    let mut guard = CLI_TERM_PROC
        .lock()
        .map_err(|_| "cli term mutex poisoned".to_string())?;
    *guard = Some(CliSession::Child(CliTermProcess {
        child,
        stdin: Mutex::new(stdin),
    }));
    Ok(())
}

#[tauri::command]
fn cli_terminal_write_line(app: tauri::AppHandle, state: State<'_, AppState>, line: String) -> Result<(), String> {
    let guard = CLI_TERM_PROC
        .lock()
        .map_err(|_| "cli term mutex poisoned".to_string())?;
    let sess = guard.as_ref().ok_or("CLI session not started; use 重新连接")?;
    match sess {
        CliSession::InProcess => {
            let line_trim = line.trim_end_matches('\n').trim_end_matches('\r');
            if line_trim.is_empty() {
                emit_cli_output(
                    &app,
                    "stderr",
                    "(进程内 CLI: 空行不执行命令；使用「断开会话」关闭)\r\n",
                );
                return Ok(());
            }
            match dispatch_mdb_command(state.inner(), line_trim.to_string()) {
                Ok(out) => {
                    if !out.is_empty() {
                        let mut chunk = out;
                        if !chunk.ends_with('\n') {
                            chunk.push('\n');
                        }
                        emit_cli_output(&app, "stdout", &chunk);
                    } else {
                        emit_cli_output(&app, "stdout", "\r\n");
                    }
                }
                Err(e) => emit_cli_output(&app, "stderr", &format!("{e}\r\n")),
            }
            Ok(())
        }
        CliSession::Child(p) => {
            let mut w = p
                .stdin
                .lock()
                .map_err(|_| "stdin mutex poisoned".to_string())?;
            w.write_all(line.as_bytes())
                .and_then(|_| w.write_all(b"\n"))
                .map_err(|e| format!("stdin write failed: {e}"))?;
            w.flush().map_err(|e| format!("stdin flush failed: {e}"))?;
            Ok(())
        }
    }
}

#[tauri::command]
fn cli_terminal_stop() -> Result<(), String> {
    cli_terminal_stop_inner()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UiState {
    data_dir: String,
    current_table: String,
    page_size: usize,
}

#[derive(Serialize)]
struct PageResult {
    headers: Vec<String>,
    columns: Vec<ColumnMeta>,
    rows: Vec<Vec<String>>,
    raw: String,
}

#[derive(Deserialize)]
struct PageJsonResult {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    columns: Option<Vec<ColumnMeta>>,
}

fn default_text_main() -> String {
    "#dbe7ff".to_string()
}
fn default_text_regular() -> String {
    "#c7d2fe".to_string()
}
fn default_text_soft() -> String {
    "#93c5fd".to_string()
}
fn default_page_bg() -> String {
    "#080d18".to_string()
}
fn default_table_bg() -> String {
    "#08121f".to_string()
}

fn default_bg_image_opacity() -> f32 {
    0.58
}

fn default_bg_video_opacity() -> f32 {
    0.58
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct UiSettings {
    accent: String,
    #[serde(default = "default_text_main")]
    text_main: String,
    #[serde(default = "default_text_regular")]
    text_regular: String,
    #[serde(default = "default_text_soft")]
    text_soft: String,
    #[serde(default = "default_page_bg")]
    page_bg: String,
    #[serde(default = "default_table_bg")]
    table_bg: String,
    bg_mode: String,
    bg_image_url: String,
    #[serde(default = "default_bg_image_opacity")]
    bg_image_opacity: f32,
    #[serde(default)]
    bg_video_url: String,
    #[serde(default = "default_bg_video_opacity")]
    bg_video_opacity: f32,
    panel_opacity: f32,
    table_view_opacity: f32,
    log_panel_opacity: f32,
    font_scale: f32,
    dense_mode: bool,
    animations: bool,
    sidebar_width: f32,
    corner_scale: f32,
    shadow_scale: f32,
    log_font_scale: f32,
    log_line_height: f32,
    border_contrast: f32,
    panel_brightness: f32,
    log_highlight_intensity: f32,
}

impl Default for UiSettings {
    fn default() -> Self {
        Self {
            accent: "#3b82f6".to_string(),
            text_main: default_text_main(),
            text_regular: default_text_regular(),
            text_soft: default_text_soft(),
            page_bg: default_page_bg(),
            table_bg: default_table_bg(),
            bg_mode: "gradient".to_string(),
            bg_image_url: String::new(),
            bg_image_opacity: default_bg_image_opacity(),
            bg_video_url: String::new(),
            bg_video_opacity: default_bg_video_opacity(),
            panel_opacity: 0.9,
            table_view_opacity: 0.74,
            log_panel_opacity: 0.92,
            font_scale: 1.0,
            dense_mode: false,
            animations: true,
            sidebar_width: 260.0,
            corner_scale: 1.0,
            shadow_scale: 1.0,
            log_font_scale: 1.0,
            log_line_height: 1.5,
            border_contrast: 1.0,
            panel_brightness: 1.0,
            log_highlight_intensity: 1.0,
        }
    }
}

fn settings_file_path(app: &tauri::AppHandle) -> Result<PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|e| format!("app_config_dir failed: {e}"))?;
    Ok(dir.join("settings.json"))
}

#[tauri::command]
fn get_settings(app: tauri::AppHandle) -> UiSettings {
    let path = match settings_file_path(&app) {
        Ok(p) => p,
        Err(_) => return UiSettings::default(),
    };
    let Ok(text) = fs::read_to_string(&path) else {
        return UiSettings::default();
    };
    serde_json::from_str::<UiSettings>(&text).unwrap_or_default()
}

#[tauri::command]
fn set_settings(app: tauri::AppHandle, settings: UiSettings) -> Result<(), String> {
    let path = settings_file_path(&app)?;
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)
            .map_err(|e| format!("failed to create config dir {}: {e}", dir.display()))?;
    }
    let text = serde_json::to_string_pretty(&settings).map_err(|e| e.to_string())?;
    fs::write(&path, text).map_err(|e| format!("failed to write {}: {e}", path.display()))?;
    Ok(())
}

fn collect_runtime_candidates_for_leaf(leaf: &str) -> Vec<PathBuf> {
    let mut c = Vec::new();
    push_runtime_artifact_candidates(&mut c, leaf);
    c
}

fn resolve_structdb_app_bin() -> Result<PathBuf, String> {
    const LEAVES: &[&str] = &["structdb_app.exe", "structdb_app"];
    resolve_first_existing_leaf(LEAVES).ok_or_else(|| {
        let tried = LEAVES
            .iter()
            .flat_map(|leaf| collect_runtime_candidates_for_leaf(leaf))
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(" | ");
        format!("structdb_app not found; tried: {tried}")
    })
}

fn resolve_results_dir() -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("scripts").join("results"));
        candidates.push(cwd.join("../scripts").join("results"));
        candidates.push(cwd.join("../../scripts").join("results"));
        candidates.push(cwd.join("src-tauri").join("resources").join("scripts").join("results"));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("results"));
            candidates.push(dir.join("scripts").join("results"));
            candidates.push(dir.join("../scripts").join("results"));
            candidates.push(dir.join("../../scripts").join("results"));
            candidates.push(dir.join("../../../scripts").join("results"));
            candidates.push(dir.join("resources").join("scripts").join("results"));
        }
    }
    if let Some(manifest_dir) = option_env!("CARGO_MANIFEST_DIR") {
        let manifest = PathBuf::from(manifest_dir);
        candidates.push(manifest.join("../scripts/results"));
        candidates.push(manifest.join("resources/scripts/results"));
    }
    candidates.push(PathBuf::from("scripts").join("results"));
    candidates.push(PathBuf::from("../scripts").join("results"));
    candidates.push(PathBuf::from("../../scripts").join("results"));

    for p in &candidates {
        if p.exists() {
            return Ok(p.clone());
        }
    }
    let tried = candidates
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(" | ");
    Err(format!("scripts/results directory not found; tried: {tried}"))
}

/// Candidate search order for runtime DLL/SO next to the packaged GUI (`structdb_capi_shared`).
fn push_runtime_artifact_candidates(candidates: &mut Vec<PathBuf>, leaf: &str) {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            // `tauri dev`: exe is …/src-tauri/target/debug|release/<name>; runtime DLLs live in …/src-tauri/bin/.
            candidates.push(dir.join("../../bin").join(leaf));
            candidates.push(dir.join(leaf));
            candidates.push(dir.join("bin").join(leaf));
            candidates.push(dir.join("../bin").join(leaf));
            candidates.push(dir.join("resources").join(leaf));
            candidates.push(dir.join("../resources").join(leaf));
            candidates.push(dir.join("..").join(leaf));
        }
    }
    candidates.push(PathBuf::from(leaf));
    candidates.push(PathBuf::from("bin").join(leaf));
    // `npm run tauri:dev` cwd is often `gui/rust_gui/`; binaries are under `src-tauri/bin/`.
    candidates.push(PathBuf::from("src-tauri").join("bin").join(leaf));
    candidates.push(PathBuf::from("../build_shared").join(leaf));
    candidates.push(PathBuf::from("../build").join(leaf));
    candidates.push(PathBuf::from("../build/RelWithDebInfo").join(leaf));
    candidates.push(PathBuf::from("../build/Release").join(leaf));
    candidates.push(PathBuf::from("../build/Debug").join(leaf));
    candidates.push(PathBuf::from("../build/src/app/RelWithDebInfo").join(leaf));
    candidates.push(PathBuf::from("../build/src/app/Release").join(leaf));
    candidates.push(PathBuf::from("../build/src/app/Debug").join(leaf));
    candidates.push(PathBuf::from("../build/src/c_api/RelWithDebInfo").join(leaf));
    candidates.push(PathBuf::from("../build/src/c_api/Release").join(leaf));
    candidates.push(PathBuf::from("../build/src/c_api/Debug").join(leaf));
    candidates.push(PathBuf::from("../../build_shared").join(leaf));
    candidates.push(PathBuf::from("../../build").join(leaf));
    candidates.push(PathBuf::from("../../build/RelWithDebInfo").join(leaf));
    candidates.push(PathBuf::from("../../build/src/app/RelWithDebInfo").join(leaf));
    candidates.push(PathBuf::from("../../build/src/app/Release").join(leaf));
    candidates.push(PathBuf::from("../../build/src/c_api/RelWithDebInfo").join(leaf));
}

fn resolve_first_existing_leaf(leaves: &[&str]) -> Option<PathBuf> {
    for leaf in leaves {
        let mut candidates: Vec<PathBuf> = Vec::new();
        push_runtime_artifact_candidates(&mut candidates, leaf);
        for p in candidates {
            if p.exists() {
                return Some(p);
            }
        }
    }
    None
}

fn resolve_structdb_capi_dll() -> PathBuf {
    resolve_first_existing_leaf(&["structdb_capi_shared.dll", "libstructdb_capi_shared.dll"])
        .unwrap_or_else(|| PathBuf::from("structdb_capi_shared.dll"))
}

fn load_cap_api() -> Result<&'static CapApi, String> {
    CAP_API.get_or_try_init(|| {
        configure_runtime_loader_paths();
        let lib_path = resolve_structdb_capi_dll();
        let lib = unsafe { Library::new(&lib_path) }
            .map_err(|e| format!("load structdb_capi_shared failed: {} ({e})", lib_path.display()))?;
        let capi_version_fn = {
            let s: Symbol<unsafe extern "C" fn() -> u32> = unsafe { lib.get(b"structdb_capi_version\0") }
                .map_err(|e| e.to_string())?;
            *s
        };
        let version_string_fn = {
            let s: Symbol<unsafe extern "C" fn() -> *const c_char> = unsafe { lib.get(b"structdb_capi_version_string\0") }
                .map_err(|e| e.to_string())?;
            *s
        };
        let engine_open_ex_fn = {
            let s: Symbol<unsafe extern "C" fn(*const c_char, u32, *mut c_char, usize) -> *mut c_void> =
                unsafe { lib.get(b"structdb_engine_open_ex\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let engine_shutdown_fn = {
            let s: Symbol<unsafe extern "C" fn(*mut c_void)> =
                unsafe { lib.get(b"structdb_engine_shutdown\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let embed_open_fn = {
            let s: Symbol<
                unsafe extern "C" fn(*mut c_void, *const c_char, *mut c_char, usize) -> *mut c_void,
            > = unsafe { lib.get(b"structdb_embed_open\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let embed_close_fn = {
            let s: Symbol<unsafe extern "C" fn(*mut c_void)> =
                unsafe { lib.get(b"structdb_embed_close\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let mdb_session_create_fn = {
            let s: Symbol<unsafe extern "C" fn() -> *mut c_void> =
                unsafe { lib.get(b"structdb_mdb_session_create\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let mdb_session_destroy_fn = {
            let s: Symbol<unsafe extern "C" fn(*mut c_void)> =
                unsafe { lib.get(b"structdb_mdb_session_destroy\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let mdb_execute_line_ex_fn = {
            let s: Symbol<
                unsafe extern "C" fn(
                    *mut c_void,
                    *mut c_void,
                    *mut c_void,
                    *const c_char,
                    *mut c_char,
                    usize,
                    *const StructdbMdbRunOptions,
                ) -> i32,
            > = unsafe { lib.get(b"structdb_mdb_execute_line_ex\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let long_task_control_create_fn = {
            let s: Symbol<unsafe extern "C" fn() -> *mut c_void> =
                unsafe { lib.get(b"structdb_long_task_control_create\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let long_task_control_destroy_fn = {
            let s: Symbol<unsafe extern "C" fn(*mut c_void)> =
                unsafe { lib.get(b"structdb_long_task_control_destroy\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let long_task_control_request_cancel_fn = {
            let s: Symbol<unsafe extern "C" fn(*mut c_void)> =
                unsafe { lib.get(b"structdb_long_task_control_request_cancel\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let long_task_control_cancel_requested_fn = {
            let s: Symbol<unsafe extern "C" fn(*const c_void) -> i32> =
                unsafe { lib.get(b"structdb_long_task_control_cancel_requested\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let long_task_control_set_progress_callback_fn = {
            let s: Symbol<
                unsafe extern "C" fn(
                    *mut c_void,
                    Option<unsafe extern "C" fn(*mut c_void, *const StructdbLongTaskProgress)>,
                    *mut c_void,
                ),
            > = unsafe { lib.get(b"structdb_long_task_control_set_progress_callback\0") }
                .map_err(|e| e.to_string())?;
            *s
        };
        let engine_begin_mdb_script_batch_fn = {
            let s: Symbol<unsafe extern "C" fn(*mut c_void, *mut c_void, u64)> =
                unsafe { lib.get(b"structdb_engine_begin_mdb_script_batch\0") }.map_err(|e| e.to_string())?;
            *s
        };
        let engine_end_mdb_script_batch_fn = {
            let s: Symbol<unsafe extern "C" fn(*mut c_void)> =
                unsafe { lib.get(b"structdb_engine_end_mdb_script_batch\0") }.map_err(|e| e.to_string())?;
            *s
        };

        Ok(CapApi {
            _lib: lib,
            capi_version: capi_version_fn,
            version_string: version_string_fn,
            engine_open_ex: engine_open_ex_fn,
            engine_shutdown: engine_shutdown_fn,
            embed_open: embed_open_fn,
            embed_close: embed_close_fn,
            mdb_session_create: mdb_session_create_fn,
            mdb_session_destroy: mdb_session_destroy_fn,
            mdb_execute_line_ex: mdb_execute_line_ex_fn,
            long_task_control_create: long_task_control_create_fn,
            long_task_control_destroy: long_task_control_destroy_fn,
            long_task_control_request_cancel: long_task_control_request_cancel_fn,
            long_task_control_cancel_requested: long_task_control_cancel_requested_fn,
            long_task_control_set_progress_callback: long_task_control_set_progress_callback_fn,
            engine_begin_mdb_script_batch: engine_begin_mdb_script_batch_fn,
            engine_end_mdb_script_batch: engine_end_mdb_script_batch_fn,
        })
    })
}

fn runtime_candidate_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            // Prefer `src-tauri/bin` (sync target) for Windows DLL dependencies before `target/debug`.
            dirs.push(dir.join("../../bin"));
            dirs.push(dir.to_path_buf());
            dirs.push(dir.join("bin"));
            dirs.push(dir.join("../bin"));
            dirs.push(dir.join("resources"));
            dirs.push(dir.join("../resources"));
        }
    }
    dirs.push(PathBuf::from("bin"));
    dirs.push(PathBuf::from("src-tauri").join("bin"));
    dirs.push(PathBuf::from("../build_shared"));
    dirs.push(PathBuf::from("../build"));
    dirs
}

#[cfg(windows)]
fn set_windows_dll_directory(dir: &Path) {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetDllDirectoryW(lpPathName: *const u16) -> i32;
    }
    let mut wide: Vec<u16> = dir.as_os_str().encode_wide().collect();
    wide.push(0);
    unsafe {
        let _ = SetDllDirectoryW(wide.as_ptr());
    }
}

fn configure_runtime_loader_paths() {
    let dirs = runtime_candidate_dirs()
        .into_iter()
        .filter(|d| d.exists())
        .collect::<Vec<_>>();
    if dirs.is_empty() {
        return;
    }
    #[cfg(windows)]
    {
        // Prefer application bundled DLL directory for this process.
        if let Some(first) = dirs.first() {
            set_windows_dll_directory(first);
        }
    }
    let path_delim = if cfg!(windows) { ';' } else { ':' };
    let existing = std::env::var("PATH").unwrap_or_default();
    let mut merged = String::new();
    for d in &dirs {
        if let Some(s) = d.to_str() {
            if !merged.is_empty() {
                merged.push(path_delim);
            }
            merged.push_str(s);
        }
    }
    if !existing.is_empty() {
        if !merged.is_empty() {
            merged.push(path_delim);
        }
        merged.push_str(&existing);
    }
    std::env::set_var("PATH", merged);
}

fn as_cstring(s: &str) -> std::ffi::CString {
    std::ffi::CString::new(s).unwrap_or_else(|_| std::ffi::CString::new("").expect("empty cstr"))
}

fn normalize_table(st: &SessionState) -> String {
    st.current_table.trim().to_string()
}

fn run_demo(st: &SessionState, extra: &[&str]) -> Result<String, String> {
    configure_runtime_loader_paths();
    let app_bin = resolve_structdb_app_bin()?;
    let mut cmd = Command::new(app_bin);
    if !st.data_dir.trim().is_empty() {
        cmd.arg("--data-dir").arg(&st.data_dir);
        let sd = Path::new(&st.data_dir).join("embed_session");
        cmd.arg("--session-dir").arg(sd.to_string_lossy().as_ref());
    }
    for a in extra {
        cmd.arg(a);
    }
    #[cfg(windows)]
    {
        cmd.creation_flags(0x08000000);
    }
    let out = cmd
        .output()
        .map_err(|e| format!("failed to run structdb_app: {e}"))?;
    let mut all = String::new();
    all.push_str(&String::from_utf8_lossy(&out.stdout));
    all.push_str(&String::from_utf8_lossy(&out.stderr));
    Ok(all)
}

fn run_demo_mdb(st: &SessionState, script_text: &str) -> Result<String, String> {
    let base_dir = if st.data_dir.trim().is_empty() {
        std::env::temp_dir()
    } else {
        PathBuf::from(&st.data_dir)
    };
    if !base_dir.exists() {
        fs::create_dir_all(&base_dir)
            .map_err(|e| format!("failed to create script dir {}: {e}", base_dir.display()))?;
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let script_path = base_dir.join(format!("gui_exec_{}_{}.mdb", std::process::id(), stamp));
    fs::write(&script_path, format!("{script_text}\n"))
        .map_err(|e| format!("failed to write temp mdb {}: {e}", script_path.display()))?;
    let script_path_str = script_path.to_string_lossy().to_string();
    let args = ["--run-mdb", script_path_str.as_str()];
    let result = run_demo(st, &args);
    let _ = fs::remove_file(&script_path);
    result
}

fn parse_use_table(command: &str) -> Option<String> {
    let text = command.trim();
    if text.len() < 6 {
        return None;
    }
    let upper = text.to_ascii_uppercase();
    if !upper.starts_with("USE(") || !text.ends_with(')') {
        return None;
    }
    let inner = &text[4..text.len() - 1];
    let t = inner.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

fn reset_cap_session(api: &CapApi, cap: &mut CapSessionState) {
    if let Some(m) = cap.mdb_session.take() {
        unsafe { (api.mdb_session_destroy)(m as *mut c_void) };
    }
    if let Some(e) = cap.embed.take() {
        unsafe { (api.embed_close)(e as *mut c_void) };
    }
    if let Some(en) = cap.engine.take() {
        unsafe { (api.engine_shutdown)(en as *mut c_void) };
    }
    cap.data_dir.clear();
}

fn cstr_err(buf: &[i8]) -> String {
    let u8s: Vec<u8> = buf
        .iter()
        .copied()
        .take_while(|&b| b != 0)
        .map(|b| b as u8)
        .collect();
    String::from_utf8_lossy(&u8s).to_string()
}

fn ensure_cap_session(api: &CapApi, st: &SessionState, cap: &mut CapSessionState) -> Result<(), String> {
    if cap.engine.is_some() && cap.data_dir != st.data_dir {
        reset_cap_session(api, cap);
    }
    if cap.engine.is_some() && cap.embed.is_some() && cap.mdb_session.is_some() {
        return Ok(());
    }
    let mut err = vec![0_i8; 4096];
    let data_c = as_cstring(st.data_dir.trim());
    const STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK: u32 = 1;
    let exclusive = std::env::var("STRUCTDB_GUI_EXCLUSIVE_DIR_LOCK")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let open_flags = if exclusive {
        STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK
    } else {
        0u32
    };
    let eng = unsafe {
        (api.engine_open_ex)(
            data_c.as_ptr(),
            open_flags,
            err.as_mut_ptr() as *mut c_char,
            err.len(),
        )
    };
    if eng.is_null() {
        let msg = cstr_err(&err);
        return Err(format!("structdb_engine_open_ex failed: {msg}"));
    }
    cap.engine = Some(eng as usize);
    let session_dir = if st.data_dir.trim().is_empty() {
        as_cstring("")
    } else {
        let p = Path::new(&st.data_dir).join("embed_session");
        as_cstring(&p.to_string_lossy())
    };
    let emb = unsafe {
        (api.embed_open)(
            eng,
            session_dir.as_ptr(),
            err.as_mut_ptr() as *mut c_char,
            err.len(),
        )
    };
    if emb.is_null() {
        reset_cap_session(api, cap);
        let msg = cstr_err(&err);
        return Err(format!("structdb_embed_open failed: {msg}"));
    }
    cap.embed = Some(emb as usize);
    let mdb = unsafe { (api.mdb_session_create)() };
    if mdb.is_null() {
        reset_cap_session(api, cap);
        return Err("structdb_mdb_session_create returned null".to_string());
    }
    cap.mdb_session = Some(mdb as usize);
    cap.data_dir = st.data_dir.clone();
    Ok(())
}

fn execute_via_cap_session(
    api: &CapApi,
    st: &SessionState,
    cap: &mut CapSessionState,
    command_line: &str,
) -> Result<String, String> {
    ensure_cap_session(api, st, cap)?;
    let engine = cap.engine.ok_or("no engine")? as *mut c_void;
    let embed = cap.embed.ok_or("no embed")? as *mut c_void;
    let mdb = cap.mdb_session.ok_or("no mdb session")? as *mut c_void;
    let line_c = as_cstring(command_line.trim_end_matches('\n'));
    let mut err = vec![0_i8; 8192];
    let mut lines: Vec<String> = Vec::new();
    let mut repl_flag: i32 = 0;
    let opts = StructdbMdbRunOptions {
        struct_size: STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3,
        reserved_flags: 0,
        fsync_each_batch: 0,
        fsync_each_session_txn_op: 0,
        fail_if_unclosed_txn: 0,
        allow_persist_while_txn_active_experimental: 1,
        log_file_path: std::ptr::null(),
        log_line_cb: Some(structdb_log_line_cb),
        log_user_data: (&mut lines) as *mut Vec<String> as *mut c_void,
        repl_exit_requested_out: &mut repl_flag,
        long_task_control: std::ptr::null_mut(),
    };
    let rc = unsafe {
        (api.mdb_execute_line_ex)(
            engine,
            embed,
            mdb,
            line_c.as_ptr(),
            err.as_mut_ptr() as *mut c_char,
            err.len(),
            &opts,
        )
    };
    if rc != STRUCTDB_CAPI_OK {
        let msg = cstr_err(&err);
        if msg.is_empty() {
            return Err(format!("structdb_mdb_execute_line_ex failed (rc={rc})"));
        }
        return Err(format!("{msg} (rc={rc})"));
    }
    Ok(lines.join("\n"))
}

fn should_fallback_to_demo(_err: &str) -> bool {
    false
}

fn should_stop_script_on_output(output: &str) -> bool {
    if output.contains("[CAPI_ERROR] code=") {
        return true;
    }
    if output.contains("[ERROR]") {
        return true;
    }
    if output.contains("[INSERT] attribute '")
        || output.contains("[UPDATE] attribute '")
        || output.contains("[SETATTR] attribute '")
    {
        return true;
    }
    output.contains("expects ") && output.contains(", got '")
}

fn parse_capi_error_code(output: &str) -> Option<String> {
    for line in output.lines() {
        let t = line.trim();
        if !t.starts_with("[CAPI_ERROR]") {
            continue;
        }
        if let Some(idx) = t.find("code=") {
            let rest = &t[idx + 5..];
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            let code = rest[..end].trim();
            if !code.is_empty() && code != "ok" {
                return Some(code.to_string());
            }
        }
    }
    None
}

fn parse_capi_error_numeric(output: &str) -> Option<i32> {
    for line in output.lines() {
        let t = line.trim();
        if !t.starts_with("[CAPI_ERROR]") {
            continue;
        }
        if let Some(idx) = t.find("numeric=") {
            let rest = &t[idx + "numeric=".len()..];
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            return rest[..end].trim().parse().ok();
        }
    }
    None
}

fn strip_capi_error_line(output: &str) -> String {
    let mut out = String::new();
    for line in output.lines() {
        if line.trim_start().starts_with("[CAPI_ERROR]") {
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(line);
    }
    out
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandExecResult {
    ok: bool,
    output: String,
    error_code: Option<String>,
    /// Stable numeric error code when the C API emits `numeric=` (optional).
    error_code_numeric: Option<i32>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InverseInferResult {
    backward: Option<String>,
    source: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ScriptExecResult {
    ok: bool,
    output: String,
    error_code: Option<String>,
    stop_line: Option<usize>,
    cancelled: bool,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct MdbScriptProgress {
    line_done: usize,
    total_lines: usize,
}

/// Unified long-task progress envelope (MDB script, compaction merge, export, …).
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct LongTaskProgressPayload {
    kind: String,
    status: String,
    units_done: u64,
    units_total: u64,
    bytes_done: u64,
    bytes_total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_id: Option<String>,
}

fn emit_long_task_progress(app: &tauri::AppHandle, payload: LongTaskProgressPayload) {
    let _ = app.emit("long-task-progress", payload.clone());
    if payload.kind == "mdbScript" {
        let _ = app.emit(
            "mdb-script-progress",
            MdbScriptProgress {
                line_done: payload.units_done as usize,
                total_lines: if payload.units_total > 0 {
                    payload.units_total as usize
                } else {
                    payload.units_done as usize
                },
            },
        );
    }
}

fn emit_mdb_script_long_task(app: &tauri::AppHandle, line_done: usize, total_lines: usize, status: &str) {
    emit_long_task_progress(
        app,
        LongTaskProgressPayload {
            kind: "mdbScript".to_string(),
            status: status.to_string(),
            units_done: line_done as u64,
            units_total: total_lines as u64,
            bytes_done: 0,
            bytes_total: 0,
            detail: None,
            task_id: None,
        },
    );
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimedExecResult {
    ok: bool,
    output: String,
    error_code: Option<String>,
    error_code_numeric: Option<i32>,
    elapsed_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StackUndoOp {
    forward: String,
    backward: Option<String>,
    table: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StackUndoUnit {
    unit_id: String,
    txn_id: String,
    savepoint_name: String,
    tables_touched: Vec<String>,
    ops: Vec<StackUndoOp>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StackExecResult {
    ok: bool,
    applied_ops: usize,
    failed_op: Option<String>,
    executed_commands: Vec<String>,
    repair_action: String,
    elapsed_ms: u64,
    message: String,
}

fn make_command_exec_result(out: String) -> CommandExecResult {
    let error_code = parse_capi_error_code(&out);
    let error_code_numeric = parse_capi_error_numeric(&out);
    CommandExecResult {
        ok: error_code.is_none(),
        output: strip_capi_error_line(&out),
        error_code,
        error_code_numeric,
    }
}

fn make_timed_exec_result(out: String, elapsed_ms: u64) -> TimedExecResult {
    let r = make_command_exec_result(out);
    TimedExecResult {
        ok: r.ok,
        output: r.output,
        error_code: r.error_code,
        error_code_numeric: r.error_code_numeric,
        elapsed_ms,
    }
}

fn exec_timed(state: State<'_, AppState>, command: String) -> Result<TimedExecResult, String> {
    let begin = std::time::Instant::now();
    let out = execute_command(state, command)?;
    Ok(make_timed_exec_result(out, begin.elapsed().as_millis() as u64))
}

fn stack_store_file_from_workspace(workspace: &str) -> PathBuf {
    let root = if workspace.trim().is_empty() {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    } else {
        PathBuf::from(workspace)
    };
    root.join(".structdb_gui").join("undo_redo_stack.jsonl")
}

fn stack_store_file_from_state(state: &State<'_, AppState>) -> Result<PathBuf, String> {
    let app = state.inner();
    let st = app.st.lock().map_err(|_| "state lock poisoned".to_string())?;
    Ok(stack_store_file_from_workspace(&st.data_dir))
}

/// Per-op table from the GUI stack; falls back to the sole `tables_touched` when the unit is single-table.
fn effective_stack_op_table(op: &StackUndoOp, unit: &StackUndoUnit) -> Option<String> {
    if let Some(ref t) = op.table {
        let tt = t.trim();
        if !tt.is_empty() {
            return Some(tt.to_string());
        }
    }
    if unit.tables_touched.len() == 1 {
        let t = unit.tables_touched[0].trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    None
}

fn sync_session_table_for_stack_op(state: &State<'_, AppState>, op: &StackUndoOp, unit: &StackUndoUnit) {
    let Some(tt) = effective_stack_op_table(op, unit) else {
        return;
    };
    let app = state.inner();
    if let Ok(mut st) = app.st.lock() {
        st.current_table = tt;
    }
}

fn effective_stack_table_for_rewrite(op: Option<&StackUndoOp>, unit: &StackUndoUnit) -> Option<String> {
    if let Some(o) = op {
        return effective_stack_op_table(o, unit);
    }
    if unit.tables_touched.len() == 1 {
        let t = unit.tables_touched[0].trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    None
}

/// Logical heap row ids are integers; canonicalize so stack commands and JSON maps agree (e.g. `03` → `3`).
fn normalize_heap_row_id_token(raw: &str) -> String {
    let t = raw.trim();
    if t.is_empty() {
        return String::new();
    }
    if let Ok(n) = t.parse::<i64>() {
        return n.to_string();
    }
    t.to_string()
}

fn table_name_matches_for_remap(stack_table: &str, layer_table: &str) -> bool {
    let a = stack_table.trim();
    let b = layer_table.trim();
    if a.is_empty() || b.is_empty() {
        return false;
    }
    if a.eq_ignore_ascii_case(b) {
        return true;
    }
    if let Some((_, ta)) = a.rsplit_once('.') {
        if ta.eq_ignore_ascii_case(b) || b.eq_ignore_ascii_case(ta) {
            return true;
        }
    }
    if let Some((_, tb)) = b.rsplit_once('.') {
        if a.eq_ignore_ascii_case(tb) || tb.eq_ignore_ascii_case(a) {
            return true;
        }
    }
    false
}

fn resolve_stack_row_id_through_remaps(table: &str, id: &str, chain: &[IdRemapLayer]) -> String {
    let mut cur = normalize_heap_row_id_token(id);
    if cur.is_empty() {
        return id.trim().to_string();
    }
    for layer in chain {
        if !table_name_matches_for_remap(table, &layer.table) {
            continue;
        }
        if let Some(next) = layer.map.get(&cur) {
            cur = normalize_heap_row_id_token(next);
        }
    }
    cur
}

fn starts_with_ascii_case_insensitive(hay: &str, needle: &str) -> bool {
    hay.len() >= needle.len() && hay[..needle.len()].eq_ignore_ascii_case(needle)
}

fn rewrite_stack_line_row_ids(line: &str, table: &str, chain: &[IdRemapLayer]) -> String {
    if table.trim().is_empty() || chain.is_empty() {
        return line.to_string();
    }
    let t = line.trim();
    // Longer prefixes first (e.g. DELETEPK before DELETE, FINDPK before FIND).
    const PREFIXES: &[(&str, bool)] = &[
        ("UPDATE(", true),
        ("INSERT(", true),
        ("DELETEPK(", false),
        ("DELETE(", false),
        ("FINDPK(", false),
        ("FIND(", false),
        ("SETATTR(", true),
    ];
    for &(prefix, multi) in PREFIXES {
        if !starts_with_ascii_case_insensitive(t, prefix) {
            continue;
        }
        let Some(inner_end) = t.rfind(')') else {
            return line.to_string();
        };
        let inner = &t[prefix.len()..inner_end];
        let (id_part, rest_comma) = if multi {
            match inner.find(',') {
                Some(i) => (inner[..i].trim(), &inner[i..]),
                None => (inner.trim(), ""),
            }
        } else {
            (inner.trim(), "")
        };
        if id_part.is_empty() {
            return line.to_string();
        }
        let new_id = resolve_stack_row_id_through_remaps(table, id_part, chain);
        let cmd = prefix.trim_end_matches('(');
        return format!("{cmd}({new_id}{rest_comma})");
    }
    line.to_string()
}

fn rewrite_stack_command_line(
    app: &AppState,
    op: Option<&StackUndoOp>,
    unit: &StackUndoUnit,
    line: &str,
) -> String {
    let chain = match app.id_remap_chain.lock() {
        Ok(c) => c,
        Err(_) => return line.to_string(),
    };
    let mut table_opt = effective_stack_table_for_rewrite(op, unit);
    if table_opt.is_none() {
        if let Ok(st) = app.st.lock() {
            let ct = st.current_table.trim().to_string();
            if !ct.is_empty() {
                table_opt = Some(ct);
            }
        }
    }
    let Some(table) = table_opt else {
        return line.to_string();
    };
    let trimmed = line.trim();
    let out = rewrite_stack_line_row_ids(trimmed, &table, &chain);
    // If remap did not apply, try other touched tables (rare qualification mismatch).
    if out == trimmed && !chain.is_empty() {
        let mut tried = std::collections::HashSet::<String>::new();
        tried.insert(table);
        for t in &unit.tables_touched {
            let tt = t.trim().to_string();
            if tt.is_empty() || tried.contains(&tt) {
                continue;
            }
            tried.insert(tt.clone());
            let alt = rewrite_stack_line_row_ids(trimmed, &tt, &chain);
            if alt != trimmed {
                return alt;
            }
        }
    }
    out
}

fn ingest_reorder_map_from_engine_output(app: &AppState, output: &str) {
    for raw in output.lines() {
        let t = raw.trim();
        let Some(json) = t.strip_prefix("[REORDER_MAP_JSON]") else {
            continue;
        };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(json.trim()) else {
            continue;
        };
        let Some(table) = v.get("table").and_then(|x| x.as_str()).map(|s| s.trim().to_string()) else {
            continue;
        };
        if table.is_empty() {
            continue;
        }
        let mut map = HashMap::new();
        if let Some(pairs) = v.get("pairs").and_then(|x| x.as_array()) {
            for p in pairs {
                let Some(arr) = p.as_array() else {
                    continue;
                };
                if arr.len() < 2 {
                    continue;
                }
                let old_s = match &arr[0] {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    _ => continue,
                };
                let new_s = match &arr[1] {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    _ => continue,
                };
                let old_n = normalize_heap_row_id_token(&old_s);
                let new_n = normalize_heap_row_id_token(&new_s);
                if old_n.is_empty() {
                    continue;
                }
                map.insert(old_n, new_n);
            }
        }
        if map.is_empty() {
            continue;
        }
        if let Ok(mut chain) = app.id_remap_chain.lock() {
            chain.push(IdRemapLayer { table, map });
        }
    }
}

fn parse_stack_payload_text(text: &str) -> (serde_json::Value, serde_json::Value, Vec<String>) {
    let mut warnings: Vec<String> = Vec::new();
    let mut undo_units = serde_json::json!([]);
    let mut redo_units = serde_json::json!([]);
    for (i, ln) in text.lines().enumerate() {
        let t = ln.trim();
        if t.is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(t) {
            Ok(v) => {
                undo_units = v.get("undo_units").cloned().unwrap_or(serde_json::json!([]));
                redo_units = v.get("redo_units").cloned().unwrap_or(serde_json::json!([]));
            }
            Err(e) => warnings.push(format!("line {} parse failed: {}", i + 1, e)),
        }
    }
    (undo_units, redo_units, warnings)
}

#[tauri::command]
fn save_stack_units(
    state: State<'_, AppState>,
    undo_units_json: String,
    redo_units_json: String,
) -> Result<(), String> {
    let p = stack_store_file_from_state(&state)?;
    if let Some(parent) = p.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create stack dir {}: {e}", parent.display()))?;
    }
    let id_chain = state
        .inner()
        .id_remap_chain
        .lock()
        .map_err(|_| "id_remap_chain lock poisoned".to_string())?;
    let id_chain_json =
        serde_json::to_value(&*id_chain).map_err(|e| format!("id_remap_chain serialize: {e}"))?;
    let payload = serde_json::json!({
        "schema_version": "structdb.gui.undo_redo_stack.v4",
        "updated_at_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        "undo_units": serde_json::from_str::<serde_json::Value>(&undo_units_json).unwrap_or(serde_json::json!([])),
        "redo_units": serde_json::from_str::<serde_json::Value>(&redo_units_json).unwrap_or(serde_json::json!([])),
        "id_remap_chain": id_chain_json,
    });
    let line = serde_json::to_string(&payload).map_err(|e| format!("stack serialize failed: {e}"))?;
    fs::write(&p, format!("{line}\n")).map_err(|e| format!("failed to write {}: {e}", p.display()))?;
    Ok(())
}

fn last_stack_json_line_object(text: &str) -> Option<serde_json::Value> {
    let mut last = None;
    for ln in text.lines() {
        let t = ln.trim();
        if t.is_empty() {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(t) {
            last = Some(v);
        }
    }
    last
}

#[tauri::command]
fn load_stack_units(state: State<'_, AppState>) -> Result<String, String> {
    let p = stack_store_file_from_state(&state)?;
    if !p.exists() {
        if let Ok(mut c) = state.inner().id_remap_chain.lock() {
            c.clear();
        }
        return Ok("{\"undo_units\":[],\"redo_units\":[],\"id_remap_chain\":[],\"warnings\":[]}".to_string());
    }
    let text = fs::read_to_string(&p).map_err(|e| format!("failed to read {}: {e}", p.display()))?;
    let (undo_units, redo_units, warnings) = parse_stack_payload_text(&text);
    let id_chain: Vec<IdRemapLayer> = last_stack_json_line_object(&text)
        .and_then(|v| v.get("id_remap_chain").cloned())
        .and_then(|x| serde_json::from_value(x).ok())
        .unwrap_or_default();
    let id_chain_json =
        serde_json::to_value(&id_chain).map_err(|e| format!("id_remap_chain response: {e}"))?;
    let meta = last_stack_json_line_object(&text);
    let suspended_redo = meta
        .as_ref()
        .and_then(|v| v.get("suspended_redo_units").cloned())
        .unwrap_or(serde_json::json!([]));
    if let Ok(mut g) = state.inner().id_remap_chain.lock() {
        *g = id_chain;
    }
    Ok(
        serde_json::json!({
            "undo_units": undo_units,
            "redo_units": redo_units,
            "legacy_suspended_redo_units": suspended_redo,
            "id_remap_chain": id_chain_json,
            "warnings": warnings
        })
        .to_string(),
    )
}

fn stack_undo_unit_with_runner<F, B>(unit: &StackUndoUnit, mut before_op: B, mut run: F) -> StackExecResult
where
    F: FnMut(Option<&StackUndoOp>, String) -> Result<CommandExecResult, String>,
    B: FnMut(&StackUndoOp),
{
    let begin = std::time::Instant::now();
    let mut repair_action = "none".to_string();
    let mut failed_op: Option<String> = None;
    let mut applied_ops = 0usize;
    let mut executed_commands: Vec<String> = Vec::new();

    let savepoint = unit.savepoint_name.trim().to_string();
    let is_internal_unit = savepoint.starts_with("__");
    if !savepoint.is_empty() && !is_internal_unit {
        let cmd = format!("ROLLBACK TO SAVEPOINT {savepoint}");
        executed_commands.push(cmd.clone());
        match run(None, cmd.clone()) {
            Ok(r) if r.ok => {
                return StackExecResult {
                    ok: true,
                    applied_ops: 1,
                    failed_op: None,
                    executed_commands,
                    repair_action,
                    elapsed_ms: begin.elapsed().as_millis() as u64,
                    message: format!("rollback to savepoint {}", savepoint),
                }
            }
            _ => {
                failed_op = Some(cmd);
                repair_action = "soft_fail".to_string();
            }
        }
    }

    for op in unit.ops.iter().rev() {
        let Some(backward) = op.backward.as_ref() else {
            failed_op = Some(op.forward.clone());
            repair_action = "soft_fail".to_string();
            continue;
        };
        before_op(op);
        let mut run_line = |line: String| run(Some(op), line);
        match run_compound_command(backward, &mut run_line, &mut executed_commands) {
            Ok(()) => {
                applied_ops += 1;
            }
            Err(failed_cmd) => {
                executed_commands.push("ROLLBACK".to_string());
                let _ = run(Some(op), "ROLLBACK".to_string());
                return StackExecResult {
                    ok: false,
                    applied_ops,
                    failed_op: Some(failed_cmd),
                    executed_commands,
                    repair_action: "hard_fail_rollback".to_string(),
                    elapsed_ms: begin.elapsed().as_millis() as u64,
                    message: "undo fallback failed; rollback issued".to_string(),
                };
            }
        }
    }

    StackExecResult {
        ok: failed_op.is_none(),
        applied_ops,
        failed_op,
        executed_commands,
        repair_action,
        elapsed_ms: begin.elapsed().as_millis() as u64,
        message: "undo unit applied".to_string(),
    }
}

fn stack_redo_unit_with_runner<F, B>(unit: &StackUndoUnit, mut before_op: B, mut run: F) -> StackExecResult
where
    F: FnMut(Option<&StackUndoOp>, String) -> Result<CommandExecResult, String>,
    B: FnMut(&StackUndoOp),
{
    let begin = std::time::Instant::now();
    let mut applied_ops = 0usize;
    let mut executed_commands: Vec<String> = Vec::new();
    for op in &unit.ops {
        if op.forward.trim().is_empty() {
            continue;
        }
        before_op(op);
        let mut run_line = |line: String| run(Some(op), line);
        match run_compound_command(&op.forward, &mut run_line, &mut executed_commands) {
            Ok(()) => {
                applied_ops += 1;
            }
            Err(failed_cmd) => {
                executed_commands.push("ROLLBACK".to_string());
                let _ = run(Some(op), "ROLLBACK".to_string());
                return StackExecResult {
                    ok: false,
                    applied_ops,
                    failed_op: Some(failed_cmd),
                    executed_commands,
                    repair_action: "hard_fail_rollback".to_string(),
                    elapsed_ms: begin.elapsed().as_millis() as u64,
                    message: "redo failed; rollback issued".to_string(),
                };
            }
        }
    }
    StackExecResult {
        ok: true,
        applied_ops,
        failed_op: None,
        executed_commands,
        repair_action: "none".to_string(),
        elapsed_ms: begin.elapsed().as_millis() as u64,
        message: "redo unit applied".to_string(),
    }
}

#[tauri::command]
fn stack_undo_unit(state: State<'_, AppState>, unit: StackUndoUnit) -> Result<StackExecResult, String> {
    Ok(stack_undo_unit_with_runner(
        &unit,
        |op| sync_session_table_for_stack_op(&state, op, &unit),
        |maybe_op, cmd| {
            let app = state.inner();
            let cmd2 = rewrite_stack_command_line(app, maybe_op, &unit, &cmd);
            execute_command_ex(state.clone(), cmd2)
        },
    ))
}

#[tauri::command]
fn stack_redo_unit(state: State<'_, AppState>, unit: StackUndoUnit) -> Result<StackExecResult, String> {
    Ok(stack_redo_unit_with_runner(
        &unit,
        |op| sync_session_table_for_stack_op(&state, op, &unit),
        |maybe_op, cmd| {
            let app = state.inner();
            let cmd2 = rewrite_stack_command_line(app, maybe_op, &unit, &cmd);
            execute_command_ex(state.clone(), cmd2)
        },
    ))
}

#[tauri::command]
fn export_bundle(app: tauri::AppHandle, state: State<'_, AppState>, dest_zip: Option<String>) -> Result<String, String> {
    configure_runtime_loader_paths();
    let inner = state.inner();
    let st = inner.st.lock().expect("state lock");
    emit_long_task_progress(
        &app,
        LongTaskProgressPayload {
            kind: "export".to_string(),
            status: "running".to_string(),
            units_done: 0,
            units_total: 0,
            bytes_done: 0,
            bytes_total: 0,
            detail: Some("export_bundle".to_string()),
            task_id: None,
        },
    );
    let results_dir = resolve_results_dir().unwrap_or_else(|_| PathBuf::from("scripts/results"));
    if !results_dir.exists() {
        fs::create_dir_all(&results_dir)
            .map_err(|e| format!("failed to create results dir {}: {e}", results_dir.display()))?;
    }
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let out_path = if let Some(p) = dest_zip {
        PathBuf::from(p)
    } else {
        results_dir.join(format!("structdb_gui_bundle_{ts}.zip"))
    };
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create bundle dir {}: {e}", parent.display()))?;
    }

    let file = fs::File::create(&out_path)
        .map_err(|e| format!("failed to create {}: {e}", out_path.display()))?;
    let mut zip = zip::ZipWriter::new(file);
    let options = FileOptions::<()>::default().compression_method(zip::CompressionMethod::Deflated);

    let mut export_units_done: u64 = 0;
    let mut add_path = |real_path: &Path, zip_path: &str| -> Result<(), String> {
        if !real_path.exists() {
            return Ok(());
        }
        if real_path.is_dir() {
            return Ok(());
        }
        let data = fs::read(real_path)
            .map_err(|e| format!("failed to read {}: {e}", real_path.display()))?;
        zip.start_file(zip_path.replace('\\', "/"), options)
            .map_err(|e| format!("zip start_file failed: {e}"))?;
        zip.write_all(&data)
            .map_err(|e| format!("zip write failed: {e}"))?;
        export_units_done += 1;
        emit_long_task_progress(
            &app,
            LongTaskProgressPayload {
                kind: "export".to_string(),
                status: "running".to_string(),
                units_done: export_units_done,
                units_total: 0,
                bytes_done: export_units_done,
                bytes_total: 0,
                detail: Some(zip_path.to_string()),
                task_id: None,
            },
        );
        Ok(())
    };

    // 1) results/*
    if results_dir.exists() {
        for entry in WalkDir::new(&results_dir).into_iter().flatten() {
            let p = entry.path();
            if !p.is_file() {
                continue;
            }
            let rel = p
                .strip_prefix(&results_dir)
                .unwrap_or(p)
                .to_string_lossy()
                .to_string();
            let zip_name = format!("results/{}", rel);
            add_path(p, &zip_name)?;
        }
    }

    // 2) workspace WAL + table binaries (best-effort)
    let ws = PathBuf::from(st.data_dir.clone());
    if ws.exists() {
        for pat in ["demodb.wal", "demodb.wal*", "demodb.walsync.conf"].iter() {
            let _ = pat;
        }
        // Minimal: include a few known filenames and common extensions.
        for name in ["demodb.wal", "demodb.walsync.conf"].iter() {
            add_path(&ws.join(name), &format!("workspace/{name}"))?;
        }
        // Include any *.wal* and *.bin in the workspace root.
        if let Ok(rd) = fs::read_dir(&ws) {
            for e in rd.flatten() {
                let p = e.path();
                if !p.is_file() {
                    continue;
                }
                let name = p.file_name().and_then(|s| s.to_str()).unwrap_or_default();
                if name.ends_with(".bin") || name.ends_with(".attr") || name.ends_with(".wal") || name.contains(".wal.") {
                    add_path(&p, &format!("workspace/{name}"))?;
                }
            }
        }
    }

    // 3) runtime artifacts snapshot + dll version
    let artifacts = runtime_artifact_info();
    let dll = dll_info();
    let manifest = serde_json::json!({
        "schema_version": "structdb.gui.bundle_manifest.v1",
        "ts_s": ts,
        "workspace": { "data_dir": st.data_dir, "current_table": st.current_table, "page_size": st.page_size },
        "dll": dll,
        "artifacts": artifacts,
    });
    zip.start_file("manifest.json", options)
        .map_err(|e| format!("zip start_file manifest failed: {e}"))?;
    zip.write_all(serde_json::to_string_pretty(&manifest).unwrap_or_default().as_bytes())
        .map_err(|e| format!("zip write manifest failed: {e}"))?;

    zip.finish().map_err(|e| format!("zip finish failed: {e}"))?;
    emit_long_task_progress(
        &app,
        LongTaskProgressPayload {
            kind: "export".to_string(),
            status: "completed".to_string(),
            units_done: export_units_done,
            units_total: export_units_done,
            bytes_done: export_units_done,
            bytes_total: export_units_done,
            detail: Some(out_path.display().to_string()),
            task_id: None,
        },
    );
    Ok(out_path.display().to_string())
}

fn resolve_structdb_bench_exe() -> Option<PathBuf> {
    for dir in runtime_candidate_dirs() {
        for sub in ["Release", "Debug", ""] {
            let p = if sub.is_empty() {
                dir.join("structdb_bench.exe")
            } else {
                dir.join("benchmarks").join(sub).join("structdb_bench.exe")
            };
            if p.is_file() {
                return Some(p);
            }
        }
        let p2 = dir.join("structdb_bench.exe");
        if p2.is_file() {
            return Some(p2);
        }
    }
  None
}

#[tauri::command]
fn run_structdb_bench(app: tauri::AppHandle, state: State<'_, AppState>) -> Result<String, String> {
    let bench = resolve_structdb_bench_exe()
        .ok_or_else(|| "structdb_bench.exe not found (build with -DSTRUCTDB_BUILD_BENCHMARKS=ON)".to_string())?;
    emit_long_task_progress(
        &app,
        LongTaskProgressPayload {
            kind: "bench".to_string(),
            status: "running".to_string(),
            units_done: 0,
            units_total: 1,
            bytes_done: 0,
            bytes_total: 0,
            detail: Some(bench.display().to_string()),
            task_id: None,
        },
    );
    state.inner().script_cancel.store(false, Ordering::Release);
    let output = Command::new(&bench)
        .arg("--benchmark_min_time=0.1s")
        .output()
        .map_err(|e| format!("failed to run {}: {e}", bench.display()))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let combined = if stderr.is_empty() {
        stdout
    } else {
        format!("{stdout}\n{stderr}")
    };
    let status = if output.status.success() { "completed" } else { "failed" };
    emit_long_task_progress(
        &app,
        LongTaskProgressPayload {
            kind: "bench".to_string(),
            status: status.to_string(),
            units_done: 1,
            units_total: 1,
            bytes_done: 0,
            bytes_total: 0,
            detail: Some(bench.display().to_string()),
            task_id: None,
        },
    );
    if !output.status.success() {
        return Err(format!("structdb_bench exited with {:?}\n{combined}", output.status.code()));
    }
    Ok(combined)
}
#[tauri::command]
fn txn_begin(state: State<'_, AppState>, _table: Option<String>) -> Result<TimedExecResult, String> {
    // StructDB MDB accepts exactly `BEGIN` (no table suffix); current USE 表由会话状态决定。
    exec_timed(state, "BEGIN".to_string())
}

#[tauri::command]
fn txn_commit(state: State<'_, AppState>) -> Result<TimedExecResult, String> {
    exec_timed(state, "COMMIT".to_string())
}

#[tauri::command]
fn txn_rollback(state: State<'_, AppState>) -> Result<TimedExecResult, String> {
    exec_timed(state, "ROLLBACK".to_string())
}

#[tauri::command]
fn txn_savepoint(state: State<'_, AppState>, name: String) -> Result<TimedExecResult, String> {
    let sp = name.trim();
    if sp.is_empty() {
        return Err("empty savepoint".to_string());
    }
    exec_timed(state, format!("SAVEPOINT {sp}"))
}

#[tauri::command]
fn txn_rollback_to(state: State<'_, AppState>, name: String) -> Result<TimedExecResult, String> {
    let sp = name.trim();
    if sp.is_empty() {
        return Err("empty savepoint".to_string());
    }
    exec_timed(state, format!("ROLLBACK TO SAVEPOINT {sp}"))
}

#[tauri::command]
fn txn_release_savepoint(state: State<'_, AppState>, name: String) -> Result<TimedExecResult, String> {
    let sp = name.trim();
    if sp.is_empty() {
        return Err("empty savepoint".to_string());
    }
    exec_timed(state, format!("RELEASE SAVEPOINT {sp}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        make_command_exec_result, parse_capi_error_code, parse_stack_payload_text, parse_table_names_from_show_tables_output,
        should_stop_script_on_output, stack_redo_unit_with_runner, stack_undo_unit_with_runner, strip_capi_error_line,
        IdRemapLayer, StackUndoOp, StackUndoUnit, rewrite_stack_line_row_ids,
    };

    #[test]
    fn parse_show_tables_output_lines() {
        let out = "[SESSION] ok\n[TABLE] hr.employees\n[tABLE] orders\n";
        let v = parse_table_names_from_show_tables_output(out);
        assert_eq!(v, vec!["hr.employees".to_string(), "orders".to_string()]);
    }

    #[test]
    fn stop_on_insert_type_mismatch() {
        let out = "[INSERT] attribute 'age' expects int, got 'a'\n";
        assert!(should_stop_script_on_output(out));
    }

    #[test]
    fn stop_on_generic_error_line() {
        let out = "[ERROR] command failed: some failure\n";
        assert!(should_stop_script_on_output(out));
    }

    #[test]
    fn do_not_stop_on_success_output() {
        let out = "[INSERT] ok: table='users' now has 1 rows.\n";
        assert!(!should_stop_script_on_output(out));
    }

    #[test]
    fn parse_capi_error_code_from_prefixed_output() {
        let out = "[CAPI_ERROR] code=execution_failed numeric=3\n[UPDATE] attribute 'age' expects int, got 'a'\n";
        assert_eq!(parse_capi_error_code(out).as_deref(), Some("execution_failed"));
    }

    #[test]
    fn strip_capi_error_line_keeps_business_output() {
        let out = "[CAPI_ERROR] code=execution_failed numeric=3\n[UPDATE] attribute 'age' expects int, got 'a'\n";
        let cleaned = strip_capi_error_line(out);
        assert!(!cleaned.contains("[CAPI_ERROR]"));
        assert!(cleaned.contains("[UPDATE] attribute 'age' expects int, got 'a'"));
    }

    #[test]
    fn parse_script_stop_line_from_output() {
        let out = "[UPDATE] attribute 'age' expects int, got 'a'\n[SCRIPT] stopped at line 5 due to command error.\n";
        let stop_line = out.lines().find_map(|line| {
            let t = line.trim();
            let prefix = "[SCRIPT] stopped at line ";
            if !t.starts_with(prefix) {
                return None;
            }
            let rest = &t[prefix.len()..];
            let n_text = rest.split_whitespace().next()?;
            n_text.parse::<usize>().ok()
        });
        assert_eq!(stop_line, Some(5));
    }

    #[test]
    fn command_exec_result_uses_error_code_and_strips_prefix() {
        let out = "[CAPI_ERROR] code=execution_failed numeric=3\n[UPDATE] attribute 'age' expects int, got 'a'\n".to_string();
        let r = make_command_exec_result(out);
        assert!(!r.ok);
        assert_eq!(r.error_code.as_deref(), Some("execution_failed"));
        assert_eq!(r.error_code_numeric, Some(3));
        assert!(!r.output.contains("[CAPI_ERROR]"));
        assert!(r.output.contains("[UPDATE] attribute 'age' expects int, got 'a'"));
    }

    #[test]
    fn command_exec_result_ok_without_error_prefix() {
        let out = "[INSERT] ok: table='users' now has 1 rows.\n".to_string();
        let r = make_command_exec_result(out);
        assert!(r.ok);
        assert!(r.error_code.is_none());
        assert!(r.error_code_numeric.is_none());
        assert!(r.output.contains("[INSERT] ok: table='users' now has 1 rows."));
    }

    #[test]
    fn remap_chain_rewrites_row_ids_for_stack_commands() {
        let mut m = HashMap::new();
        m.insert("5".to_string(), "1".to_string());
        m.insert("7".to_string(), "2".to_string());
        let chain = vec![IdRemapLayer {
            table: "users".to_string(),
            map: m,
        }];
        assert_eq!(
            rewrite_stack_line_row_ids("DELETE(5)", "users", &chain),
            "DELETE(1)"
        );
        assert_eq!(
            rewrite_stack_line_row_ids("UPDATE(7,name,x)", "users", &chain),
            "UPDATE(2,name,x)"
        );
        let mut m1 = HashMap::new();
        m1.insert("5".to_string(), "1".to_string());
        let mut m2 = HashMap::new();
        m2.insert("1".to_string(), "10".to_string());
        let chain2 = vec![
            IdRemapLayer {
                table: "users".to_string(),
                map: m1,
            },
            IdRemapLayer {
                table: "users".to_string(),
                map: m2,
            },
        ];
        assert_eq!(
            rewrite_stack_line_row_ids("DELETE(5)", "users", &chain2),
            "DELETE(10)"
        );
        assert_eq!(
            rewrite_stack_line_row_ids("DELETEPK(5)", "users", &chain),
            "DELETEPK(1)"
        );
        assert_eq!(
            rewrite_stack_line_row_ids("FINDPK(7)", "users", &chain),
            "FINDPK(2)"
        );
        // DELETEPK must not match the shorter DELETE( prefix.
        assert_eq!(
            rewrite_stack_line_row_ids("DELETEPK(9)", "users", &chain),
            "DELETEPK(9)"
        );
        let mut m3 = HashMap::new();
        m3.insert("3".to_string(), "2".to_string());
        let ch3 = vec![IdRemapLayer {
            table: "users".to_string(),
            map: m3,
        }];
        assert_eq!(
            rewrite_stack_line_row_ids("UPDATE(03,name,x)", "users", &ch3),
            "UPDATE(2,name,x)"
        );
    }

    #[test]
    fn parse_stack_payload_tolerates_bad_lines() {
        let text = r#"{"undo_units":[{"unitId":"u1"}],"redo_units":[]}
not-json
{"undo_units":[{"unitId":"u2"}],"redo_units":[{"unitId":"r1"}]}
"#;
        let (undo_units, redo_units, warnings) = parse_stack_payload_text(text);
        assert!(warnings.len() >= 1);
        assert_eq!(undo_units.as_array().map(|v| v.len()).unwrap_or(0), 1);
        assert_eq!(redo_units.as_array().map(|v| v.len()).unwrap_or(0), 1);
    }

    #[test]
    fn stack_undo_runner_prefers_savepoint() {
        let unit = StackUndoUnit {
            unit_id: "u1".to_string(),
            txn_id: "t1".to_string(),
            savepoint_name: "sp1".to_string(),
            tables_touched: vec!["users".to_string()],
            ops: vec![StackUndoOp {
                forward: "UPDATE(1,A,2)".to_string(),
                backward: Some("UPDATE(1,A,1)".to_string()),
                table: Some("users".to_string()),
            }],
        };
        let mut called: Vec<String> = Vec::new();
        let r = stack_undo_unit_with_runner(&unit, |_| {}, |_op, cmd| {
            called.push(cmd.clone());
            Ok(make_command_exec_result("[OK]".to_string()))
        });
        assert!(r.ok);
        assert_eq!(
            called.first().map(|s| s.as_str()),
            Some("ROLLBACK TO SAVEPOINT sp1")
        );
    }

    #[test]
    fn stack_undo_runner_skips_internal_single_savepoint_rollback_probe() {
        let unit = StackUndoUnit {
            unit_id: "u_single".to_string(),
            txn_id: "t_single".to_string(),
            savepoint_name: "__single__".to_string(),
            tables_touched: vec!["users".to_string()],
            ops: vec![StackUndoOp {
                forward: "INSERT(1)".to_string(),
                backward: Some("DELETE(1)".to_string()),
                table: Some("users".to_string()),
            }],
        };
        let mut called: Vec<String> = Vec::new();
        let r = stack_undo_unit_with_runner(&unit, |_| {}, |_op, cmd| {
            called.push(cmd.clone());
            Ok(make_command_exec_result("[OK]".to_string()))
        });
        assert!(r.ok);
        assert_eq!(r.repair_action, "none");
        assert_eq!(r.applied_ops, 1);
        assert_eq!(called, vec!["DELETE(1)".to_string()]);
    }

    #[test]
    fn stack_redo_runner_hard_fail_rollbacks() {
        let unit = StackUndoUnit {
            unit_id: "u2".to_string(),
            txn_id: "t2".to_string(),
            savepoint_name: "__commit__".to_string(),
            tables_touched: vec!["users".to_string()],
            ops: vec![
                StackUndoOp {
                    forward: "INSERT(1,A,1)".to_string(),
                    backward: Some("DELETE(1)".to_string()),
                    table: Some("users".to_string()),
                },
                StackUndoOp {
                    forward: "UPDATE(1,A,a)".to_string(),
                    backward: Some("UPDATE(1,A,1)".to_string()),
                    table: Some("users".to_string()),
                },
            ],
        };
        let mut idx = 0usize;
        let r = stack_redo_unit_with_runner(&unit, |_| {}, |_op, cmd| {
            idx += 1;
            if idx == 2 {
                return Ok(make_command_exec_result(
                    "[CAPI_ERROR] code=execution_failed numeric=3\n[ERROR] fail".to_string(),
                ));
            }
            if cmd == "ROLLBACK" {
                return Ok(make_command_exec_result("[OK]".to_string()));
            }
            Ok(make_command_exec_result("[OK]".to_string()))
        });
        assert!(!r.ok);
        assert_eq!(r.repair_action, "hard_fail_rollback");
        assert!(r.failed_op.unwrap_or_default().contains("UPDATE"));
    }
}

fn parse_list_tables_from_fs(dir: &str) -> Vec<String> {
    let mut out = Vec::new();
    if dir.is_empty() {
        return out;
    }
    let p = Path::new(dir);
    if !p.exists() {
        return out;
    }
    if let Ok(rd) = fs::read_dir(p) {
        for ent in rd.flatten() {
            let path = ent.path();
            if path.extension() == Some(OsStr::new("bin")) {
                let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or_default();
                if !stem.ends_with("_log") && stem != "demo_log" {
                    out.push(stem.to_string());
                }
            }
        }
    }
    out.sort();
    out
}

/// Parses `SHOW TABLES` / `LIST TABLES` log lines: `[TABLE] name`.
fn parse_table_names_from_show_tables_output(output: &str) -> Vec<String> {
    const PFX: &str = "[TABLE]";
    let mut out = Vec::new();
    for line in output.lines() {
        let t = line.trim();
        if t.len() > PFX.len() && t[..PFX.len()].eq_ignore_ascii_case(PFX) {
            let name = t[PFX.len()..].trim().to_string();
            if !name.is_empty() {
                out.push(name);
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

fn parse_page_output(raw: &str) -> PageResult {
    let mut headers = Vec::new();
    let mut rows = Vec::new();

    // Only parse the last PAGE section to avoid picking up any other box drawings
    // from history / interactive banners.
    let mut page_start_idx: Option<usize> = None;
    for (i, line) in raw.lines().enumerate() {
        if line.contains("[PAGE] table=") {
            page_start_idx = Some(i);
        }
    }

    let lines: Vec<&str> = raw.lines().collect();
    let start = page_start_idx.unwrap_or(0);
    let mut saw_top_border = false;
    let mut in_table = false;

    for line in lines.iter().skip(start) {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        // Detect the actual table borders for PAGE rendering.
        if t.starts_with('┌') && t.ends_with('┐') {
            saw_top_border = true;
            in_table = true;
            continue;
        }
        if !saw_top_border {
            continue;
        }
        if in_table && t.starts_with('└') && t.ends_with('┘') {
            break;
        }
        if in_table && (t.starts_with('├') && t.ends_with('┤')) {
            continue;
        }
        if in_table
            && ((t.starts_with('|') && t.ends_with('|'))
                || (t.starts_with('│') && t.ends_with('│')))
        {
            let cols: Vec<String> = t
                .trim_matches(|c| c == '|' || c == '│')
                .split(|c| c == '|' || c == '│')
                .map(|s| s.trim().to_string())
                .collect();
            if headers.is_empty() {
                headers = cols;
            } else {
                rows.push(cols);
            }
        }
    }

    if headers.is_empty() {
        headers = vec!["raw".to_string()];
        rows = raw.lines().map(|v| vec![v.to_string()]).collect();
    }
    PageResult { headers, columns: Vec::new(), rows, raw: raw.to_string() }
}

fn parse_page_json_output(raw: &str) -> Option<PageResult> {
    for line in raw.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix("[PAGE_JSON]") {
            let json_text = rest.trim();
            let parsed = serde_json::from_str::<PageJsonResult>(json_text).ok()?;
            return Some(PageResult {
                headers: parsed.headers,
                columns: parsed.columns.unwrap_or_default(),
                rows: parsed.rows,
                raw: raw.to_string(),
            });
        }
    }
    None
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ColumnMeta {
    name: String,
    ty: String,
}

fn schema_columns_from_attr(st: &SessionState) -> Vec<ColumnMeta> {
    let mut cols = vec![ColumnMeta { name: "id".to_string(), ty: "int".to_string() }];
    let table = normalize_table(st);
    if st.data_dir.trim().is_empty() || table.trim().is_empty() {
        return cols;
    }
    let attr_path = Path::new(&st.data_dir).join(format!("{table}.attr"));
    let Ok(content) = fs::read_to_string(attr_path) else {
        return cols;
    };
    for line in content.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with("PRIMARY_KEY:") || t.starts_with("HEAP_FORMAT:") {
            continue;
        }
        if let Some((name, tp)) = t.split_once(':') {
            let col = name.trim();
            let ty = tp.trim();
            if !col.is_empty() && col != "id" {
                cols.push(ColumnMeta { name: col.to_string(), ty: ty.to_string() });
            }
        }
    }
    cols
}

/// Parses `SHOW ATTR` log lines: `[ATTR] table=...` then `  col:type`.
fn schema_columns_from_show_attr_output(text: &str) -> Vec<ColumnMeta> {
    let mut out = Vec::new();
    let mut capture = false;
    for line in text.lines() {
        let t = line.trim();
        if t.starts_with("[ATTR] table=") {
            capture = true;
            continue;
        }
        if !capture {
            continue;
        }
        if t.is_empty() {
            continue;
        }
        if t.starts_with('[') {
            break;
        }
        if let Some((raw_name, ty)) = t.split_once(':') {
            let name = raw_name.trim();
            let ty = ty.trim();
            if !name.is_empty() && !ty.is_empty() {
                out.push(ColumnMeta {
                    name: name.to_string(),
                    ty: ty.to_string(),
                });
            }
        }
    }
    out
}

/// Heap 行 id 在 MDB 中为 INSERT 首格；列元数据以 `SHOW ATTR` 为准。合并为 UI 网格列序：先 `id`，再属性列（跳过重复 `id`）。
fn merge_logical_row_id_with_attr_columns(parsed: Vec<ColumnMeta>) -> Vec<ColumnMeta> {
    let mut out = vec![ColumnMeta {
        name: "id".to_string(),
        ty: "int".to_string(),
    }];
    for c in parsed {
        if c.name.eq_ignore_ascii_case("id") {
            continue;
        }
        out.push(c);
    }
    out
}

/// 优先读工作区遗留 `{table}.attr`；否则在已加载 C API 时用同会话 `SHOW ATTR` 解析（StructDB 默认仅存 KV，无侧车 `.attr`）。
fn schema_columns_for_ui(app: &AppState, st: &SessionState) -> Vec<ColumnMeta> {
    let file_cols = schema_columns_from_attr(st);
    if file_cols.len() > 1 {
        return file_cols;
    }
    if let Ok(api) = load_cap_api() {
        if let Ok(mut cap) = app.cap.lock() {
            let tbl = normalize_table(st);
            if !tbl.is_empty() {
                let _ = execute_via_cap_session(api, st, &mut cap, &format!("USE({tbl})"));
            }
            if let Ok(out) = execute_via_cap_session(api, st, &mut cap, "SHOW ATTR") {
                let parsed = schema_columns_from_show_attr_output(&out);
                if !parsed.is_empty() {
                    return merge_logical_row_id_with_attr_columns(parsed);
                }
            }
        }
    }
    file_cols
}

fn schema_headers_for_ui(app: &AppState, st: &SessionState) -> Vec<String> {
    schema_columns_for_ui(app, st)
        .into_iter()
        .map(|c| c.name)
        .collect()
}

/// 与 `execute_command` 相同 MDB 会话上执行 `PAGE_JSON`（逻辑表分页；GUI 侧强制 `page_size` 上限）。
fn query_page_via_cap_session(
    api: &CapApi,
    st: &SessionState,
    cap: &mut CapSessionState,
    page_no: usize,
    page_size: usize,
    order_key: &str,
    descending: bool,
) -> Result<PageResult, String> {
    let tbl = normalize_table(st);
    if tbl.is_empty() {
        return Err("no table selected".to_string());
    }
    execute_via_cap_session(api, st, cap, &format!("USE({tbl})"))?;
    let page = page_no.max(1);
    let psz = page_size.max(1).min(MDB_PAGE_JSON_MAX_PAGE_SIZE);
    let order = if order_key.trim().is_empty() {
        "id"
    } else {
        order_key.trim()
    };
    let dir = if descending { "desc" } else { "asc" };
    let cmd = format!("PAGE_JSON({page},{psz},{order},{dir})");
    let out = execute_via_cap_session(api, st, cap, &cmd)?;
    let mut pr = parse_page_json_output(&out).ok_or_else(|| {
        format!(
            "PAGE_JSON: missing or invalid [PAGE_JSON] line (output len={})",
            out.len()
        )
    })?;
    if pr.columns.is_empty() || pr.headers.is_empty() {
        let show_out = execute_via_cap_session(api, st, cap, "SHOW ATTR")?;
        let cols = merge_logical_row_id_with_attr_columns(schema_columns_from_show_attr_output(&show_out));
        if pr.columns.is_empty() && !cols.is_empty() {
            pr.columns = cols.clone();
        }
        if pr.headers.is_empty() && !cols.is_empty() {
            pr.headers = std::iter::once("#".to_string())
                .chain(cols.iter().map(|c| c.name.clone()))
                .collect();
        }
    }
    Ok(PageResult {
        headers: pr.headers,
        columns: pr.columns,
        rows: pr.rows,
        raw: out,
    })
}

#[tauri::command]
fn get_state(state: State<'_, AppState>) -> UiState {
    let app = state.inner();
    let st = app.st.lock().expect("state lock");
    UiState {
        data_dir: st.data_dir.clone(),
        current_table: st.current_table.clone(),
        page_size: if st.page_size == 0 { 12 } else { st.page_size },
    }
}

#[tauri::command]
fn set_workspace(state: State<'_, AppState>, data_dir: String) {
    let api = load_cap_api().ok();
    let app = state.inner();
    let mut st = app.st.lock().expect("state lock");
    let mut cap = app.cap.lock().expect("cap lock");
    st.data_dir = data_dir;
    if let Ok(mut chain) = app.id_remap_chain.lock() {
        chain.clear();
    }
    if let Some(api) = api {
        reset_cap_session(api, &mut cap);
    }
}

#[tauri::command]
fn set_current_table(state: State<'_, AppState>, table: String) {
    let app = state.inner();
    let mut st = app.st.lock().expect("state lock");
    st.current_table = table;
}

#[tauri::command]
fn list_tables(state: State<'_, AppState>) -> Vec<String> {
    let app = state.inner();
    let st = app.st.lock().expect("state lock");
    let mut names = parse_list_tables_from_fs(&st.data_dir);
    if let Ok(api) = load_cap_api() {
        if let Ok(mut cap) = app.cap.lock() {
            if let Ok(out) = execute_via_cap_session(api, &st, &mut cap, "SHOW TABLES") {
                for t in parse_table_names_from_show_tables_output(&out) {
                    if !names.iter().any(|x| x == &t) {
                        names.push(t);
                    }
                }
            }
        }
    }
    names.sort();
    names.dedup();
    names
}

fn dispatch_mdb_command(app_state: &AppState, command: String) -> Result<String, String> {
    let out = {
        let mut st = app_state.st.lock().expect("state lock");
        // Prefer DLL persistent session so txn BEGIN/ROLLBACK works.
        let out_inner = if let Ok(api) = load_cap_api() {
            let mut cap = app_state.cap.lock().expect("cap lock");
            match execute_via_cap_session(api, &st, &mut cap, &command) {
                Ok(s) => s,
                Err(e) => {
                    if should_fallback_to_demo(&e) {
                        run_demo_mdb(&st, &command)?
                    } else {
                        e
                    }
                }
            }
        } else {
            run_demo_mdb(&st, &command)?
        };
        // Persist selected table across command invocations.
        if let Some(t) = parse_use_table(&command) {
            st.current_table = t;
        }
        out_inner
    };
    let cmd_trim = command.trim();
    let cmd_up = cmd_trim.to_ascii_uppercase();
    if cmd_up.starts_with("CONFIRM_REORDER(") || cmd_up == "CONFIRM_REORDER" {
        ingest_reorder_map_from_engine_output(app_state, &out);
    }
    Ok(out)
}

#[tauri::command]
fn execute_command(state: State<'_, AppState>, command: String) -> Result<String, String> {
    dispatch_mdb_command(state.inner(), command)
}

#[tauri::command]
fn execute_command_ex(state: State<'_, AppState>, command: String) -> Result<CommandExecResult, String> {
    let out = dispatch_mdb_command(state.inner(), command)?;
    Ok(make_command_exec_result(out))
}

fn parse_paren_args(command: &str) -> Option<Vec<String>> {
    let l = command.find('(')?;
    let r = command.rfind(')')?;
    if r <= l {
        return None;
    }
    let inner = &command[(l + 1)..r];
    let args = inner
        .split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>();
    Some(args)
}

fn parse_create_table_name(command: &str) -> Option<String> {
    let args = parse_paren_args(command)?;
    let name = args.first()?.trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn parse_drop_table_name(command: &str) -> Option<String> {
    let t = command.trim();
    let up = t.to_ascii_uppercase();
    if up.starts_with("DROP TABLE(") {
        let args = parse_paren_args(t)?;
        let name = args.first()?.trim();
        if name.is_empty() {
            return None;
        }
        return Some(name.to_string());
    }
    if up.starts_with("DROP TABLE ") {
        let name = t[10..].trim();
        if name.is_empty() {
            return None;
        }
        return Some(name.to_string());
    }
    None
}

fn parse_show_attr_order(output: &str) -> Vec<String> {
    let mut out = Vec::new();
    for raw in output.lines() {
        let t = raw.trim();
        if t.is_empty() || t.starts_with('[') || t.starts_with("(no DEFATTR)") {
            continue;
        }
        if let Some((name, _)) = t.split_once(':') {
            let n = name.trim();
            if !n.is_empty() {
                out.push(n.to_string());
            }
        }
    }
    if out.is_empty() || out[0] != "id" {
        out.insert(0, "id".to_string());
    }
    out
}

fn parse_show_attr_entries(output: &str) -> Vec<(String, String, bool)> {
    let mut out = Vec::new();
    for raw in output.lines() {
        let t = raw.trim();
        if t.is_empty() || t.starts_with('[') || t.starts_with("(no DEFATTR)") {
            continue;
        }
        if let Some((name_raw, rest_raw)) = t.split_once(':') {
            let name = name_raw.trim().to_string();
            if name.is_empty() {
                continue;
            }
            let rest = rest_raw.trim();
            let ty = rest.split_whitespace().next().unwrap_or("").trim().to_string();
            if ty.is_empty() {
                continue;
            }
            let is_pk = rest.contains("[PK]");
            out.push((name, ty, is_pk));
        }
    }
    out
}

fn parse_find_row_map(output: &str, id: &str) -> Option<HashMap<String, String>> {
    let mut line_hit: Option<String> = None;
    for raw in output.lines().rev() {
        let t = raw.trim();
        if !t.starts_with("[FIND] ") {
            continue;
        }
        if t.contains(" not found") || t.contains("lsm_tombstone") {
            continue;
        }
        if !t.contains(&format!("id={id}")) {
            continue;
        }
        line_hit = Some(t.to_string());
        break;
    }
    let line = line_hit?;
    let mut kv = HashMap::new();
    for token in line.split_whitespace() {
        if let Some((k, v)) = token.split_once('=') {
            let key = k.trim().trim_matches(|c: char| c == '[' || c == ']');
            let val = v.trim().trim_matches(|c: char| c == '[' || c == ']');
            if !key.is_empty() {
                kv.insert(key.to_string(), val.to_string());
            }
        }
    }
    if kv.get("id").map(|x| x.as_str()) == Some(id) {
        Some(kv)
    } else {
        None
    }
}

fn find_row_map_by_id_via_query_page(
    state: State<'_, AppState>,
    id: &str,
    max_pages: usize,
    page_size: usize,
) -> Option<HashMap<String, String>> {
    if id.trim().is_empty() {
        return None;
    }
    let psz = page_size.max(1).min(MDB_PAGE_JSON_MAX_PAGE_SIZE);
    let mut page_no = 1usize;
    for _ in 0..max_pages {
        let page = query_page(state.clone(), page_no, psz, "id".to_string(), false).ok()?;
        if page.rows.is_empty() {
            break;
        }
        let id_idx = page
            .headers
            .iter()
            .position(|h| h.trim().eq_ignore_ascii_case("id"))?;
        for row in &page.rows {
            if row.len() <= id_idx {
                continue;
            }
            if row[id_idx].trim() != id {
                continue;
            }
            let mut kv = HashMap::new();
            for (i, h) in page.headers.iter().enumerate() {
                let key = h.trim();
                if key.is_empty() || key == "#" {
                    continue;
                }
                let val = row.get(i).cloned().unwrap_or_default();
                kv.insert(key.to_string(), val);
            }
            if kv.get("id").map(|x| x.trim() == id).unwrap_or(false) {
                return Some(kv);
            }
        }
        if page.rows.len() < psz {
            break;
        }
        page_no += 1;
    }
    None
}

fn run_compound_command<F>(
    cmd_text: &str,
    run: &mut F,
    executed_commands: &mut Vec<String>,
) -> Result<(), String>
where
    F: FnMut(String) -> Result<CommandExecResult, String>,
{
    let parts: Vec<String> = cmd_text
        .lines()
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .map(|x| x.to_string())
        .collect();
    if parts.is_empty() {
        return Ok(());
    }
    for part in parts {
        executed_commands.push(part.clone());
        match run(part.clone()) {
            Ok(r) if r.ok => {}
            _ => return Err(part),
        }
    }
    Ok(())
}

fn parse_primary_key_from_show_output(output: &str) -> Option<String> {
    for raw in output.lines().rev() {
        let t = raw.trim();
        if !t.starts_with("[KEY] ") {
            continue;
        }
        if let Some(idx) = t.find("primary_key=") {
            let v = t[(idx + "primary_key=".len())..].trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

fn build_drop_table_snapshot_inverse(state: State<'_, AppState>, table_name: &str) -> Option<(String, usize, bool)> {
    let target = table_name.trim();
    if target.is_empty() {
        return None;
    }
    let prev_table = {
        let app = state.inner();
        let st = app.st.lock().ok()?;
        st.current_table.trim().to_string()
    };
    let switched = if prev_table != target {
        execute_command(state.clone(), format!("USE({target})")).ok()?;
        true
    } else {
        false
    };

    let mut lines: Vec<String> = vec![format!("CREATE TABLE({target})")];
    let show_out = execute_command(state.clone(), "SHOW ATTR".to_string()).ok()?;
    let entries = parse_show_attr_entries(&show_out);
    if entries.is_empty() {
        if switched {
            let app = state.inner();
            if let Ok(mut st) = app.st.lock() {
                st.current_table = prev_table;
            }
        }
        return Some((lines.join("\n"), 0, false));
    }
    let non_id_defs = entries
        .iter()
        .filter(|(name, _, _)| name != "id")
        .map(|(name, ty, _)| format!("{name}:{ty}"))
        .collect::<Vec<_>>();
    if !non_id_defs.is_empty() {
        lines.push(format!("DEFATTR({})", non_id_defs.join(",")));
    }
    let pk = entries
        .iter()
        .find_map(|(name, _, is_pk)| if *is_pk { Some(name.clone()) } else { None })
        .unwrap_or_else(|| "id".to_string());
    if pk != "id" {
        lines.push(format!("SET PRIMARY KEY({pk})"));
    }

    let mut page_no = 1usize;
    let page_size = 200usize;
    let max_rows: usize = std::env::var("NEWDB_GUI_DROP_SNAPSHOT_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2000);
    let mut snapshot_rows = 0usize;
    let mut truncated = false;
    loop {
        let page = query_page(state.clone(), page_no, page_size, "id".to_string(), false).ok()?;
        if page.rows.is_empty() {
            break;
        }
        let id_idx = page
            .headers
            .iter()
            .position(|h| h.trim().eq_ignore_ascii_case("id"));
        let Some(id_idx) = id_idx else {
            break;
        };
        for row in &page.rows {
            if row.len() <= id_idx {
                continue;
            }
            if snapshot_rows >= max_rows {
                truncated = true;
                break;
            }
            let mut vals: Vec<String> = Vec::new();
            vals.push(row[id_idx].clone());
            for (name, _, _) in &entries {
                let idx = page
                    .headers
                    .iter()
                    .position(|h| h.trim().eq_ignore_ascii_case(name.as_str()));
                let cell = idx
                    .and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or_default();
                vals.push(cell);
            }
            lines.push(format!("INSERT({})", vals.join(",")));
            snapshot_rows += 1;
        }
        if truncated {
            break;
        }
        if page.rows.len() < page_size {
            break;
        }
        page_no += 1;
    }

    if switched {
        let app = state.inner();
        if let Ok(mut st) = app.st.lock() {
            st.current_table = prev_table;
        }
    }
    Some((lines.join("\n"), snapshot_rows, truncated))
}

fn row_map_lookup_ci(row_map: &HashMap<String, String>, key: &str) -> Option<String> {
    if let Some(v) = row_map.get(key) {
        return Some(v.clone());
    }
    for (k, v) in row_map {
        if k.eq_ignore_ascii_case(key) {
            return Some(v.clone());
        }
    }
    None
}

/// `DELATTR` 逆向：在命令**尚未执行**时调用，可读取被删列与各行的当前值。
/// 生成 **`ADDATTR(引擎 schema 下标,规范名:类型)`**（下标与 `SHOW ATTR` / 引擎列序一致，含 `id` 列）以及按行 **`UPDATE(行键, 全列值…)`**（与引擎 `1+列数` 元数一致）。
/// 行数超过 `NEWDB_GUI_DROP_SNAPSHOT_MAX_ROWS` 时放弃逆向，避免不完整恢复。
fn build_delattr_inverse_compound(state: State<'_, AppState>, dropped: &str) -> Option<(String, String)> {
    let dropped = dropped.trim();
    if dropped.is_empty() || dropped.eq_ignore_ascii_case("id") {
        return None;
    }
    let show_out = execute_command(state.clone(), "SHOW ATTR".to_string()).ok()?;
    let entries = parse_show_attr_entries(&show_out);
    if entries.is_empty() {
        return None;
    }
    let dropped_pos = entries
        .iter()
        .position(|(name, _, _)| name.eq_ignore_ascii_case(dropped))?;
    let (canon, ty, _) = entries.get(dropped_pos)?;
    if ty.trim().is_empty() {
        return None;
    }

    let mut lines: Vec<String> = vec![format!(
        "ADDATTR({dropped_pos},{canon}:{})",
        ty.trim()
    )];
    let page_size = 200usize;
    let max_rows: usize = std::env::var("NEWDB_GUI_DROP_SNAPSHOT_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    let mut page_no = 1usize;
    let mut rows_done = 0usize;
    loop {
        let page = query_page(state.clone(), page_no, page_size, "id".to_string(), false).ok()?;
        if page.rows.is_empty() {
            break;
        }
        let id_idx = page
            .headers
            .iter()
            .position(|h| h.trim().eq_ignore_ascii_case("id"))?;
        for row in &page.rows {
            if row.len() <= id_idx {
                continue;
            }
            if rows_done >= max_rows {
                return None;
            }
            let mut vals: Vec<String> = Vec::new();
            vals.push(row[id_idx].clone());
            for (name, _, _) in &entries {
                let idx = page
                    .headers
                    .iter()
                    .position(|h| h.trim().eq_ignore_ascii_case(name.as_str()));
                let cell = idx
                    .and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or_default();
                vals.push(cell);
            }
            lines.push(format!("UPDATE({})", vals.join(",")));
            rows_done += 1;
        }
        if page.rows.len() < page_size {
            break;
        }
        page_no += 1;
    }

    Some((
        lines.join("\n"),
        "backend:snapshot_delattr_addattr_updates".to_string(),
    ))
}

fn infer_inverse_backend(state: State<'_, AppState>, command: &str, op_type: Option<&str>) -> Option<(String, String)> {
    let t = command.trim();
    if t.is_empty() {
        return None;
    }
    let up = t.to_ascii_uppercase();
    let op = op_type.unwrap_or("").to_ascii_lowercase();

    if up.starts_with("BEGIN") || op == "txn_begin" {
        return Some(("ROLLBACK".to_string(), "rule:txn_begin".to_string()));
    }
    if up.starts_with("CREATE TABLE(") || op == "table_create" {
        let name = parse_create_table_name(t)?;
        return Some((format!("DROP TABLE({name})"), "rule:table_create".to_string()));
    }
    if up.starts_with("DROP TABLE(") || up.starts_with("DROP TABLE ") || op == "table_drop" {
        let name = parse_drop_table_name(t)?;
        if let Some((script, rows, truncated)) = build_drop_table_snapshot_inverse(state.clone(), &name) {
            let source = if truncated {
                format!("backend:snapshot_table_drop rows={} truncated=true", rows)
            } else {
                format!("backend:snapshot_table_drop rows={}", rows)
            };
            return Some((script, source));
        }
        // Fallback soft inverse when snapshot cannot be built.
        return Some((format!("CREATE TABLE({name})"), "rule:table_drop_soft".to_string()));
    }
    if up.starts_with("USE(") || up.starts_with("USE ") || op == "table_use" {
        let prev_table = {
            let app = state.inner();
            let st = app.st.lock().ok()?;
            st.current_table.trim().to_string()
        };
        if prev_table.is_empty() {
            return None;
        }
        return Some((format!("USE({prev_table})"), "backend:state_table_use".to_string()));
    }
    if up.starts_with("RENAME TABLE(") || op == "table_rename" {
        let prev_table = {
            let app = state.inner();
            let st = app.st.lock().ok()?;
            st.current_table.trim().to_string()
        };
        if prev_table.is_empty() {
            return None;
        }
        return Some((
            format!("RENAME TABLE({prev_table})"),
            "backend:state_table_rename".to_string(),
        ));
    }
    if up.starts_with("SET PRIMARY KEY(") || op == "schema_set_pk" {
        let out = execute_command(state, "SHOW PRIMARY KEY".to_string()).ok()?;
        let current_pk = parse_primary_key_from_show_output(&out)?;
        if current_pk.is_empty() {
            return None;
        }
        return Some((
            format!("SET PRIMARY KEY({current_pk})"),
            "backend:snapshot_primary_key".to_string(),
        ));
    }
    if up.starts_with("ADDATTR(") {
        let args = parse_paren_args(t)?;
        let (col, _ty) = match args.len() {
            1 => {
                let pair = args[0].trim();
                pair.split_once(':')?
            }
            2 => {
                let idx = args[0].trim();
                if idx.is_empty() || !idx.chars().all(|c| c.is_ascii_digit()) {
                    return None;
                }
                let pair = args[1].trim();
                pair.split_once(':')?
            }
            _ => return None,
        };
        let col = col.trim();
        if col.is_empty() {
            return None;
        }
        return Some((format!("DELATTR({col})"), "rule:addattr".to_string()));
    }
    if up.starts_with("DEFATTR(") {
        let show_out = execute_command(state, "SHOW ATTR".to_string()).ok()?;
        let entries = parse_show_attr_entries(&show_out);
        if entries.is_empty() {
            return None;
        }
        let non_id = entries
            .iter()
            .filter(|(name, _, _)| name != "id")
            .map(|(name, ty, _)| format!("{name}:{ty}"))
            .collect::<Vec<_>>();
        // Old schema without user attrs means restore to RESET+empty DEFATTR state
        // cannot be faithfully represented by DEFATTR itself; keep conservative.
        if non_id.is_empty() {
            return None;
        }
        let pk = entries
            .iter()
            .find_map(|(name, _, is_pk)| if *is_pk { Some(name.clone()) } else { None })
            .unwrap_or_else(|| "id".to_string());
        let mut backward = format!("DEFATTR({})", non_id.join(","));
        if pk != "id" {
            backward.push('\n');
            backward.push_str(&format!("SET PRIMARY KEY({pk})"));
        }
        return Some((backward, "backend:snapshot_defattr".to_string()));
    }
    if up.starts_with("RENATTR(") {
        let args = parse_paren_args(t)?;
        if args.len() != 2 {
            return None;
        }
        let old_name = args[0].trim();
        let new_name = args[1].trim();
        if old_name.is_empty() || new_name.is_empty() {
            return None;
        }
        return Some((
            format!("RENATTR({new_name},{old_name})"),
            "rule:renattr".to_string(),
        ));
    }
    if up.starts_with("DELATTR(") {
        let args = parse_paren_args(t)?;
        if args.len() != 1 {
            return None;
        }
        let dropped = args[0].trim();
        if dropped.is_empty() {
            return None;
        }
        return build_delattr_inverse_compound(state, dropped);
    }
    if up.starts_with("SETATTR(") {
        let args = parse_paren_args(t)?;
        if args.len() != 3 {
            return None;
        }
        let id = args[0].trim();
        let attr = args[1].trim();
        if id.is_empty() || attr.is_empty() {
            return None;
        }
        let row_map = find_row_map_by_id_via_query_page(state.clone(), id, 64, 200).or_else(|| {
            let find_out = execute_command(state.clone(), format!("FIND({id})")).ok()?;
            parse_find_row_map(&find_out, id)
        })?;
        let old_val = row_map_lookup_ci(&row_map, attr).unwrap_or_default();
        return Some((
            format!("SETATTR({id},{attr},{old_val})"),
            "backend:snapshot_setattr".to_string(),
        ));
    }
    if up.starts_with("INSERT(") || up.starts_with("BULKINSERT(") || op == "data_insert" {
        let args = parse_paren_args(t)?;
        let id = args.first()?.trim();
        if id.is_empty() {
            return None;
        }
        return Some((format!("DELETE({id})"), "rule:insert".to_string()));
    }
    if !(up.starts_with("DELETE(") || up.starts_with("UPDATE(") || op == "data_delete" || op == "data_update") {
        return None;
    }

    let args = parse_paren_args(t)?;
    let id = args.first()?.trim().to_string();
    if id.is_empty() {
        return None;
    }
    let show_attr_out = execute_command(state.clone(), "SHOW ATTR".to_string()).ok()?;
    let attr_order = parse_show_attr_order(&show_attr_out);
    let row_map = find_row_map_by_id_via_query_page(state.clone(), &id, 64, 200).or_else(|| {
        let find_out = execute_command(state.clone(), format!("FIND({id})")).ok()?;
        parse_find_row_map(&find_out, &id)
    })?;

    let mut ordered = Vec::new();
    ordered.push(id.clone());
    for attr in attr_order.iter().skip(1) {
        ordered.push(row_map_lookup_ci(&row_map, attr).unwrap_or_default());
    }
    if up.starts_with("DELETE(") || op == "data_delete" {
        return Some((format!("INSERT({})", ordered.join(",")), "backend:snapshot_delete".to_string()));
    }
    Some((format!("UPDATE({})", ordered.join(",")), "backend:snapshot_update".to_string()))
}

#[tauri::command]
fn infer_inverse_command(
    state: State<'_, AppState>,
    command: String,
    op_type: Option<String>,
) -> Result<InverseInferResult, String> {
    let inferred = infer_inverse_backend(state, &command, op_type.as_deref());
    Ok(match inferred {
        Some((backward, source)) => InverseInferResult {
            backward: Some(backward),
            source,
        },
        None => InverseInferResult {
            backward: None,
            source: "none".to_string(),
        },
    })
}

fn run_script_via_cap(
    app: &tauri::AppHandle,
    api: &CapApi,
    state: &AppState,
    st: &mut SessionState,
    cap: &mut CapSessionState,
    script: &str,
    total_lines: usize,
    cancel: &AtomicBool,
) -> Result<String, String> {
    ensure_cap_session(api, st, cap)?;
    let engine = cap.engine.ok_or("no engine")? as *mut c_void;
    let embed = cap.embed.ok_or("no embed")? as *mut c_void;
    let mdb = cap.mdb_session.ok_or("no mdb session")? as *mut c_void;
    let ctrl = unsafe { (api.long_task_control_create)() };
    if ctrl.is_null() {
        return Err("structdb_long_task_control_create returned null".to_string());
    }
    let mut cb_ctx = LongTaskCbCtx { app: app.clone() };
    unsafe {
        (api.long_task_control_set_progress_callback)(
            ctrl,
            Some(cap_long_task_progress_cb),
            &mut cb_ctx as *mut LongTaskCbCtx as *mut c_void,
        );
        (api.engine_begin_mdb_script_batch)(engine, ctrl, total_lines as u64);
    }
    *state.active_long_task.lock().expect("long task lock") = Some(LongTaskCtrlPtr(ctrl));
    let mut all = String::new();
    for (line_no, line) in script.lines().enumerate() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        let cancelled = cancel.load(Ordering::Relaxed)
            || unsafe { (api.long_task_control_cancel_requested)(ctrl) != 0 };
        if cancelled {
            if !all.is_empty() {
                all.push('\n');
            }
            all.push_str("[SCRIPT] cancelled\n");
            break;
        }
        if let Some(table) = parse_use_table(t) {
            st.current_table = table;
        }
        let line_c = as_cstring(t);
        let mut err = vec![0_i8; 8192];
        let mut lines: Vec<String> = Vec::new();
        let mut repl_flag: i32 = 0;
        let opts = StructdbMdbRunOptions {
            struct_size: STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3,
            reserved_flags: 0,
            fsync_each_batch: 0,
            fsync_each_session_txn_op: 0,
            fail_if_unclosed_txn: 0,
            allow_persist_while_txn_active_experimental: 1,
            log_file_path: std::ptr::null(),
            log_line_cb: Some(structdb_log_line_cb),
            log_user_data: (&mut lines) as *mut Vec<String> as *mut c_void,
            repl_exit_requested_out: &mut repl_flag,
            long_task_control: std::ptr::null_mut(),
        };
        let rc = unsafe {
            (api.mdb_execute_line_ex)(
                engine,
                embed,
                mdb,
                line_c.as_ptr(),
                err.as_mut_ptr() as *mut c_char,
                err.len(),
                &opts,
            )
        };
        if rc == STRUCTDB_CAPI_ERR_CANCELLED {
            if !all.is_empty() {
                all.push('\n');
            }
            all.push_str("[SCRIPT] cancelled\n");
            break;
        }
        if rc != STRUCTDB_CAPI_OK {
            let msg = cstr_err(&err);
            return Err(if msg.is_empty() {
                format!("structdb_mdb_execute_line_ex failed (rc={rc})")
            } else {
                format!("{msg} (rc={rc})")
            });
        }
        let one = lines.join("\n");
        if !all.is_empty() {
            all.push('\n');
        }
        all.push_str(&one);
        if should_stop_script_on_output(&one) {
            all.push_str(&format!(
                "\n[SCRIPT] stopped at line {} due to command error.\n",
                line_no + 1
            ));
            break;
        }
    }
    unsafe {
        (api.engine_end_mdb_script_batch)(engine);
        (api.long_task_control_destroy)(ctrl);
    }
    *state.active_long_task.lock().expect("long task lock") = None;
    Ok(all)
}

fn run_script_via_demo_mdb(
    app: &tauri::AppHandle,
    cancel: &AtomicBool,
    st: &mut SessionState,
    script: &str,
    total_lines: usize,
    executed: &mut usize,
) -> Result<String, String> {
    let mut last_use: Option<String> = None;
    for line in script.lines() {
        if let Some(t) = parse_use_table(line) {
            last_use = Some(t);
        }
    }
    if let Some(t) = last_use {
        st.current_table = t;
    }
    let mut all = String::new();
    for (line_no, line) in script.lines().enumerate() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        if cancel.load(Ordering::Relaxed) {
            if !all.is_empty() {
                all.push('\n');
            }
            all.push_str("[SCRIPT] cancelled\n");
            break;
        }
        if let Some(table) = parse_use_table(t) {
            st.current_table = table;
        }
        let one = run_demo_mdb(&st, t)?;
        if !all.is_empty() {
            all.push('\n');
        }
        all.push_str(&one);
        *executed += 1;
        emit_mdb_script_long_task(app, *executed, total_lines, "running");
        if should_stop_script_on_output(&one) {
            all.push_str(&format!(
                "\n[SCRIPT] stopped at line {} due to command error.\n",
                line_no + 1
            ));
            break;
        }
    }
    Ok(all)
}

#[tauri::command]
fn cancel_mdb_script(state: State<'_, AppState>) -> Result<(), String> {
    cancel_long_task(state)
}

/// Unified cancel: C API `LongTaskCancelToken` + demo-path atomic fallback.
#[tauri::command]
fn cancel_long_task(state: State<'_, AppState>) -> Result<(), String> {
    let inner = state.inner();
    if let Ok(api) = load_cap_api() {
        if let Some(LongTaskCtrlPtr(ctrl)) = *inner.active_long_task.lock().expect("long task lock") {
            unsafe {
                (api.long_task_control_request_cancel)(ctrl);
            }
        }
    }
    inner.script_cancel.store(true, Ordering::Release);
    Ok(())
}

#[tauri::command]
fn run_script(app: tauri::AppHandle, state: State<'_, AppState>, script: String) -> Result<String, String> {
    let inner = state.inner();
    let cancel = inner.script_cancel.clone();
    cancel.store(false, Ordering::Release);
    let total_lines = script
        .lines()
        .filter(|l| {
            let t = l.trim();
            !t.is_empty() && !t.starts_with('#')
        })
        .count();

    let mut st = inner.st.lock().expect("state lock");
    let out = if let Ok(api) = load_cap_api() {
        let mut cap = inner.cap.lock().expect("cap lock");
        run_script_via_cap(&app, api, inner, &mut st, &mut cap, &script, total_lines, cancel.as_ref())?
    } else {
        let mut executed = 0usize;
        run_script_via_demo_mdb(&app, cancel.as_ref(), &mut st, &script, total_lines, &mut executed)?
    };
    Ok(out)
}

#[tauri::command]
fn run_script_ex(app: tauri::AppHandle, state: State<'_, AppState>, script: String) -> Result<ScriptExecResult, String> {
    let out = run_script(app, state, script)?;
    let cancelled = out.contains("[SCRIPT] cancelled");
    let error_code = parse_capi_error_code(&out);
    let stop_line = out.lines().find_map(|line| {
        let t = line.trim();
        let prefix = "[SCRIPT] stopped at line ";
        if !t.starts_with(prefix) {
            return None;
        }
        let rest = &t[prefix.len()..];
        let n_text = rest.split_whitespace().next()?;
        n_text.parse::<usize>().ok()
    });
    Ok(ScriptExecResult {
        ok: !cancelled && error_code.is_none() && stop_line.is_none(),
        output: strip_capi_error_line(&out),
        error_code,
        stop_line,
        cancelled,
    })
}

#[tauri::command]
fn query_page(
    state: State<'_, AppState>,
    page_no: usize,
    page_size: usize,
    order_key: String,
    descending: bool,
) -> Result<PageResult, String> {
    let app = state.inner();
    let st_work = {
        let mut st = app.st.lock().expect("state lock");
        st.page_size = page_size.max(1).min(MDB_PAGE_JSON_MAX_PAGE_SIZE);
        SessionState {
            data_dir: st.data_dir.clone(),
            current_table: st.current_table.clone(),
            page_size: st.page_size,
        }
    };
    if normalize_table(&st_work).is_empty() {
        return Ok(PageResult {
            headers: vec!["id".to_string()],
            columns: vec![ColumnMeta { name: "id".to_string(), ty: "int".to_string() }],
            rows: Vec::new(),
            raw: "[INFO] no table selected".to_string(),
        });
    }
    if let Ok(api) = load_cap_api() {
        if let Ok(mut cap) = app.cap.lock() {
            if let Ok(r) = query_page_via_cap_session(
                &api,
                &st_work,
                &mut cap,
                page_no,
                st_work.page_size,
                &order_key,
                descending,
            ) {
                return Ok(r);
            }
        }
    }
    let p = page_no.max(1).to_string();
    let s = st_work.page_size.to_string();
    let order = if order_key.trim().is_empty() {
        "id".to_string()
    } else {
        order_key.trim().to_string()
    };
    let tbl = normalize_table(&st_work);
    let mut arg_strings: Vec<String> = vec![
        "--page".into(),
        p,
        s,
        "--order".into(),
        order,
        "--page-json".into(),
        "--table".into(),
        tbl,
    ];
    if descending {
        arg_strings.push("--desc".into());
    }
    let args_ref: Vec<&str> = arg_strings.iter().map(|x| x.as_str()).collect();
    let out = run_demo(&st_work, &args_ref)?;
    if let Some(mut parsed_json) = parse_page_json_output(&out) {
        if parsed_json.columns.is_empty() {
            parsed_json.columns = schema_columns_for_ui(app, &st_work);
        }
        if parsed_json.headers.is_empty() {
            parsed_json.headers = schema_headers_for_ui(app, &st_work);
        }
        return Ok(parsed_json);
    }
    let parsed = parse_page_output(&out);
    if parsed.headers.len() == 1 && parsed.headers[0] == "raw" {
        let mut headers = schema_headers_for_ui(app, &st_work);
        if headers.is_empty() || headers[0] != "#".to_string() {
            headers.insert(0, "#".to_string());
        }
        let columns = schema_columns_for_ui(app, &st_work);
        let mut rows = Vec::new();
        if let Some(data_start) = out.lines().position(|l| l.contains("[PAGE] table=")) {
            let lines: Vec<&str> = out.lines().collect();
            for line in lines.iter().skip(data_start + 1) {
                let t = line.trim();
                if t.starts_with('┌') || t.starts_with('└') || t.starts_with('├') {
                    continue;
                }
                if t.starts_with('│') || t.starts_with('|') {
                    let cols: Vec<String> = t
                        .trim_matches(|c| c == '|' || c == '│')
                        .split(|c| c == '|' || c == '│')
                        .map(|s| s.trim().to_string())
                        .collect();
                    if cols.len() == headers.len() {
                        rows.push(cols);
                    } else if cols.len() + 1 == headers.len() {
                        // Older or nonstandard renderings may miss the leading "#" cell.
                        let mut with_index = Vec::with_capacity(headers.len());
                        with_index.push(String::new());
                        with_index.extend(cols);
                        rows.push(with_index);
                    }
                }
            }
        }
        return Ok(PageResult { headers, columns, rows, raw: out });
    }
    Ok(parsed)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DllInfo {
    loaded: bool,
    version: String,
    path: String,
    message: String,
    api_version_major: i32,
    api_version_minor: i32,
    api_version_patch: i32,
    abi_version: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeArtifactInfo {
    gui_exe_path: String,
    gui_exe_modified: String,
    /// Bundled `structdb_app` (CLI / REPL helper).
    demo_path: String,
    demo_modified: String,
    /// Resolved `structdb_capi_shared` DLL path.
    dll_path: String,
    dll_modified: String,
    /// Legacy field: always empty (no plugin CLI backend).
    cli_backend_plugin_path: String,
    cli_backend_plugin_modified: String,
    /// Legacy field: always empty.
    newdb_cli_backend_path_env: String,
    /// Contract label for `SHOW TUNING JSON` / JSONL stats (engine and GUI stay aligned).
    runtime_stats_schema_version: String,
    /// Best-effort: set `STRUCTDB_GIT_COMMIT` in CI pack scripts to populate.
    backend_git_commit: String,
    /// `Release` / `RelWithDebInfo` / etc. when `STRUCTDB_BUILD_PROFILE` is set at pack time.
    build_profile: String,
    /// Tauri / GUI build: `debug` vs `release` (from `cfg!(debug_assertions)`).
    gui_package_kind: String,
    /// Bundled GoogleTest C API shim DLL when synced (`libgtest_capi.dll`); empty if absent.
    gtest_capi_dll_path: String,
    gtest_capi_dll_modified: String,
    /// From loaded `structdb_capi_shared` (`structdb_capi_version`); `-1` when DLL not loaded.
    c_api_version_major: i32,
    c_api_version_minor: i32,
    c_api_version_patch: i32,
    c_api_abi_version: i32,
}

fn format_modified_time(path: &Path) -> String {
    match fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
    {
        Some(d) => d.as_secs().to_string(),
        None => "unknown".to_string(),
    }
}

#[tauri::command]
fn runtime_artifact_info() -> RuntimeArtifactInfo {
    configure_runtime_loader_paths();
    let gui_exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("unknown"));
    let demo = resolve_structdb_app_bin().unwrap_or_else(|_| PathBuf::from("not-found"));
    let dll = resolve_structdb_capi_dll();
    let gui_package_kind = if cfg!(debug_assertions) {
        "debug".to_string()
    } else {
        "release".to_string()
    };
    let build_profile = std::env::var("STRUCTDB_BUILD_PROFILE")
        .or_else(|_| std::env::var("NEWDB_BUILD_PROFILE"))
        .unwrap_or_default();
    let backend_git_commit = std::env::var("STRUCTDB_GIT_COMMIT")
        .or_else(|_| std::env::var("NEWDB_GIT_COMMIT"))
        .unwrap_or_default();

    let (c_api_version_major, c_api_version_minor, c_api_version_patch, c_api_abi_version) =
        match load_cap_api() {
            Ok(api) => unsafe {
                let v = (api.capi_version)();
                let major = ((v >> 16) & 0xff) as i32;
                let minor = ((v >> 8) & 0xff) as i32;
                let patch = (v & 0xff) as i32;
                (major, minor, patch, -1)
            },
            Err(_) => (-1, -1, -1, -1),
        };
    RuntimeArtifactInfo {
        gui_exe_path: gui_exe.display().to_string(),
        gui_exe_modified: format_modified_time(&gui_exe),
        demo_path: demo.display().to_string(),
        demo_modified: format_modified_time(&demo),
        dll_path: dll.display().to_string(),
        dll_modified: format_modified_time(&dll),
        cli_backend_plugin_path: String::new(),
        cli_backend_plugin_modified: "n/a".to_string(),
        newdb_cli_backend_path_env: String::new(),
        runtime_stats_schema_version: "structdb.gui.not_available".to_string(),
        backend_git_commit,
        build_profile,
        gui_package_kind,
        gtest_capi_dll_path: String::new(),
        gtest_capi_dll_modified: "n/a".to_string(),
        c_api_version_major,
        c_api_version_minor,
        c_api_version_patch,
        c_api_abi_version,
    }
}

#[tauri::command]
fn dll_info() -> DllInfo {
    let path = resolve_structdb_capi_dll().display().to_string();
    match load_cap_api() {
        Ok(api) => unsafe {
            let c = (api.version_string)();
            let version = if c.is_null() {
                "unknown".to_string()
            } else {
                CStr::from_ptr(c).to_string_lossy().to_string()
            };
            let v = (api.capi_version)();
            let major = ((v >> 16) & 0xff) as i32;
            let minor = ((v >> 8) & 0xff) as i32;
            let patch = (v & 0xff) as i32;
            DllInfo {
                loaded: true,
                version,
                path,
                message: "structdb_capi_shared loaded".to_string(),
                api_version_major: major,
                api_version_minor: minor,
                api_version_patch: patch,
                abi_version: -1,
            }
        },
        Err(e) => DllInfo {
            loaded: false,
            version: "n/a".to_string(),
            path,
            message: e,
            api_version_major: -1,
            api_version_minor: -1,
            api_version_patch: -1,
            abi_version: -1,
        },
    }
}

pub fn run() {
    configure_runtime_loader_paths();
    tauri::Builder::default()
        .manage(AppState {
            st: Mutex::new(SessionState {
                data_dir: std::env::current_dir()
                    .ok()
                    .and_then(|p| p.parent().map(|v| v.to_string_lossy().to_string()))
                    .unwrap_or_default(),
                current_table: String::new(),
                page_size: 12,
            }),
            cap: Mutex::new(CapSessionState::default()),
            id_remap_chain: Mutex::new(Vec::new()),
            active_long_task: Mutex::new(None),
            script_cancel: Arc::new(AtomicBool::new(false)),
        })
        .invoke_handler(tauri::generate_handler![
            get_state,
            set_workspace,
            set_current_table,
            list_tables,
            get_settings,
            set_settings,
            execute_command,
            execute_command_ex,
            infer_inverse_command,
            txn_begin,
            txn_commit,
            txn_rollback,
            txn_savepoint,
            txn_rollback_to,
            txn_release_savepoint,
            save_stack_units,
            load_stack_units,
            stack_undo_unit,
            stack_redo_unit,
            export_bundle,
            run_structdb_bench,
            run_script,
            run_script_ex,
            cancel_mdb_script,
            cancel_long_task,
            query_page,
            dll_info,
            runtime_artifact_info,
            cli_terminal_start,
            cli_terminal_write_line,
            cli_terminal_stop
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

pub fn self_check() -> Result<(), String> {
    // 1) Ensure we can resolve binaries/scripts using current layout heuristics.
    let _demo = resolve_structdb_app_bin().ok();

    // 2) Exercise DLL detection path; CI can optionally require it.
    let info = dll_info();
    if std::env::var("STRUCTDB_GUI_EXPECT_DLL").ok().as_deref() == Some("1") && !info.loaded {
        return Err(format!("DLL expected but not loaded: path={} msg={}", info.path, info.message));
    }

    // 3) Minimal business self-check on a temp workspace.
    //    Run a few core commands via the DLL persistent session and ensure
    //    list_tables can discover the created table on disk.
    let tmp = std::env::temp_dir().join("structdb_gui_self_check");
    fs::create_dir_all(&tmp).map_err(|e| format!("create temp workspace failed: {e}"))?;
    let tmp_dir = tmp.to_string_lossy().to_string();

    if let Ok(api) = load_cap_api() {
        let st = SessionState {
            data_dir: tmp_dir.clone(),
            current_table: String::new(),
            page_size: 12,
        };
        let mut cap = CapSessionState::default();

        let exec = |api: &CapApi,
                    cap: &mut CapSessionState,
                    st: &SessionState,
                    table: &str,
                    cmd: &str|
         -> Result<String, String> {
            let st2 = SessionState {
                data_dir: st.data_dir.clone(),
                current_table: table.to_string(),
                page_size: st.page_size,
            };
            execute_via_cap_session(api, &st2, cap, cmd)
        };

        let _ = exec(api, &mut cap, &st, "", "CREATE TABLE(users)")?;
        let _ = exec(api, &mut cap, &st, "", "USE(users)")?;
        let _ = exec(api, &mut cap, &st, "", "DEFATTR(name:string,dept:string,age:int,salary:int)")?;
        let _ = exec(api, &mut cap, &st, "", "INSERT(1,Alice,ENG,29,18000)")?;
        let count_out = exec(api, &mut cap, &st, "users", "COUNT")?;

        fn parse_count_rows(out: &str) -> Option<u64> {
            // Expected format (one example):
            //   [COUNT] table='users' rows=123 decode_calls=...
            // or:
            //   [COUNT] 123 / 456 rows (...)
            let lower = out;
            for token in lower.split(|c: char| c == '\n' || c == '\r') {
                if !token.contains("[COUNT]") {
                    continue;
                }
                if let Some(idx) = token.find("rows=") {
                    let rest = &token[(idx + 5)..];
                    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                    if digits.is_empty() {
                        continue;
                    }
                    if let Ok(v) = digits.parse::<u64>() {
                        return Some(v);
                    }
                }
                // Fallback: try parsing "[COUNT] <n> / <m> rows"
                if let Some(after) = token.split("[COUNT]").nth(1) {
                    let t = after.trim();
                    let n_digits: String = t.chars().take_while(|c| c.is_ascii_digit()).collect();
                    if !n_digits.is_empty() {
                        if let Ok(v) = n_digits.parse::<u64>() {
                            return Some(v);
                        }
                    }
                }
            }
            None
        }

        // Transaction smoke: BEGIN/ROLLBACK should prevent persistence of changes.
        let _ = exec(api, &mut cap, &st, "users", "BEGIN")?;
        let _ = exec(api, &mut cap, &st, "users", "INSERT(2,Bob,ENG,33,22000)")?;
        let _ = exec(api, &mut cap, &st, "users", "ROLLBACK")?;
        let count_after_rb = exec(api, &mut cap, &st, "users", "COUNT")?;
        let initial_rows = parse_count_rows(&count_out);
        let after_rows = parse_count_rows(&count_after_rb);
        if after_rows != Some(1) {
            // Be conservative: still treat as failure if we can't see the expected count.
            return Err(format!(
                "txn rollback count check failed: expected rows=1; parsed initial={:?} after={:?}; initial_out={:?} after_rb_out={:?}",
                initial_rows, after_rows, count_out, count_after_rb
            ));
        }

        let tables = parse_list_tables_from_fs(&tmp_dir);
        if !tables.iter().any(|t| t == "users") {
            return Err(format!("expected table `users` in workspace, got={tables:?}"));
        }
    } else {
        // If DLL is not loadable (e.g. local dev), the check still should not crash.
        let _tables = parse_list_tables_from_fs(&tmp_dir);
    }
    Ok(())
}

#[cfg(test)]
mod phase38_remap_ingest_tests {
    use super::*;

    fn minimal_app() -> AppState {
        AppState {
            st: Mutex::new(SessionState::default()),
            cap: Mutex::new(CapSessionState::default()),
            id_remap_chain: Mutex::new(Vec::new()),
            active_long_task: Mutex::new(None),
            script_cancel: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn reorder_map_ingest_pushes_each_line() {
        let app = minimal_app();
        let out = concat!(
            "noise\n",
            "[REORDER_MAP_JSON]{\"table\":\"a\",\"pairs\":[[\"1\",\"2\"]]}\n",
            "[REORDER_MAP_JSON]{\"table\":\"b\",\"pairs\":[[\"3\",\"4\"]]}\n",
        );
        ingest_reorder_map_from_engine_output(&app, out);
        let chain = app.id_remap_chain.lock().expect("lock");
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].table, "a");
        assert_eq!(chain[1].table, "b");
        assert_eq!(chain[0].map.get("1").map(String::as_str), Some("2"));
        assert_eq!(chain[1].map.get("3").map(String::as_str), Some("4"));
    }
}
