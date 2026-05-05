use serde::{Deserialize, Serialize};
use libloading::{Library, Symbol};
use once_cell::sync::OnceCell;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
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

#[derive(Default)]
struct SessionState {
    data_dir: String,
    current_table: String,
    page_size: usize,
}

#[derive(Default)]
struct DllSessionState {
    // Store raw handle as integer so AppState stays Send+Sync for Tauri.
    handle: Option<usize>,
    data_dir: String,
}

struct AppState {
    st: Mutex<SessionState>,
    dll: Mutex<DllSessionState>,
}

struct DllApi {
    _lib: Library,
    version: unsafe extern "C" fn() -> *const std::os::raw::c_char,
    session_create: unsafe extern "C" fn(
        *const std::os::raw::c_char,
        *const std::os::raw::c_char,
        *const std::os::raw::c_char,
    ) -> *mut std::ffi::c_void,
    session_destroy: unsafe extern "C" fn(*mut std::ffi::c_void),
    session_set_table: unsafe extern "C" fn(*mut std::ffi::c_void, *const std::os::raw::c_char) -> i32,
    session_execute: unsafe extern "C" fn(
        *mut std::ffi::c_void,
        *const std::os::raw::c_char,
        *mut std::os::raw::c_char,
        usize,
    ) -> i32,
    session_where_plan_json: unsafe extern "C" fn(
        *mut std::ffi::c_void,
        i32,
        *const *const std::os::raw::c_char,
        *mut std::os::raw::c_char,
        usize,
    ) -> i32,
}

static DLL_API: OnceCell<DllApi> = OnceCell::new();

struct CliTermProcess {
    child: std::process::Child,
    stdin: Mutex<std::process::ChildStdin>,
}

static CLI_TERM_PROC: Mutex<Option<CliTermProcess>> = Mutex::new(None);

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CliTermChunk {
    chunk: String,
    stream: String,
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
    if let Some(mut p) = guard.take() {
        let _ = p.child.kill();
        let _ = p.child.wait();
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

    let demo_bin = resolve_demo_bin()?;
    let mut cmd = Command::new(&demo_bin);
    cmd.env_remove("NEWDB_TABLE");
    cmd.env_remove("NEWDB_DATA_DIR");
    cmd.env_remove("NEWDB_LOG");
    if !dir.trim().is_empty() {
        cmd.arg("--data-dir").arg(&dir);
    }
    let log_base = if dir.trim().is_empty() {
        std::env::temp_dir()
    } else {
        PathBuf::from(&dir)
    };
    if !log_base.exists() {
        fs::create_dir_all(&log_base)
            .map_err(|e| format!("failed to create workspace {}: {e}", log_base.display()))?;
    }
    let log_name = format!(
        "cli_term_{}_{}.bin",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );
    let log_path = log_base.join(log_name);
    cmd.arg("--log-file").arg(log_path.to_string_lossy().to_string());

    let table_trim = tbl.trim().to_string();
    if !table_trim.is_empty() {
        let bin_path = Path::new(&dir).join(format!("{table_trim}.bin"));
        if bin_path.exists() {
            cmd.arg("--table").arg(&table_trim);
        }
    }

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        cmd.creation_flags(0x08000000);
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn newdb_demo: {e}"))?;
    let stdin = child.stdin.take().ok_or("newdb_demo stdin unavailable")?;
    let stdout = child
        .stdout
        .take()
        .ok_or("newdb_demo stdout unavailable")?;
    let stderr = child
        .stderr
        .take()
        .ok_or("newdb_demo stderr unavailable")?;

    spawn_cli_pipe_reader(app.clone(), stdout, "stdout");
    spawn_cli_pipe_reader(app.clone(), stderr, "stderr");

    let mut guard = CLI_TERM_PROC
        .lock()
        .map_err(|_| "cli term mutex poisoned".to_string())?;
    *guard = Some(CliTermProcess {
        child,
        stdin: Mutex::new(stdin),
    });
    Ok(())
}

#[tauri::command]
fn cli_terminal_write_line(line: String) -> Result<(), String> {
    let guard = CLI_TERM_PROC
        .lock()
        .map_err(|_| "cli term mutex poisoned".to_string())?;
    let proc = guard.as_ref().ok_or("CLI session not started; use 重新连接")?;
    let mut w = proc
        .stdin
        .lock()
        .map_err(|_| "stdin mutex poisoned".to_string())?;
    w.write_all(line.as_bytes())
        .and_then(|_| w.write_all(b"\n"))
        .map_err(|e| format!("stdin write failed: {e}"))?;
    w.flush().map_err(|e| format!("stdin flush failed: {e}"))?;
    Ok(())
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
    bg_image_opacity: f32,
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
            bg_image_opacity: 0.22,
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

fn resolve_demo_bin() -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("newdb_demo.exe"));
            candidates.push(dir.join("newdb_demo"));
            // Common installer layout: binaries in a `bin/` folder.
            candidates.push(dir.join("bin/newdb_demo.exe"));
            candidates.push(dir.join("bin/newdb_demo"));
            candidates.push(dir.join("../bin/newdb_demo.exe"));
            candidates.push(dir.join("../bin/newdb_demo"));
            candidates.push(dir.join("resources/newdb_demo.exe"));
            candidates.push(dir.join("resources/newdb_demo"));
            candidates.push(dir.join("../resources/newdb_demo.exe"));
            candidates.push(dir.join("../resources/newdb_demo"));
            candidates.push(dir.join("../newdb_demo.exe"));
            candidates.push(dir.join("../newdb_demo"));
        }
    }
    candidates.push(PathBuf::from("newdb_demo.exe"));
    candidates.push(PathBuf::from("newdb_demo"));
    candidates.push(PathBuf::from("../build_shared/newdb_demo.exe"));
    candidates.push(PathBuf::from("../build/newdb_demo.exe"));
    candidates.push(PathBuf::from("../build_all/newdb_demo.exe"));
    candidates.push(PathBuf::from("../../build_shared/newdb_demo.exe"));
    candidates.push(PathBuf::from("../../build/newdb_demo.exe"));

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
    Err(format!("newdb_demo not found; tried: {tried}"))
}

fn resolve_perf_bin() -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("newdb_perf.exe"));
            candidates.push(dir.join("newdb_perf"));
            candidates.push(dir.join("bin/newdb_perf.exe"));
            candidates.push(dir.join("bin/newdb_perf"));
            candidates.push(dir.join("../bin/newdb_perf.exe"));
            candidates.push(dir.join("../bin/newdb_perf"));
            candidates.push(dir.join("resources/newdb_perf.exe"));
            candidates.push(dir.join("resources/newdb_perf"));
            candidates.push(dir.join("../resources/newdb_perf.exe"));
            candidates.push(dir.join("../resources/newdb_perf"));
            candidates.push(dir.join("../newdb_perf.exe"));
            candidates.push(dir.join("../newdb_perf"));
        }
    }
    candidates.push(PathBuf::from("newdb_perf.exe"));
    candidates.push(PathBuf::from("newdb_perf"));
    candidates.push(PathBuf::from("../build_shared/newdb_perf.exe"));
    candidates.push(PathBuf::from("../build/newdb_perf.exe"));
    candidates.push(PathBuf::from("../build_all/newdb_perf.exe"));
    candidates.push(PathBuf::from("../../build_shared/newdb_perf.exe"));
    candidates.push(PathBuf::from("../../build/newdb_perf.exe"));

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
    Err(format!("newdb_perf not found; tried: {tried}"))
}

fn resolve_runtime_report_bin() -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join("newdb_runtime_report.exe"));
            candidates.push(dir.join("newdb_runtime_report"));
            candidates.push(dir.join("bin/newdb_runtime_report.exe"));
            candidates.push(dir.join("bin/newdb_runtime_report"));
            candidates.push(dir.join("../bin/newdb_runtime_report.exe"));
            candidates.push(dir.join("../bin/newdb_runtime_report"));
            candidates.push(dir.join("resources/newdb_runtime_report.exe"));
            candidates.push(dir.join("resources/newdb_runtime_report"));
            candidates.push(dir.join("../resources/newdb_runtime_report.exe"));
            candidates.push(dir.join("../resources/newdb_runtime_report"));
            candidates.push(dir.join("../newdb_runtime_report.exe"));
            candidates.push(dir.join("../newdb_runtime_report"));
        }
    }
    candidates.push(PathBuf::from("newdb_runtime_report.exe"));
    candidates.push(PathBuf::from("newdb_runtime_report"));
    candidates.push(PathBuf::from("../build_shared/newdb_runtime_report.exe"));
    candidates.push(PathBuf::from("../build/newdb_runtime_report.exe"));
    candidates.push(PathBuf::from("../build_all/newdb_runtime_report.exe"));
    candidates.push(PathBuf::from("../../build_shared/newdb_runtime_report.exe"));
    candidates.push(PathBuf::from("../../build/newdb_runtime_report.exe"));

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
    Err(format!("newdb_runtime_report not found; tried: {tried}"))
}

fn resolve_script_path(script_name: &str) -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("scripts").join(script_name));
        candidates.push(cwd.join("../scripts").join(script_name));
        candidates.push(cwd.join("../../scripts").join(script_name));
        candidates.push(cwd.join("resources").join("scripts").join(script_name));
        candidates.push(cwd.join("src-tauri").join("resources").join("scripts").join(script_name));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join(script_name));
            candidates.push(dir.join("scripts").join(script_name));
            candidates.push(dir.join("../scripts").join(script_name));
            candidates.push(dir.join("../../scripts").join(script_name));
            candidates.push(dir.join("../../../scripts").join(script_name));
            candidates.push(dir.join("resources").join("scripts").join(script_name));
            candidates.push(dir.join("../resources").join("scripts").join(script_name));
        }
    }
    if let Some(manifest_dir) = option_env!("CARGO_MANIFEST_DIR") {
        let manifest = PathBuf::from(manifest_dir);
        candidates.push(manifest.join("../scripts").join(script_name));
        candidates.push(manifest.join("resources").join("scripts").join(script_name));
        candidates.push(manifest.join("../src-tauri").join("resources").join("scripts").join(script_name));
    }
    candidates.push(PathBuf::from("scripts").join(script_name));
    candidates.push(PathBuf::from("../scripts").join(script_name));
    candidates.push(PathBuf::from("../../scripts").join(script_name));
    candidates.push(PathBuf::from(script_name));

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
    Err(format!("{script_name} not found; tried: {tried}"))
}

fn resolve_results_file(file_name: &str) -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("scripts").join("results").join(file_name));
        candidates.push(cwd.join("../scripts").join("results").join(file_name));
        candidates.push(cwd.join("../../scripts").join("results").join(file_name));
        candidates.push(cwd.join("src-tauri").join("resources").join("scripts").join("results").join(file_name));
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join(file_name));
            candidates.push(dir.join("results").join(file_name));
            candidates.push(dir.join("scripts").join("results").join(file_name));
            candidates.push(dir.join("../scripts").join("results").join(file_name));
            candidates.push(dir.join("../../scripts").join("results").join(file_name));
            candidates.push(dir.join("../../../scripts").join("results").join(file_name));
            candidates.push(dir.join("resources").join("scripts").join("results").join(file_name));
            candidates.push(dir.join("../resources").join("scripts").join("results").join(file_name));
        }
    }
    if let Some(manifest_dir) = option_env!("CARGO_MANIFEST_DIR") {
        let manifest = PathBuf::from(manifest_dir);
        candidates.push(manifest.join("../scripts/results").join(file_name));
        candidates.push(manifest.join("resources/scripts/results").join(file_name));
    }
    candidates.push(PathBuf::from("scripts").join("results").join(file_name));
    candidates.push(PathBuf::from("../scripts").join("results").join(file_name));
    candidates.push(PathBuf::from("../../scripts").join("results").join(file_name));
    candidates.push(PathBuf::from(file_name));

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
    Err(format!("{file_name} not found; tried: {tried}"))
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

fn is_safe_results_basename(name: &str) -> bool {
    if name.trim().is_empty() {
        return false;
    }
    // Disallow path traversal / separators.
    if name.contains('/') || name.contains('\\') {
        return false;
    }
    if name.contains("..") {
        return false;
    }
    true
}

#[tauri::command]
fn list_results_files(prefix: Option<String>, limit: Option<u32>) -> Result<Vec<String>, String> {
    let results_dir = resolve_results_dir()?;
    if !results_dir.exists() {
        return Ok(Vec::new());
    }
    let pref = prefix.unwrap_or_default();
    let take_n = limit.unwrap_or(20).min(200) as usize;
    let mut files: Vec<PathBuf> = fs::read_dir(&results_dir)
        .map_err(|e| format!("read_dir failed {}: {e}", results_dir.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|s| s.to_str()).unwrap_or_default();
            if !pref.is_empty() && !name.starts_with(&pref) {
                return false;
            }
            name.ends_with(".json")
        })
        .collect();
    files.sort_by_key(|p| fs::metadata(p).and_then(|m| m.modified()).ok());
    files.reverse();
    Ok(files
        .into_iter()
        .take(take_n)
        .filter_map(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.to_string()))
        .collect())
}

#[tauri::command]
fn read_results_json(file_name: String) -> Result<String, String> {
    if !is_safe_results_basename(&file_name) {
        return Err("invalid results file name".to_string());
    }
    let results_dir = resolve_results_dir()?;
    let p = results_dir.join(&file_name);
    fs::read_to_string(&p).map_err(|e| format!("failed to read {}: {e}", p.display()))
}

fn resolve_newdb_dll() -> PathBuf {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            // Prefer dll beside the packaged exe.
            candidates.push(dir.join("libnewdb.dll"));
            candidates.push(dir.join("newdb.dll"));
            // Common installer layout: binaries in a `bin/` folder.
            candidates.push(dir.join("bin/libnewdb.dll"));
            candidates.push(dir.join("bin/newdb.dll"));
            candidates.push(dir.join("../bin/libnewdb.dll"));
            candidates.push(dir.join("../bin/newdb.dll"));
            // Bundled resources fallback.
            candidates.push(dir.join("resources/libnewdb.dll"));
            candidates.push(dir.join("resources/newdb.dll"));
            candidates.push(dir.join("../resources/libnewdb.dll"));
            candidates.push(dir.join("../resources/newdb.dll"));
            // Common fallback when running from subfolders.
            candidates.push(dir.join("../libnewdb.dll"));
            candidates.push(dir.join("../newdb.dll"));
        }
    }

    // Fallback for dev-mode working directories.
    candidates.push(PathBuf::from("libnewdb.dll"));
    candidates.push(PathBuf::from("newdb.dll"));
    candidates.push(PathBuf::from("../build_shared/libnewdb.dll"));
    candidates.push(PathBuf::from("../build_shared/newdb.dll"));
    candidates.push(PathBuf::from("../build/libnewdb.dll"));
    candidates.push(PathBuf::from("../build/newdb.dll"));
    candidates.push(PathBuf::from("../../build_shared/libnewdb.dll"));
    candidates.push(PathBuf::from("../../build_shared/newdb.dll"));

    for p in candidates {
        if p.exists() {
            return p;
        }
    }
    PathBuf::from("libnewdb.dll")
}

fn runtime_candidate_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            dirs.push(dir.to_path_buf());
            dirs.push(dir.join("bin"));
            dirs.push(dir.join("../bin"));
            dirs.push(dir.join("resources"));
            dirs.push(dir.join("../resources"));
        }
    }
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

fn load_dll_api() -> Result<&'static DllApi, String> {
    DLL_API.get_or_try_init(|| {
        configure_runtime_loader_paths();
        let lib_path = resolve_newdb_dll();
        let lib = unsafe { Library::new(&lib_path) }
            .map_err(|e| format!("load dll failed: {} ({e})", lib_path.display()))?;
        unsafe {
            let version = {
                let sym: Symbol<unsafe extern "C" fn() -> *const std::os::raw::c_char> =
                    lib.get(b"newdb_version_string\0").map_err(|e| e.to_string())?;
                *sym
            };
            let session_create = {
                let sym: Symbol<
                    unsafe extern "C" fn(
                        *const std::os::raw::c_char,
                        *const std::os::raw::c_char,
                        *const std::os::raw::c_char,
                    ) -> *mut std::ffi::c_void,
                > = lib.get(b"newdb_session_create\0").map_err(|e| e.to_string())?;
                *sym
            };
            let session_destroy = {
                let sym: Symbol<unsafe extern "C" fn(*mut std::ffi::c_void)> =
                    lib.get(b"newdb_session_destroy\0").map_err(|e| e.to_string())?;
                *sym
            };
            let session_set_table = {
                let sym: Symbol<
                    unsafe extern "C" fn(*mut std::ffi::c_void, *const std::os::raw::c_char) -> i32,
                > = lib.get(b"newdb_session_set_table\0").map_err(|e| e.to_string())?;
                *sym
            };
            let session_execute = {
                let sym: Symbol<
                    unsafe extern "C" fn(
                        *mut std::ffi::c_void,
                        *const std::os::raw::c_char,
                        *mut std::os::raw::c_char,
                        usize,
                    ) -> i32,
                > = lib.get(b"newdb_session_execute\0").map_err(|e| e.to_string())?;
                *sym
            };
            let session_where_plan_json = {
                let sym: Symbol<
                    unsafe extern "C" fn(
                        *mut std::ffi::c_void,
                        i32,
                        *const *const std::os::raw::c_char,
                        *mut std::os::raw::c_char,
                        usize,
                    ) -> i32,
                > = lib.get(b"newdb_session_where_plan_json\0").map_err(|e| e.to_string())?;
                *sym
            };
            Ok(DllApi {
                _lib: lib,
                version,
                session_create,
                session_destroy,
                session_set_table,
                session_execute,
                session_where_plan_json,
            })
        }
    })
}

fn as_cstring(s: &str) -> std::ffi::CString {
    std::ffi::CString::new(s).unwrap_or_else(|_| std::ffi::CString::new("").expect("empty cstr"))
}

fn dll_log_path_for(st: &SessionState) -> String {
    if st.data_dir.trim().is_empty() {
        "gui_dll.log".to_string()
    } else {
        Path::new(&st.data_dir)
            .join("gui_dll.log")
            .to_string_lossy()
            .to_string()
    }
}

fn normalize_table(st: &SessionState) -> String {
    st.current_table.trim().to_string()
}

fn table_bin_exists(st: &SessionState, table: &str) -> bool {
    if st.data_dir.trim().is_empty() || table.trim().is_empty() {
        return false;
    }
    Path::new(&st.data_dir).join(format!("{table}.bin")).exists()
}

fn pick_preload_table(st: &SessionState) -> Option<String> {
    let table = normalize_table(st);
    if !table.is_empty() && table_bin_exists(st, &table) {
        return Some(table);
    }
    None
}

fn run_demo(st: &SessionState, extra: &[&str]) -> Result<String, String> {
    configure_runtime_loader_paths();
    let demo_bin = resolve_demo_bin()?;
    let mut cmd = Command::new(demo_bin);
    // Disable env-driven implicit defaults (especially NEWDB_TABLE=users).
    cmd.env_remove("NEWDB_TABLE");
    cmd.env_remove("NEWDB_DATA_DIR");
    cmd.env_remove("NEWDB_LOG");
    if !st.data_dir.trim().is_empty() {
        cmd.arg("--data-dir").arg(&st.data_dir);
    }
    // Use an ephemeral log file to avoid replaying large history on every invocation.
    let log_base = if st.data_dir.trim().is_empty() {
        std::env::temp_dir()
    } else {
        PathBuf::from(&st.data_dir)
    };
    let log_name = format!(
        "gui_runtime_{}_{}.bin",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );
    let log_path = log_base.join(log_name);
    cmd.arg("--log-file").arg(log_path.to_string_lossy().to_string());
    if let Some(table) = pick_preload_table(st) {
        cmd.arg("--table").arg(table);
    }
    for a in extra {
        cmd.arg(a);
    }
    #[cfg(windows)]
    {
        // Prevent extra console windows and reduce UX impact of child process failures.
        cmd.creation_flags(0x08000000);
    }
    let out = cmd.output().map_err(|e| format!("failed to run newdb_demo: {e}"))?;
    let _ = fs::remove_file(&log_path);
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

fn reset_dll_session(api: &DllApi, dll: &mut DllSessionState) {
    if let Some(h) = dll.handle.take() {
        unsafe { (api.session_destroy)(h as *mut std::ffi::c_void) };
    }
    dll.data_dir.clear();
}

fn ensure_dll_session(
    api: &DllApi,
    st: &SessionState,
    dll: &mut DllSessionState,
) -> Result<usize, String> {
    // Recreate session when workspace changes.
    if dll.handle.is_some() && dll.data_dir != st.data_dir {
        reset_dll_session(api, dll);
    }
    if let Some(h) = dll.handle {
        return Ok(h);
    }
    let data_dir = as_cstring(&st.data_dir);
    let table = as_cstring("");
    let log_name = as_cstring(&dll_log_path_for(st));
    let h = unsafe { (api.session_create)(data_dir.as_ptr(), table.as_ptr(), log_name.as_ptr()) };
    if h.is_null() {
        return Err("newdb_session_create returned null".to_string());
    }
    dll.handle = Some(h as usize);
    dll.data_dir = st.data_dir.clone();
    Ok(h as usize)
}

fn merge_where_plan_c_api_to_show_plan_shell(js: &serde_json::Value) -> Option<String> {
    if js.get("ok")?.as_i64()? != 1 {
        return None;
    }
    let candidates_in = js.get("candidates")?.as_array()?.clone();
    let chosen_plan = js.get("chosen_plan_id").and_then(|v| v.as_str());
    let mut plan_candidates: Vec<serde_json::Value> = Vec::new();
    for c in &candidates_in {
        let id = c.get("id")?.as_str()?;
        let estimated_cost = c.get("estimated_cost")?.as_f64()?;
        let rows = c.get("estimated_rows").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let rationale = c.get("rationale").and_then(|v| v.as_str()).unwrap_or("");
        let is_ch = chosen_plan == Some(id);
        let mut ent = serde_json::json!({
            "id": id,
            "estimated_cost": estimated_cost,
            "cost": {"estimated_rows": rows},
            "rationale": rationale,
        });
        if let Some(m) = ent.as_object_mut() {
            if is_ch {
                m.insert("chosen".to_string(), serde_json::json!(true));
            } else if chosen_plan.is_some() && chosen_plan != Some("") {
                m.insert(
                    "reason_rejected".to_string(),
                    serde_json::json!("not_chosen"),
                );
            }
        }
        plan_candidates.push(ent);
    }
    let considered = plan_candidates.len().max(1);
    let chosen = js
        .get("chosen_plan_id")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let chosen_reason = js
        .get("chosen_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let out = serde_json::json!({
        "plan_id": chosen,
        "chosen_reason": chosen_reason,
        "logical_rows": 0,
        "matched_rows": 0,
        "estimated_scan_rows": 0,
        "plan_candidates_considered": considered,
        "plan_candidates": plan_candidates,
        "table_stats_stale": false,
        "path": "where_executor",
        "snapshot_lsn": 0,
        "snapshot_source": "",
        "readpath_enabled": true,
        "delta": {
            "fallback_scans": 0,
            "eq_sidecar": 0,
            "id_pk": 0,
            "plan_fallback": 0,
            "rows_scanned": 0,
            "rows_returned": 0
        }
    });
    serde_json::to_string(&out).ok()
}

fn try_execute_show_plan_via_where_plan_json(
    api: &DllApi,
    handle: usize,
    command_line: &str,
) -> Option<Result<String, String>> {
    let t = command_line.trim_start();
    let prefix = "SHOW PLAN";
    if t.len() < prefix.len() {
        return None;
    }
    if !t[..prefix.len()].eq_ignore_ascii_case(prefix) {
        return None;
    }
    let tail = t[prefix.len()..].trim();
    if tail.is_empty() {
        return None;
    }
    let tokens: Vec<&str> = tail.split_whitespace().collect();
    if tokens.len() < 3 {
        return None;
    }
    let argv_c: Vec<std::ffi::CString> = tokens.iter().map(|s| as_cstring(s)).collect();
    let argv_ptrs: Vec<*const std::os::raw::c_char> =
        argv_c.iter().map(|c| c.as_ptr()).collect();
    let argc = argv_ptrs.len() as i32;
    let mut buf = vec![0_i8; 256 * 1024];
    let ok = unsafe {
        (api.session_where_plan_json)(
            handle as *mut std::ffi::c_void,
            argc,
            argv_ptrs.as_ptr(),
            buf.as_mut_ptr(),
            buf.len(),
        )
    };
    let out_u8: Vec<u8> = buf
        .into_iter()
        .take_while(|b| *b != 0)
        .map(|b| b as u8)
        .collect();
    let text = String::from_utf8_lossy(&out_u8).trim().to_string();
    if ok == 0 {
        return Some(Err(if text.is_empty() {
            "newdb_session_where_plan_json failed".to_string()
        } else {
            text
        }));
    }
    let v: serde_json::Value =
        match serde_json::from_str(&text) {
            Ok(x) => x,
            Err(_) => return Some(Ok(text + "\n")),
        };
    if let Some(merged) = merge_where_plan_c_api_to_show_plan_shell(&v) {
        return Some(Ok(merged));
    }
    Some(Ok(text + "\n"))
}

fn execute_via_dll_session(
    api: &DllApi,
    handle: usize,
    st: &SessionState,
    command_line: &str,
) -> Result<String, String> {
    if let Some(r) = try_execute_show_plan_via_where_plan_json(api, handle, command_line) {
        return r;
    }
    let table = as_cstring(&normalize_table(st));
    let cmd = as_cstring(command_line);
    let mut out = vec![0_i8; 256 * 1024];
    let out_u8 = unsafe {
        if !table.to_bytes().is_empty() {
            let set_ok = (api.session_set_table)(handle as *mut std::ffi::c_void, table.as_ptr());
            if set_ok == 0 {
                return Err("newdb_session_set_table failed".to_string());
            }
        }
        let ok = (api.session_execute)(
            handle as *mut std::ffi::c_void,
            cmd.as_ptr(),
            out.as_mut_ptr(),
            out.len(),
        );
        let out_u8: Vec<u8> = out.into_iter().take_while(|b| *b != 0).map(|b| b as u8).collect();
        if ok == 0 {
            let msg = String::from_utf8_lossy(&out_u8).trim().to_string();
            if msg.is_empty() {
                return Err("newdb_session_execute failed".to_string());
            }
            return Err(msg);
        }
        out_u8
    };
    Ok(String::from_utf8_lossy(&out_u8).to_string())
}

fn should_fallback_to_demo(err: &str) -> bool {
    err.contains("newdb_session_set_table failed")
        || err.contains("newdb_session_execute failed")
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
    /// Stable `newdb_session_last_error` style code when the C API emits `numeric=` (optional).
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimedScriptResult {
    ok: bool,
    output: String,
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

fn run_powershell_script_timed(mut cmd: Command) -> Result<TimedScriptResult, String> {
    let begin = std::time::Instant::now();
    let out = cmd.output().map_err(|e| format!("failed to run powershell: {e}"))?;
    let mut all = String::new();
    all.push_str(&String::from_utf8_lossy(&out.stdout));
    all.push_str(&String::from_utf8_lossy(&out.stderr));
    let elapsed_ms = begin.elapsed().as_millis() as u64;
    Ok(TimedScriptResult {
        ok: out.status.success(),
        output: all,
        elapsed_ms,
    })
}

fn stack_store_file_from_workspace(workspace: &str) -> PathBuf {
    let root = if workspace.trim().is_empty() {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
    } else {
        PathBuf::from(workspace)
    };
    root.join(".newdb_gui").join("undo_redo_stack.jsonl")
}

fn stack_store_file_from_state(state: &State<'_, AppState>) -> Result<PathBuf, String> {
    let app = state.inner();
    let st = app.st.lock().map_err(|_| "state lock poisoned".to_string())?;
    Ok(stack_store_file_from_workspace(&st.data_dir))
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
    let payload = serde_json::json!({
        "schema_version": "newdb.gui.undo_redo_stack.v1",
        "updated_at_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        "undo_units": serde_json::from_str::<serde_json::Value>(&undo_units_json).unwrap_or(serde_json::json!([])),
        "redo_units": serde_json::from_str::<serde_json::Value>(&redo_units_json).unwrap_or(serde_json::json!([])),
    });
    let line = serde_json::to_string(&payload).map_err(|e| format!("stack serialize failed: {e}"))?;
    fs::write(&p, format!("{line}\n")).map_err(|e| format!("failed to write {}: {e}", p.display()))?;
    Ok(())
}

#[tauri::command]
fn load_stack_units(state: State<'_, AppState>) -> Result<String, String> {
    let p = stack_store_file_from_state(&state)?;
    if !p.exists() {
        return Ok("{\"undo_units\":[],\"redo_units\":[],\"warnings\":[]}".to_string());
    }
    let text = fs::read_to_string(&p).map_err(|e| format!("failed to read {}: {e}", p.display()))?;
    let (undo_units, redo_units, warnings) = parse_stack_payload_text(&text);
    Ok(
        serde_json::json!({
            "undo_units": undo_units,
            "redo_units": redo_units,
            "warnings": warnings
        })
        .to_string(),
    )
}

fn stack_undo_unit_with_runner<F>(unit: &StackUndoUnit, mut run: F) -> StackExecResult
where
    F: FnMut(String) -> Result<CommandExecResult, String>,
{
    let begin = std::time::Instant::now();
    let mut repair_action = "none".to_string();
    let mut failed_op: Option<String> = None;
    let mut applied_ops = 0usize;
    let mut executed_commands: Vec<String> = Vec::new();

    let savepoint = unit.savepoint_name.trim().to_string();
    let is_internal_unit = savepoint.starts_with("__");
    if !savepoint.is_empty() && !is_internal_unit {
        let cmd = format!("ROLLBACK TO {savepoint}");
        executed_commands.push(cmd.clone());
        match run(cmd.clone()) {
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
        match run_compound_command(backward, &mut run, &mut executed_commands) {
            Ok(()) => {
                applied_ops += 1;
            }
            Err(failed_cmd) => {
                executed_commands.push("ROLLBACK".to_string());
                let _ = run("ROLLBACK".to_string());
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

fn stack_redo_unit_with_runner<F>(unit: &StackUndoUnit, mut run: F) -> StackExecResult
where
    F: FnMut(String) -> Result<CommandExecResult, String>,
{
    let begin = std::time::Instant::now();
    let mut applied_ops = 0usize;
    let mut executed_commands: Vec<String> = Vec::new();
    for op in &unit.ops {
        if op.forward.trim().is_empty() {
            continue;
        }
        match run_compound_command(&op.forward, &mut run, &mut executed_commands) {
            Ok(()) => {
                applied_ops += 1;
            }
            Err(failed_cmd) => {
                executed_commands.push("ROLLBACK".to_string());
                let _ = run("ROLLBACK".to_string());
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
    Ok(stack_undo_unit_with_runner(&unit, |cmd| execute_command_ex(state.clone(), cmd)))
}

#[tauri::command]
fn stack_redo_unit(state: State<'_, AppState>, unit: StackUndoUnit) -> Result<StackExecResult, String> {
    Ok(stack_redo_unit_with_runner(&unit, |cmd| execute_command_ex(state.clone(), cmd)))
}

#[tauri::command]
fn export_bundle(state: State<'_, AppState>, dest_zip: Option<String>) -> Result<String, String> {
    configure_runtime_loader_paths();
    let app = state.inner();
    let st = app.st.lock().expect("state lock");
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
        results_dir.join(format!("newdb_gui_bundle_{ts}.zip"))
    };
    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create bundle dir {}: {e}", parent.display()))?;
    }

    let file = fs::File::create(&out_path)
        .map_err(|e| format!("failed to create {}: {e}", out_path.display()))?;
    let mut zip = zip::ZipWriter::new(file);
    let options = FileOptions::<()>::default().compression_method(zip::CompressionMethod::Deflated);

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
        "schema_version": "newdb.gui.bundle_manifest.v1",
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
    Ok(out_path.display().to_string())
}
#[tauri::command]
fn run_redo_undo_gate(state: State<'_, AppState>, idempotent_mode: Option<String>) -> Result<TimedScriptResult, String> {
    configure_runtime_loader_paths();
    let app = state.inner();
    let st = app.st.lock().expect("state lock");
    let script = resolve_script_path("ci/redo_undo_recovery_gate.ps1")?;
    let demo_bin = resolve_demo_bin()?;
    let build_dir = demo_bin
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("build_mingw"));
    let mode = idempotent_mode.unwrap_or_else(|| "both".to_string());

    let mut cmd = Command::new("powershell");
    cmd.arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg(script)
        .arg("-BuildDir")
        .arg(build_dir)
        .arg("-IdempotentMode")
        .arg(mode)
        .env("NEWDB_DATA_DIR", st.data_dir.clone());
    #[cfg(windows)]
    {
        cmd.creation_flags(0x08000000);
    }
    Ok(run_powershell_script_timed(cmd)?)
}

#[tauri::command]
fn run_nightly_pressure_profile_compare(state: State<'_, AppState>) -> Result<TimedScriptResult, String> {
    configure_runtime_loader_paths();
    let _app = state.inner();
    let script = resolve_script_path("ci/nightly_pressure_profile_compare.ps1")?;
    let demo_bin = resolve_demo_bin()?;
    let build_dir = demo_bin
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("build_mingw"));
    let mut cmd = Command::new("powershell");
    cmd.arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg(script)
        .arg("-BuildDir")
        .arg(build_dir);
    #[cfg(windows)]
    {
        cmd.creation_flags(0x08000000);
    }
    Ok(run_powershell_script_timed(cmd)?)
}
#[tauri::command]
fn txn_begin(state: State<'_, AppState>, table: Option<String>) -> Result<TimedExecResult, String> {
    let mut cmd = "BEGIN".to_string();
    if let Some(t) = table {
        let t2 = t.trim().to_string();
        if !t2.is_empty() {
            cmd.push(' ');
            cmd.push_str(&t2);
        }
    }
    exec_timed(state, cmd)
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
    exec_timed(state, format!("ROLLBACK TO {sp}"))
}

#[tauri::command]
fn txn_release_savepoint(state: State<'_, AppState>, name: String) -> Result<TimedExecResult, String> {
    let sp = name.trim();
    if sp.is_empty() {
        return Err("empty savepoint".to_string());
    }
    exec_timed(state, format!("RELEASE SAVEPOINT {sp}"))
}

#[tauri::command]
fn pitr_recover_to_lsn(state: State<'_, AppState>, lsn: u64) -> Result<TimedExecResult, String> {
    if lsn == 0 {
        return Err("invalid target lsn".to_string());
    }
    exec_timed(state, format!("RECOVER TO LSN {lsn}"))
}

#[tauri::command]
fn pitr_recover_to_time(state: State<'_, AppState>, ts_ms: u64) -> Result<TimedExecResult, String> {
    if ts_ms == 0 {
        return Err("invalid target ts_ms".to_string());
    }
    exec_timed(state, format!("RECOVER TO TIME {ts_ms}"))
}

#[cfg(test)]
mod tests {
    use super::{
        make_command_exec_result, parse_capi_error_code, parse_stack_payload_text, should_stop_script_on_output,
        stack_redo_unit_with_runner, stack_undo_unit_with_runner, strip_capi_error_line, StackUndoOp, StackUndoUnit,
    };

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
        let r = stack_undo_unit_with_runner(&unit, |cmd| {
            called.push(cmd.clone());
            Ok(make_command_exec_result("[OK]".to_string()))
        });
        assert!(r.ok);
        assert_eq!(called.first().map(|s| s.as_str()), Some("ROLLBACK TO sp1"));
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
        let r = stack_undo_unit_with_runner(&unit, |cmd| {
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
        let r = stack_redo_unit_with_runner(&unit, |cmd| {
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

fn schema_headers_from_attr(st: &SessionState) -> Vec<String> {
    schema_columns_from_attr(st).into_iter().map(|c| c.name).collect()
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
    let api = load_dll_api().ok();
    let app = state.inner();
    let mut st = app.st.lock().expect("state lock");
    let mut dll = app.dll.lock().expect("dll lock");
    st.data_dir = data_dir;
    if let Some(api) = api {
        reset_dll_session(api, &mut dll);
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
    parse_list_tables_from_fs(&st.data_dir)
}

#[tauri::command]
fn execute_command(state: State<'_, AppState>, command: String) -> Result<String, String> {
    let app = state.inner();
    let mut st = app.st.lock().expect("state lock");
    // Prefer DLL persistent session so txn BEGIN/ROLLBACK works.
    let out = if let Ok(api) = load_dll_api() {
        let mut dll = app.dll.lock().expect("dll lock");
        let h = ensure_dll_session(api, &st, &mut dll)?;
        match execute_via_dll_session(api, h, &st, &command) {
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
        // Fallback to exe (stateless).
        run_demo_mdb(&st, &command)?
    };
    // Persist selected table across command invocations.
    if let Some(t) = parse_use_table(&command) {
        st.current_table = t;
    }
    Ok(out)
}

#[tauri::command]
fn execute_command_ex(state: State<'_, AppState>, command: String) -> Result<CommandExecResult, String> {
    let out = execute_command(state, command)?;
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
    let mut page_no = 1usize;
    for _ in 0..max_pages {
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
        if page.rows.len() < page_size {
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
            for (name, _, _) in entries.iter().filter(|(n, _, _)| n != "id") {
                let idx = page.headers.iter().position(|h| h.trim() == name.as_str());
                if let Some(ci) = idx {
                    vals.push(row.get(ci).cloned().unwrap_or_default());
                } else {
                    vals.push(String::new());
                }
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
    if up.starts_with("DEFATTR(") || op == "schema_defattr" {
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
        let show_out = execute_command(state.clone(), "SHOW ATTR".to_string()).ok()?;
        let entries = parse_show_attr_entries(&show_out);
        if entries.is_empty() {
            return None;
        }
        let has_dropped = entries.iter().any(|(name, _, _)| name == dropped);
        if !has_dropped {
            return None;
        }
        let non_id = entries
            .iter()
            .filter(|(name, _, _)| name != "id")
            .map(|(name, ty, _)| format!("{name}:{ty}"))
            .collect::<Vec<_>>();
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
        return Some((backward, "backend:snapshot_delattr".to_string()));
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
        let old_val = row_map.get(attr).cloned().unwrap_or_default();
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
        ordered.push(row_map.get(attr).cloned().unwrap_or_default());
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

#[tauri::command]
fn run_script(state: State<'_, AppState>, script: String) -> Result<String, String> {
    let app = state.inner();
    let mut st = app.st.lock().expect("state lock");
    // Prefer DLL persistent session so txn blocks spanning lines work.
    let out = if let Ok(api) = load_dll_api() {
        let mut dll = app.dll.lock().expect("dll lock");
        let h = ensure_dll_session(api, &st, &mut dll)?;
        let mut all = String::new();
        for (line_no, line) in script.lines().enumerate() {
            let t = line.trim();
            if t.is_empty() || t.starts_with('#') {
                continue;
            }
            if let Some(table) = parse_use_table(t) {
                st.current_table = table;
            }
            let one = match execute_via_dll_session(api, h, &st, t) {
                Ok(s) => s,
                Err(e) => {
                    if should_fallback_to_demo(&e) {
                        run_demo_mdb(&st, t)?
                    } else {
                        e
                    }
                }
            };
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
        all
    } else {
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
            if let Some(table) = parse_use_table(t) {
                st.current_table = table;
            }
            let one = run_demo_mdb(&st, t)?;
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
        all
    };
    Ok(out)
}

#[tauri::command]
fn run_script_ex(state: State<'_, AppState>, script: String) -> Result<ScriptExecResult, String> {
    let out = run_script(state, script)?;
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
        ok: error_code.is_none() && stop_line.is_none(),
        output: strip_capi_error_line(&out),
        error_code,
        stop_line,
    })
}

#[tauri::command]
fn run_perf_bench(
    state: State<'_, AppState>,
    sizes_csv: Option<String>,
    query_loops: Option<u32>,
    txn_per_mode: Option<u32>,
    build_chunk_size: Option<u32>,
) -> Result<String, String> {
    configure_runtime_loader_paths();
    let app = state.inner();
    let st = app.st.lock().expect("state lock");

    let perf_bin = resolve_perf_bin()?;
    let demo_bin = resolve_demo_bin()?;
    let data_dir = if st.data_dir.trim().is_empty() {
        std::env::current_dir()
            .ok()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default()
    } else {
        st.data_dir.clone()
    };

    let mut cmd = Command::new(perf_bin);
    cmd.arg("--demo-exe")
        .arg(demo_bin)
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--sizes")
        .arg(sizes_csv.unwrap_or_else(|| "1000000".to_string()))
        .arg("--query-loops")
        .arg(query_loops.unwrap_or(1).to_string())
        .arg("--txn-per-mode")
        .arg(txn_per_mode.unwrap_or(60).to_string())
        .arg("--build-chunk-size")
        .arg(build_chunk_size.unwrap_or(50000).to_string());
    #[cfg(windows)]
    {
        cmd.creation_flags(0x08000000);
    }
    let out = cmd.output().map_err(|e| format!("failed to run newdb_perf: {e}"))?;
    let mut all = String::new();
    all.push_str(&String::from_utf8_lossy(&out.stdout));
    all.push_str(&String::from_utf8_lossy(&out.stderr));
    Ok(all)
}

#[tauri::command]
fn run_nightly_soak(
    state: State<'_, AppState>,
    runs: Option<u32>,
    soak_minutes: Option<u32>,
    sleep_seconds_between_runs: Option<u32>,
    continue_on_failure: Option<bool>,
    skip_build_and_core_gates: Option<bool>,
) -> Result<String, String> {
    configure_runtime_loader_paths();
    let app = state.inner();
    let st = app.st.lock().expect("state lock");
    let script = resolve_script_path("soak/nightly_soak_runner.ps1")?;
    let demo_bin = resolve_demo_bin()?;
    let data_dir = if st.data_dir.trim().is_empty() {
        std::env::current_dir()
            .ok()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default()
    } else {
        st.data_dir.clone()
    };
    // Prefer using the resolved demo binary directory as build root; it is the most
    // reliable indicator for packaged/dev layouts.
    let build_dir = demo_bin
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("build_mingw"));

    let mut cmd = Command::new("powershell");
    cmd.arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg(script)
        .arg("-BuildDir")
        .arg(build_dir)
        .arg("-DemoExe")
        .arg(demo_bin)
        .arg("-DataDir")
        .arg(data_dir)
        .arg("-Table")
        .arg(if st.current_table.trim().is_empty() {
            "qa_regression"
        } else {
            st.current_table.trim()
        })
        .arg("-Runs")
        .arg(runs.unwrap_or(1).to_string())
        .arg("-SoakMinutes")
        .arg(soak_minutes.unwrap_or(30).to_string())
        .arg("-SleepSecondsBetweenRuns")
        .arg(sleep_seconds_between_runs.unwrap_or(30).to_string());
    if continue_on_failure.unwrap_or(false) {
        cmd.arg("-ContinueOnFailure");
    }
    if skip_build_and_core_gates.unwrap_or(false) {
        cmd.arg("-SkipBuildAndCoreGates");
    }
    #[cfg(windows)]
    {
        cmd.creation_flags(0x08000000);
    }
    let out = cmd
        .output()
        .map_err(|e| format!("failed to run nightly soak script: {e}"))?;
    let mut all = String::new();
    all.push_str(&String::from_utf8_lossy(&out.stdout));
    all.push_str(&String::from_utf8_lossy(&out.stderr));
    if out.status.success() {
        Ok(all)
    } else {
        Err(all)
    }
}

#[tauri::command]
fn run_concurrent_pressure_bench(
    state: State<'_, AppState>,
    jobs: Option<u32>,
    batches: Option<u32>,
    batch_size: Option<u32>,
    segment_target_bytes: Option<u32>,
    sidecar_invalidate_every_n: Option<u32>,
    lsm_compaction_workers: Option<u32>,
    lsm_compaction_reap_budget: Option<u32>,
    lsm_l0_compact_trigger: Option<u32>,
    lsm_l0_compact_batch: Option<u32>,
    lsm_flush_trigger_multiplier: Option<u32>,
    repeat_until_fail: Option<u32>,
) -> Result<String, String> {
    configure_runtime_loader_paths();
    let app = state.inner();
    let _st = app.st.lock().expect("state lock");
    let script = resolve_script_path("bench/concurrent_pressure_bench.ps1")?;
    let demo_bin = resolve_demo_bin()?;
    // Note: the bench script runs its own isolated workspace under TEMP.
    // We keep GUI state.workspace as the default for interactive shells, but
    // pressure bench is intentionally self-contained.
    let build_dir = demo_bin
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("build_mingw"));

    let mut cmd = Command::new("powershell");
    cmd.arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg(script)
        .arg("-BuildDir")
        .arg(build_dir)
        .arg("-RuntimePressureBatches")
        .arg(batches.unwrap_or(64).to_string())
        .arg("-RuntimePressureBatchSize")
        .arg(batch_size.unwrap_or(500).to_string())
        .arg("-RuntimeSampleEveryBatches")
        .arg("4")
        .arg("-RuntimeProgressEveryBatches")
        .arg("1")
        .arg("-RuntimeProgressEveryRows")
        .arg("100")
        .arg("-RuntimeLsmSegmentTargetBytes")
        .arg(segment_target_bytes.unwrap_or(256).to_string())
        .arg("-RuntimeSidecarInvalidateEveryN")
        .arg(sidecar_invalidate_every_n.unwrap_or(16).to_string())
        .arg("-RuntimeLsmCompactionWorkers")
        .arg(lsm_compaction_workers.unwrap_or(2).to_string())
        .arg("-RuntimeLsmCompactionReapBudget")
        .arg(lsm_compaction_reap_budget.unwrap_or(4).to_string())
        .arg("-RuntimeLsmL0CompactTrigger")
        .arg(lsm_l0_compact_trigger.unwrap_or(8).to_string())
        .arg("-RuntimeLsmL0CompactBatch")
        .arg(lsm_l0_compact_batch.unwrap_or(12).to_string())
        .arg("-RuntimeLsmFlushTriggerMultiplier")
        .arg(lsm_flush_trigger_multiplier.unwrap_or(2).to_string())
        .arg("-Jobs")
        .arg(jobs.unwrap_or(16).to_string())
        .arg("-RepeatUntilFail")
        .arg(repeat_until_fail.unwrap_or(2).to_string());

    // Enable the high-throughput runtime profile.
    cmd.arg("-RuntimeSidecarInvalidateAsync")
        .arg("-RuntimeQuietSessionLog")
        .arg("-RuntimeUseBulkInsertFast")
        .arg("-RuntimeLsmCompactionAsync")
        .arg("-RuntimeEchoProgress");

    // Make output visible for the GUI log panel.
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    #[cfg(windows)]
    {
        cmd.creation_flags(0x08000000);
    }
    let out = cmd
        .output()
        .map_err(|e| format!("failed to run concurrent pressure bench: {e}"))?;
    let mut all = String::new();
    all.push_str(&String::from_utf8_lossy(&out.stdout));
    all.push_str(&String::from_utf8_lossy(&out.stderr));
    if out.status.success() {
        Ok(all)
    } else {
        Err(all)
    }
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
    let mut st = app.st.lock().expect("state lock");
    st.page_size = page_size.max(1);
    if normalize_table(&st).is_empty() {
        return Ok(PageResult {
            headers: vec!["id".to_string()],
            columns: vec![ColumnMeta { name: "id".to_string(), ty: "int".to_string() }],
            rows: Vec::new(),
            raw: "[INFO] no table selected".to_string(),
        });
    }
    let p = page_no.max(1).to_string();
    let s = st.page_size.to_string();
    let order = if order_key.trim().is_empty() { "id" } else { order_key.trim() };
    let mut args = vec!["--page", p.as_str(), s.as_str(), "--order", order, "--page-json"];
    if descending {
        args.push("--desc");
    }
    let out = run_demo(&st, &args)?;
    if let Some(mut parsed_json) = parse_page_json_output(&out) {
        if parsed_json.columns.is_empty() {
            parsed_json.columns = schema_columns_from_attr(&st);
        }
        if parsed_json.headers.is_empty() {
            parsed_json.headers = schema_headers_from_attr(&st);
        }
        return Ok(parsed_json);
    }
    let parsed = parse_page_output(&out);
    if parsed.headers.len() == 1 && parsed.headers[0] == "raw" {
        let mut headers = schema_headers_from_attr(&st);
        if headers.is_empty() || headers[0] != "#".to_string() {
            headers.insert(0, "#".to_string());
        }
        let columns = schema_columns_from_attr(&st);
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
struct DllInfo {
    loaded: bool,
    version: String,
    path: String,
    message: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeArtifactInfo {
    gui_exe_path: String,
    gui_exe_modified: String,
    demo_path: String,
    demo_modified: String,
    perf_path: String,
    perf_modified: String,
    runtime_report_path: String,
    runtime_report_modified: String,
    dll_path: String,
    dll_modified: String,
    /// Contract label for `SHOW TUNING JSON` / JSONL stats (engine and GUI stay aligned).
    runtime_stats_schema_version: String,
    /// Best-effort: set `NEWDB_GIT_COMMIT` in CI pack scripts to populate.
    backend_git_commit: String,
    /// `Release` / `RelWithDebInfo` / etc. when `NEWDB_BUILD_PROFILE` is set at pack time.
    build_profile: String,
    /// Tauri / GUI build: `debug` vs `release` (from `cfg!(debug_assertions)`).
    gui_package_kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PressureBenchProfile {
    jobs: u32,
    batches: u32,
    batch_size: u32,
    segment_target_bytes: u32,
    sidecar_invalidate_every_n: u32,
    lsm_compaction_workers: u32,
    lsm_compaction_reap_budget: u32,
    lsm_l0_compact_trigger: u32,
    lsm_l0_compact_batch: u32,
    lsm_flush_trigger_multiplier: u32,
    repeat_until_fail: u32,
    source_summary: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PressureBenchSummaryItem {
    path: String,
    timestamp: String,
    benchmark_profile: String,
    runtime_walsync_mode: String,
    runtime_pressure_tps_est: f64,
    runtime_pressure_batch_ms_p95: f64,
}

impl Default for PressureBenchProfile {
    fn default() -> Self {
        Self {
            jobs: 16,
            batches: 64,
            batch_size: 500,
            segment_target_bytes: 256,
            sidecar_invalidate_every_n: 16,
            lsm_compaction_workers: 2,
            lsm_compaction_reap_budget: 4,
            lsm_l0_compact_trigger: 8,
            lsm_l0_compact_batch: 12,
            lsm_flush_trigger_multiplier: 2,
            repeat_until_fail: 2,
            source_summary: "default".to_string(),
        }
    }
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
    let gui_exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("unknown"));
    let demo = resolve_demo_bin().unwrap_or_else(|_| PathBuf::from("not-found"));
    let perf = resolve_perf_bin().unwrap_or_else(|_| PathBuf::from("not-found"));
    let runtime_report = resolve_runtime_report_bin().unwrap_or_else(|_| PathBuf::from("not-found"));
    let dll = resolve_newdb_dll();
    let backend_git_commit = std::env::var("NEWDB_GIT_COMMIT").unwrap_or_default();
    let build_profile = std::env::var("NEWDB_BUILD_PROFILE").unwrap_or_default();
    let gui_package_kind = if cfg!(debug_assertions) {
        "debug".to_string()
    } else {
        "release".to_string()
    };
    RuntimeArtifactInfo {
        gui_exe_path: gui_exe.display().to_string(),
        gui_exe_modified: format_modified_time(&gui_exe),
        demo_path: demo.display().to_string(),
        demo_modified: format_modified_time(&demo),
        perf_path: perf.display().to_string(),
        perf_modified: format_modified_time(&perf),
        runtime_report_path: runtime_report.display().to_string(),
        runtime_report_modified: format_modified_time(&runtime_report),
        dll_path: dll.display().to_string(),
        dll_modified: format_modified_time(&dll),
        runtime_stats_schema_version: "newdb.runtime_stats.v1".to_string(),
        backend_git_commit,
        build_profile,
        gui_package_kind,
    }
}

#[tauri::command]
fn runtime_trend_dashboard_json() -> Result<String, String> {
    let dashboard = resolve_results_file("runtime_trend_dashboard.json")?;
    fs::read_to_string(&dashboard)
        .map_err(|e| format!("failed to read {}: {e}", dashboard.display()))
}

#[tauri::command]
fn suggest_concurrent_pressure_profile() -> Result<PressureBenchProfile, String> {
    let script = resolve_script_path("bench/concurrent_pressure_bench.ps1")?;
    let results_dir = script
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("results"))
        .ok_or("failed to resolve scripts/results directory")?;
    if !results_dir.exists() {
        return Ok(PressureBenchProfile::default());
    }
    let mut files: Vec<PathBuf> = fs::read_dir(&results_dir)
        .map_err(|e| format!("read_dir failed {}: {e}", results_dir.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|s| s.to_str()).unwrap_or_default();
            name.starts_with("concurrent_pressure_summary_") && name.ends_with(".json")
        })
        .collect();
    if files.is_empty() {
        return Ok(PressureBenchProfile::default());
    }
    files.sort_by_key(|p| fs::metadata(p).and_then(|m| m.modified()).ok());
    files.reverse();
    let mut best = PressureBenchProfile::default();
    let mut best_tps = f64::MIN;
    for p in files.into_iter().take(40) {
        let Ok(text) = fs::read_to_string(&p) else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
        let tps = v
            .get("runtime_pressure_tps_est")
            .and_then(|x| x.as_f64())
            .unwrap_or(-1.0);
        if tps < 0.0 {
            continue;
        }
        let mut cand = PressureBenchProfile::default();
        cand.jobs = v.get("jobs").and_then(|x| x.as_u64()).unwrap_or(cand.jobs as u64) as u32;
        cand.batches = v
            .get("runtime_pressure_batches")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.batches as u64) as u32;
        cand.batch_size = v
            .get("runtime_pressure_batch_size")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.batch_size as u64) as u32;
        cand.segment_target_bytes = v
            .get("runtime_lsm_segment_target_bytes")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.segment_target_bytes as u64) as u32;
        cand.sidecar_invalidate_every_n = v
            .get("runtime_sidecar_invalidate_every_n")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.sidecar_invalidate_every_n as u64) as u32;
        cand.lsm_compaction_workers = v
            .get("runtime_lsm_compaction_workers")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.lsm_compaction_workers as u64) as u32;
        cand.lsm_compaction_reap_budget = v
            .get("runtime_lsm_compaction_reap_budget")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.lsm_compaction_reap_budget as u64) as u32;
        cand.lsm_l0_compact_trigger = v
            .get("runtime_lsm_l0_compact_trigger")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.lsm_l0_compact_trigger as u64) as u32;
        cand.lsm_l0_compact_batch = v
            .get("runtime_lsm_l0_compact_batch")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.lsm_l0_compact_batch as u64) as u32;
        cand.lsm_flush_trigger_multiplier = v
            .get("runtime_lsm_flush_trigger_multiplier")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.lsm_flush_trigger_multiplier as u64) as u32;
        cand.repeat_until_fail = v
            .get("repeat_until_fail")
            .and_then(|x| x.as_u64())
            .unwrap_or(cand.repeat_until_fail as u64) as u32;
        cand.source_summary = p.display().to_string();
        if tps > best_tps {
            best_tps = tps;
            best = cand;
        }
    }
    Ok(best)
}

#[tauri::command]
fn list_concurrent_pressure_summaries(limit: Option<u32>) -> Result<Vec<PressureBenchSummaryItem>, String> {
    let script = resolve_script_path("bench/concurrent_pressure_bench.ps1")?;
    let results_dir = script
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join("results"))
        .ok_or("failed to resolve scripts/results directory")?;
    if !results_dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = fs::read_dir(&results_dir)
        .map_err(|e| format!("read_dir failed {}: {e}", results_dir.display()))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|s| s.to_str()).unwrap_or_default();
            name.starts_with("concurrent_pressure_summary_") && name.ends_with(".json")
        })
        .collect();
    files.sort_by_key(|p| fs::metadata(p).and_then(|m| m.modified()).ok());
    files.reverse();
    let take_n = limit.unwrap_or(8) as usize;
    let mut out = Vec::new();
    for p in files.into_iter().take(take_n) {
        let Ok(text) = fs::read_to_string(&p) else { continue };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) else { continue };
        out.push(PressureBenchSummaryItem {
            path: p.display().to_string(),
            timestamp: v
                .get("timestamp")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            benchmark_profile: v
                .get("benchmark_profile")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            runtime_walsync_mode: v
                .get("runtime_walsync_mode")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string(),
            runtime_pressure_tps_est: v
                .get("runtime_pressure_tps_est")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0),
            runtime_pressure_batch_ms_p95: v
                .get("runtime_pressure_batch_ms_p95")
                .and_then(|x| x.as_f64())
                .unwrap_or(0.0),
        });
    }
    Ok(out)
}

#[tauri::command]
fn dll_info() -> DllInfo {
    let path = resolve_newdb_dll().display().to_string();
    match load_dll_api() {
        Ok(api) => unsafe {
            let c = (api.version)();
            let version = if c.is_null() {
                "unknown".to_string()
            } else {
                std::ffi::CStr::from_ptr(c).to_string_lossy().to_string()
            };
            DllInfo {
                loaded: true,
                version,
                path,
                message: "dll loaded".to_string(),
            }
        },
        Err(e) => DllInfo {
            loaded: false,
            version: "n/a".to_string(),
            path,
            message: e,
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
            dll: Mutex::new(DllSessionState::default()),
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
            pitr_recover_to_lsn,
            pitr_recover_to_time,
            run_redo_undo_gate,
            run_nightly_pressure_profile_compare,
            save_stack_units,
            load_stack_units,
            stack_undo_unit,
            stack_redo_unit,
            export_bundle,
            run_script,
            run_script_ex,
            run_perf_bench,
            run_concurrent_pressure_bench,
            run_nightly_soak,
            query_page,
            dll_info,
            runtime_artifact_info,
            runtime_trend_dashboard_json,
            list_results_files,
            read_results_json,
            suggest_concurrent_pressure_profile,
            list_concurrent_pressure_summaries,
            cli_terminal_start,
            cli_terminal_write_line,
            cli_terminal_stop
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

pub fn self_check() -> Result<(), String> {
    // 1) Ensure we can resolve binaries/scripts using current layout heuristics.
    let _demo = resolve_demo_bin().ok();
    let _perf = resolve_perf_bin().ok();
    let _runtime_report = resolve_runtime_report_bin().ok();
    let _soak = resolve_script_path("soak/nightly_soak_runner.ps1").ok();

    // 2) Exercise DLL detection path; CI can optionally require it.
    let info = dll_info();
    if std::env::var("NEWDB_GUI_EXPECT_DLL").ok().as_deref() == Some("1") && !info.loaded {
        return Err(format!("DLL expected but not loaded: path={} msg={}", info.path, info.message));
    }

    // 3) Minimal business self-check on a temp workspace.
    //    Run a few core commands via the DLL persistent session and ensure
    //    list_tables can discover the created table on disk.
    let tmp = std::env::temp_dir().join("newdb_gui_self_check");
    fs::create_dir_all(&tmp).map_err(|e| format!("create temp workspace failed: {e}"))?;
    let tmp_dir = tmp.to_string_lossy().to_string();

    if let Ok(api) = load_dll_api() {
        let mut st = SessionState {
            data_dir: tmp_dir.clone(),
            current_table: String::new(),
            page_size: 12,
        };
        let mut dll = DllSessionState::default();
        let h = ensure_dll_session(api, &st, &mut dll)?;

        let exec = |api: &DllApi,
                    h: usize,
                    st: &SessionState,
                    table: &str,
                    cmd: &str|
         -> Result<String, String> {
            let st2 = SessionState {
                data_dir: st.data_dir.clone(),
                current_table: table.to_string(),
                page_size: st.page_size,
            };
            execute_via_dll_session(api, h, &st2, cmd)
        };

        // Create table without pre-setting current_table (so session_set_table won't fail).
        let _ = exec(api, h, &st, "", "CREATE TABLE(users)")?;
        let _ = exec(api, h, &st, "", "USE(users)")?;
        let _ = exec(api, h, &st, "", "DEFATTR(name:string,dept:string,age:int,salary:int)")?;
        let _ = exec(api, h, &st, "", "INSERT(1,Alice,ENG,29,18000)")?;
        let count_out = exec(api, h, &st, "users", "COUNT")?;

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
        let _ = exec(api, h, &st, "users", "BEGIN")?;
        let _ = exec(api, h, &st, "users", "INSERT(2,Bob,ENG,33,22000)")?;
        let _ = exec(api, h, &st, "users", "ROLLBACK")?;
        let count_after_rb = exec(api, h, &st, "users", "COUNT")?;
        let initial_rows = parse_count_rows(&count_out);
        let after_rows = parse_count_rows(&count_after_rb);
        if after_rows != Some(1) {
            // Be conservative: still treat as failure if we can't see the expected count.
            return Err(format!(
                "txn rollback count check failed: expected rows=1; parsed initial={:?} after={:?}; initial_out={:?} after_rb_out={:?}",
                initial_rows, after_rows, count_out, count_after_rb
            ));
        }

        // Pagination JSON smoke (mirrors GUI `query_page` path).
        // It uses the CLI exe output and ensures we can parse a [PAGE_JSON] record.
        st.current_table = "users".to_string();
        let page_raw = run_demo(&st, &["--page", "1", "12", "--order", "id", "--page-json"])?;
        let parsed = parse_page_json_output(&page_raw).ok_or_else(|| {
            format!(
                "PAGE_JSON parse failed; output did not include [PAGE_JSON] payload. raw={:?}",
                page_raw
            )
        })?;
        if parsed.headers.is_empty() {
            return Err("PAGE_JSON headers empty".to_string());
        }

        // Ensure table file is visible for UI tree listing.
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

pub fn e2e_undo_redo_check() -> Result<(), String> {
    let api = load_dll_api().map_err(|e| format!("e2e requires dll api: {e}"))?;
    let tmp = std::env::temp_dir().join(format!(
        "newdb_gui_e2e_undo_redo_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    ));
    fs::create_dir_all(&tmp).map_err(|e| format!("create e2e workspace failed: {e}"))?;
    let tmp_dir = tmp.to_string_lossy().to_string();

    let mut st = SessionState {
        data_dir: tmp_dir.clone(),
        current_table: String::new(),
        page_size: 12,
    };
    let mut dll = DllSessionState::default();
    let h = ensure_dll_session(api, &st, &mut dll)?;

    let exec = |api: &DllApi,
                h: usize,
                st: &SessionState,
                table: &str,
                cmd: &str|
     -> Result<String, String> {
        let st2 = SessionState {
            data_dir: st.data_dir.clone(),
            current_table: table.to_string(),
            page_size: st.page_size,
        };
        execute_via_dll_session(api, h, &st2, cmd)
    };

    fn parse_count_rows(out: &str) -> Option<u64> {
        for token in out.split(|c: char| c == '\n' || c == '\r') {
            if !token.contains("[COUNT]") {
                continue;
            }
            if let Some(idx) = token.find("rows=") {
                let rest = &token[(idx + 5)..];
                let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                if let Ok(v) = digits.parse::<u64>() {
                    return Some(v);
                }
            }
        }
        None
    }

    let _ = exec(api, h, &st, "", "CREATE TABLE(users)")?;
    let _ = exec(api, h, &st, "", "USE(users)")?;
    let _ = exec(api, h, &st, "", "DEFATTR(name:string,dept:string,age:int,salary:int)")?;
    let _ = exec(api, h, &st, "", "CREATE TABLE(orders)")?;
    let _ = exec(api, h, &st, "", "USE(orders)")?;
    let _ = exec(api, h, &st, "", "DEFATTR(user_id:int,amount:int,status:string)")?;

    // Multi-table transaction with savepoint rollback.
    let _ = exec(api, h, &st, "users", "BEGIN")?;
    let _ = exec(api, h, &st, "users", "INSERT(101,Alice,ENG,29,18000)")?;
    let savepoint_users_out = exec(api, h, &st, "users", "SAVEPOINT(sp_users)")?;
    if savepoint_users_out.contains("unknown command") {
        return Err("native SAVEPOINT(...) not supported in DLL session".to_string());
    }
    let _ = exec(api, h, &st, "orders", "USE(orders)")?;
    let _ = exec(api, h, &st, "orders", "INSERT(1,1,300,ok)")?;
    let savepoint_orders_out = exec(api, h, &st, "orders", "SAVEPOINT(sp_orders)")?;
    if savepoint_orders_out.contains("unknown command") {
        return Err("native SAVEPOINT(...) not supported in DLL session".to_string());
    }
    let _ = exec(api, h, &st, "orders", "INSERT(2,1,500,pending)")?;
    let rb_to_out = exec(api, h, &st, "orders", "ROLLBACK TO(sp_orders)")?;
    if rb_to_out.contains("unknown command") {
        return Err("native ROLLBACK TO(...) not supported in DLL session".to_string());
    }
    let release_out = exec(api, h, &st, "orders", "RELEASE SAVEPOINT(sp_users)")?;
    if release_out.contains("unknown command") {
        return Err("native RELEASE SAVEPOINT(...) not supported in DLL session".to_string());
    }
    let _ = exec(api, h, &st, "orders", "COMMIT")?;

    let users_count = parse_count_rows(&exec(api, h, &st, "users", "COUNT")?);
    let orders_count = parse_count_rows(&exec(api, h, &st, "orders", "COUNT")?);
    let expected_orders_after_commit = 1;
    if users_count != Some(1) || orders_count != Some(expected_orders_after_commit) {
        return Err(format!(
            "post-commit count mismatch users={:?} orders={:?} expected_orders={}",
            users_count, orders_count, expected_orders_after_commit
        ));
    }

    let unit = StackUndoUnit {
        unit_id: "e2e_u1".to_string(),
        txn_id: "e2e_txn_1".to_string(),
        savepoint_name: "__commit__".to_string(),
        tables_touched: vec!["users".to_string(), "orders".to_string()],
        ops: vec![
            StackUndoOp {
                forward: "INSERT(101,Alice,ENG,29,18000)".to_string(),
                backward: Some("DELETE(101)".to_string()),
                table: Some("users".to_string()),
            },
            StackUndoOp {
                forward: "INSERT(1,1,300,ok)".to_string(),
                backward: Some("DELETE(1)".to_string()),
                table: Some("orders".to_string()),
            },
        ],
    };

    let undo_r = stack_undo_unit_with_runner(&unit, |cmd| {
        let up = cmd.to_uppercase();
        let table = if up.contains("101") { "users" } else { "orders" };
        execute_command_ex_for_e2e(api, h, &st, table, cmd)
    });
    if !undo_r.ok {
        return Err(format!("stack undo failed: {}", undo_r.message));
    }

    let users_after_undo = parse_count_rows(&exec(api, h, &st, "users", "COUNT")?);
    let orders_after_undo = parse_count_rows(&exec(api, h, &st, "orders", "COUNT")?);
    if users_after_undo != Some(0) || orders_after_undo != Some(0) {
        return Err(format!(
            "post-undo count mismatch users={:?} orders={:?}",
            users_after_undo, orders_after_undo
        ));
    }

    let redo_r = stack_redo_unit_with_runner(&unit, |cmd| {
        let up = cmd.to_uppercase();
        let table = if up.contains("101") { "users" } else { "orders" };
        execute_command_ex_for_e2e(api, h, &st, table, cmd)
    });
    if !redo_r.ok {
        return Err(format!("stack redo failed: {}", redo_r.message));
    }

    let users_after_redo = parse_count_rows(&exec(api, h, &st, "users", "COUNT")?);
    let orders_after_redo = parse_count_rows(&exec(api, h, &st, "orders", "COUNT")?);
    if users_after_redo != Some(1) || orders_after_redo != Some(expected_orders_after_commit) {
        return Err(format!(
            "post-redo count mismatch users={:?} orders={:?} expected_orders={}",
            users_after_redo, orders_after_redo, expected_orders_after_commit
        ));
    }

    // Persist stack snapshot and validate parse after restart simulation.
    let stack_path = stack_store_file_from_workspace(&tmp_dir);
    if let Some(parent) = stack_path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create stack dir failed: {e}"))?;
    }
    let payload = serde_json::json!({
        "schema_version": "newdb.gui.undo_redo_stack.v1",
        "undo_units": [unit],
        "redo_units": []
    });
    fs::write(&stack_path, format!("{}\n", serde_json::to_string(&payload).unwrap_or_default()))
        .map_err(|e| format!("write stack snapshot failed: {e}"))?;
    let stack_text = fs::read_to_string(&stack_path).map_err(|e| format!("read stack snapshot failed: {e}"))?;
    let (undo_units, _redo_units, warnings) = parse_stack_payload_text(&stack_text);
    if !warnings.is_empty() || undo_units.as_array().map(|v| v.is_empty()).unwrap_or(true) {
        return Err(format!("stack snapshot parse failed warnings={warnings:?}"));
    }

    // Restart recovery simulation: reopen session, counts remain stable.
    let mut dll2 = DllSessionState::default();
    let h2 = ensure_dll_session(api, &st, &mut dll2)?;
    let users_after_restart = parse_count_rows(&exec(api, h2, &st, "users", "COUNT")?);
    let orders_after_restart = parse_count_rows(&exec(api, h2, &st, "orders", "COUNT")?);
    if users_after_restart != Some(1) || orders_after_restart != Some(expected_orders_after_commit) {
        return Err(format!(
            "post-restart count mismatch users={:?} orders={:?} expected_orders={}",
            users_after_restart, orders_after_restart, expected_orders_after_commit
        ));
    }
    Ok(())
}

fn execute_command_ex_for_e2e(
    api: &DllApi,
    h: usize,
    st: &SessionState,
    table: &str,
    cmd: String,
) -> Result<CommandExecResult, String> {
    let st2 = SessionState {
        data_dir: st.data_dir.clone(),
        current_table: table.to_string(),
        page_size: st.page_size,
    };
    let out = execute_via_dll_session(api, h, &st2, &cmd)?;
    Ok(make_command_exec_result(out))
}
