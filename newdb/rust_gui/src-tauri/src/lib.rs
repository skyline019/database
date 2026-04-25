use serde::{Deserialize, Serialize};
use libloading::{Library, Symbol};
use once_cell::sync::OnceCell;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::io::{Read, Write};
use std::process::Stdio;
use std::thread;
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct UiSettings {
    accent: String,
    bg_mode: String,
    bg_image_url: String,
    bg_image_opacity: f32,
    panel_opacity: f32,
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
            bg_mode: "gradient".to_string(),
            bg_image_url: String::new(),
            bg_image_opacity: 0.22,
            panel_opacity: 0.9,
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
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join(script_name));
            candidates.push(dir.join("scripts").join(script_name));
            candidates.push(dir.join("../scripts").join(script_name));
            candidates.push(dir.join("../../scripts").join(script_name));
            candidates.push(dir.join("resources").join("scripts").join(script_name));
            candidates.push(dir.join("../resources").join("scripts").join(script_name));
        }
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
            candidates.push(dir.join("resources").join("scripts").join("results").join(file_name));
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
            Ok(DllApi {
                _lib: lib,
                version,
                session_create,
                session_destroy,
                session_set_table,
                session_execute,
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

fn execute_via_dll_session(
    api: &DllApi,
    handle: usize,
    st: &SessionState,
    command_line: &str,
) -> Result<String, String> {
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
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ScriptExecResult {
    ok: bool,
    output: String,
    error_code: Option<String>,
    stop_line: Option<usize>,
}

fn make_command_exec_result(out: String) -> CommandExecResult {
    let error_code = parse_capi_error_code(&out);
    CommandExecResult {
        ok: error_code.is_none(),
        output: strip_capi_error_line(&out),
        error_code,
    }
}

#[cfg(test)]
mod tests {
    use super::{make_command_exec_result, parse_capi_error_code, should_stop_script_on_output, strip_capi_error_line};

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
        assert!(!r.output.contains("[CAPI_ERROR]"));
        assert!(r.output.contains("[UPDATE] attribute 'age' expects int, got 'a'"));
    }

    #[test]
    fn command_exec_result_ok_without_error_prefix() {
        let out = "[INSERT] ok: table='users' now has 1 rows.\n".to_string();
        let r = make_command_exec_result(out);
        assert!(r.ok);
        assert!(r.error_code.is_none());
        assert!(r.output.contains("[INSERT] ok: table='users' now has 1 rows."));
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
    }
}

#[tauri::command]
fn runtime_trend_dashboard_json() -> Result<String, String> {
    let dashboard = resolve_results_file("runtime_trend_dashboard.json")?;
    fs::read_to_string(&dashboard)
        .map_err(|e| format!("failed to read {}: {e}", dashboard.display()))
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
            run_script,
            run_script_ex,
            run_perf_bench,
            run_nightly_soak,
            query_page,
            dll_info,
            runtime_artifact_info,
            runtime_trend_dashboard_json,
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
