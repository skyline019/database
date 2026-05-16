/**
 * Reads STRUCTDB_CAPI_VERSION_* from src/c_api/include/structdb_capi.h and writes
 * the same semver into gui/rust_gui package.json, src-tauri/tauri.conf.json, and
 * src-tauri/Cargo.toml [package].version (GUI release version tracks C API ABI).
 */
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..", "..");
const capiHeader = join(repoRoot, "src", "c_api", "include", "structdb_capi.h");
const guiRoot = join(__dirname, "..");
const packageJsonPath = join(guiRoot, "package.json");
const tauriConfPath = join(guiRoot, "src-tauri", "tauri.conf.json");
const cargoTomlPath = join(guiRoot, "src-tauri", "Cargo.toml");

const src = readFileSync(capiHeader, "utf8");
const maj = src.match(/#define\s+STRUCTDB_CAPI_VERSION_MAJOR\s+(\d+)/);
const min = src.match(/#define\s+STRUCTDB_CAPI_VERSION_MINOR\s+(\d+)/);
const pat = src.match(/#define\s+STRUCTDB_CAPI_VERSION_PATCH\s+(\d+)/);
if (!maj || !min || !pat) {
  console.error(`sync_gui_version_from_capi: could not parse version macros in ${capiHeader}`);
  process.exit(1);
}
const version = `${maj[1]}.${min[1]}.${pat[1]}`;

const pkg = JSON.parse(readFileSync(packageJsonPath, "utf8"));
if (pkg.version !== version) {
  pkg.version = version;
  writeFileSync(packageJsonPath, `${JSON.stringify(pkg, null, 2)}\n`, "utf8");
}

const tauri = JSON.parse(readFileSync(tauriConfPath, "utf8"));
if (tauri.version !== version) {
  tauri.version = version;
  writeFileSync(tauriConfPath, `${JSON.stringify(tauri, null, 2)}\n`, "utf8");
}

let cargo = readFileSync(cargoTomlPath, "utf8");
const cargoNext = cargo.replace(/^version\s*=\s*"[^"]*"/m, `version = "${version}"`);
if (cargoNext !== cargo) {
  writeFileSync(cargoTomlPath, cargoNext, "utf8");
}

console.log(`sync_gui_version_from_capi: GUI version set to ${version} (from C API header).`);
