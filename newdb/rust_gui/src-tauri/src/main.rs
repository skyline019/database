#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    // Minimal e2e smoke for CI/packaging:
    // - `--self-check` should not require GUI interaction.
    // - It validates that the binary starts and core command wiring is loadable.
    if std::env::args().any(|a| a == "--self-check") {
        // Avoid popping UI on CI. The actual checks are implemented in the lib so
        // both main binary and tests can reuse them.
        if let Err(e) = newdb_rust_vue_gui_lib::self_check() {
            eprintln!("[self-check] failed: {e}");
            std::process::exit(2);
        }
        println!("[self-check] ok");
        return;
    }
    newdb_rust_vue_gui_lib::run()
}
