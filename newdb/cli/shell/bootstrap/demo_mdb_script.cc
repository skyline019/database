#include <waterfall/config.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/shell/diag/demo_diag.h"
#include "cli/shell/repl/demo_shell.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/shell/state/shell_state_ops.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/common/util/utils.h"

namespace {

std::uintmax_t log_size_or_zero(const std::string& path) {
    std::error_code ec;
    const std::uintmax_t n = std::filesystem::file_size(path, ec);
    return ec ? 0 : n;
}

std::string read_log_tail_from(const std::string& path, const std::uintmax_t start_pos) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return {};
    }
    in.seekg(0, std::ios::end);
    const std::streamoff end = in.tellg();
    if (end <= 0 || start_pos >= static_cast<std::uintmax_t>(end)) {
        return {};
    }
    in.seekg(static_cast<std::streamoff>(start_pos), std::ios::beg);
    std::string out;
    out.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    return out;
}

bool should_stop_script_on_output(const std::string& output) {
    if (output.find("[ERROR]") != std::string::npos) {
        return true;
    }
    if (output.find("[INSERT] attribute '") != std::string::npos
        || output.find("[UPDATE] attribute '") != std::string::npos
        || output.find("[SETATTR] attribute '") != std::string::npos) {
        return true;
    }
    return output.find("expects ") != std::string::npos && output.find(", got '") != std::string::npos;
}

} // namespace

void run_mdb_script(ShellState& st, const char* script_file) {
    ShellStateFacade f(st);
    f.bind_logging();
    f.txn().set_workspace_root(f.data_dir());
    std::string& current_table = f.table_name();
    std::string& current_file = f.data_path();
    const char* log_file = f.log_file_path().c_str();
    if (!current_table.empty()) {
        current_file = current_table + ".bin";
        reload_schema_from_data_path(st, current_file);
        demo_verbose(f, "mdb script: table=%s file=%s\n", current_table.c_str(), current_file.c_str());
    } else {
        current_file.clear();
        demo_verbose(f, "mdb script: no default table preselected\n");
    }

    FILE* sf = std::fopen(script_file, "r");
    if (!sf) {
        std::perror("open script file");
        return;
    }

    log_and_print(log_file,
                  "[SCRIPT] running command file '%s' on table '%s' (file=%s)\n",
                  script_file,
                  current_table.empty() ? "(none)" : current_table.c_str(),
                  current_file.empty() ? "(none)" : current_file.c_str());

    char line[512];
    std::size_t line_no = 0;
    while (std::fgets(line, sizeof(line), sf)) {
        ++line_no;
        std::string s = trim(line);
        if (s.empty() || s[0] == '#') continue;
        const std::uintmax_t before = log_size_or_zero(log_file);
        if (!process_command_line(st, s.c_str())) {
            break;
        }
        const std::string tail = read_log_tail_from(log_file, before);
        if (should_stop_script_on_output(tail)) {
            log_and_print(log_file,
                          "[SCRIPT] stopped at line %zu due to command error.\n",
                          line_no);
            break;
        }
    }

    std::fclose(sf);
    log_and_print(log_file,
                  "[SCRIPT] command file '%s' finished.\n", script_file);
}
