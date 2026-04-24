#include <waterfall/config.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>

#include <newdb/schema.h>
#include <newdb/schema_io.h>
#include <newdb/session.h>

#include "demo_commands.h"
#include "demo_diag.h"
#include "demo_shell.h"
#include "logging.h"
#include "shell_state.h"
#include "utils.h"

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
    return output.find("expects ") != std::string::npos
           && output.find(", got '") != std::string::npos;
}

} // namespace

void demo_autopick_initial_table_if_empty(ShellState& st) {
    std::string& current_table = st.session.table_name;
    std::string& current_file = st.session.data_path;
    if (!current_table.empty() || !current_file.empty()) {
        return;
    }
    st.session.schema = newdb::TableSchema{};
    namespace fs = std::filesystem;
    std::error_code ec;
    const fs::path cwd = workspace_directory(st);
    std::vector<std::string> tables;
    for (fs::directory_iterator it(cwd, ec), end; !ec && it != end; it.increment(ec)) {
        const fs::directory_entry& ent = *it;
        if (!ent.is_regular_file(ec)) {
            continue;
        }
        fs::path p = ent.path();
        if (p.extension() != ".bin") {
            continue;
        }

        std::string filename = p.filename().string();
        std::string stem = p.stem().string();
        bool looks_like_log =
            (filename == std::filesystem::path(st.log_file_path).filename().string());
        if (!looks_like_log) {
            const std::string suf = "_log";
            if (stem.size() >= suf.size() &&
                stem.compare(stem.size() - suf.size(), suf.size(), suf) == 0) {
                looks_like_log = true;
            }
        }
        if (looks_like_log) {
            continue;
        }
        tables.push_back(stem);
    }
    std::sort(tables.begin(), tables.end());
    if (!tables.empty()) {
        std::string pick = tables.front();
        if (std::find(tables.begin(), tables.end(), "users") != tables.end()) {
            pick = "users";
        }
        current_table = pick;
        current_file = pick + ".bin";
        reload_schema_from_data_path(st, current_file);
    }
}

void interactive_shell(ShellState& st, const char* data_file, const char* table_name) {
    const char* log_file = st.log_file_path.c_str();
    logging_bind_shell(&st);
    st.txn.set_workspace_root(st.data_dir);

    // When stdout is redirected to a pipe (e.g. GUI embedded terminal),
    // stdio becomes fully-buffered and prompts like "demo> " won't show up
    // unless we flush explicitly. Make interactive stdout unbuffered.
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    std::string& current_table = st.session.table_name;
    std::string& current_file = st.session.data_path;
    if (data_file) {
        current_file = data_file;
    }
    if (table_name) {
        current_table = table_name;
        if (!data_file) {
            current_file = std::string(table_name) + ".bin";
            reload_schema_from_data_path(st, current_file);
        }
    }

    demo_verbose(st, "workspace directory: %s\n", workspace_directory(st).string().c_str());

    demo_autopick_initial_table_if_empty(st);

    log_and_print(log_file,
                  "┌──────────────────────────────────────────────────────────────┐\n");
    if (current_table.empty()) {
        log_and_print(log_file,
                      "│        Interactive Demo Service  (no table selected)        │\n");
    } else {
        log_and_print(log_file,
                      "│        Interactive Demo Service  (table '%s')        │\n",
                      current_table.c_str());
    }
    log_and_print(log_file,
                  "└──────────────────────────────────────────────────────────────┘\n");
    log_and_print(log_file, "Commands:\n");
    log_and_print(log_file, "  DEFATTR(name:type, ...)              # type=int|char|string|timestamp|date|datetime|float|double|bool\n");
    log_and_print(log_file, "  CREATE SCHEMA(schema_name)           # create new schema\n");
    log_and_print(log_file, "  DROP SCHEMA(schema_name)             # drop empty schema\n");
    log_and_print(log_file, "  LIST SCHEMAS / SHOW SCHEMAS          # list all schemas\n");
    log_and_print(log_file, "  CREATE TABLE(name)                   # create new table\n");
    log_and_print(log_file, "  ALTER TABLE name SET SCHEMA(schema_name)    # set table's schema\n");
    log_and_print(log_file, "  ALTER TABLE name REMOVE SCHEMA            # remove table's schema\n");
    log_and_print(log_file, "  USE(name)\n");
    log_and_print(log_file, "  RENAME TABLE(new_name)\n");
    log_and_print(log_file, "  INSERT(id, v1, v2, ...)              # with DEFATTR\n");
    log_and_print(log_file, "  BULKINSERT(start_id,count[,dept])    # high-throughput bulk insert\n");
    log_and_print(log_file, "  BULKINSERTFAST(start_id,count[,dept])# skip duplicate checks when ids are guaranteed fresh\n");
    log_and_print(log_file, "  UPDATE(id, name, balance) or UPDATE(id, v1, v2, ...) with DEFATTR\n");
    log_and_print(log_file, "  SETATTR(id, key, value)\n");
    log_and_print(log_file, "  RENATTR(old, new)      # rename attribute in all rows\n");
    log_and_print(log_file, "  DELATTR(key)           # drop attribute from all rows\n");
    log_and_print(log_file, "  DELETE(id)\n");
    log_and_print(log_file, "  FIND(id)\n");
    log_and_print(log_file, "  PAGE(page, page_size, [order_key], [desc])  # order_key=id or any attr name; desc=desc\n");
    log_and_print(log_file, "  QBAL(min_balance)\n");
    log_and_print(log_file, "  WHERE(attr, op, value [, AND|OR, attr, op, value] ...)   # generic conditional query, op =,!=,>,<,>=,<=,contains\n");
    log_and_print(log_file, "  COUNT / COUNT(attr, op, value [, AND|OR, attr, op, value] ...)   # aggregate count (optionally with conditions)\n");
    log_and_print(log_file, "  SUM(attr [, WHERE, ...]) / AVG(attr [, WHERE, ...]) / MIN(attr [, WHERE, ...]) / MAX(attr [, WHERE, ...])\n");
    log_and_print(log_file, "  LIST TABLES / SHOW TABLES        # list tables in workspace (--data-dir or cwd)\n");
    log_and_print(log_file, "  DROP TABLE(name)                 # drop table (data + .attr)\n");
    log_and_print(log_file, "  SHOW ATTR / DESCRIBE             # show current table attributes\n");
    log_and_print(log_file, "  SHOW KEY / SHOW PRIMARY KEY      # show primary key (default: id)\n");
    log_and_print(log_file, "  SET PRIMARY KEY(key)             # set primary key (id or a DEFATTR attribute)\n");
    log_and_print(log_file, "  SHOWLOG          # dump session log (plain text or legacy XOR)\n");
    log_and_print(log_file, "  RESET            # recreate empty table\n");
    log_and_print(log_file, "  VACUUM           # rewrite compacted table file (remove old versions/tombstones)\n");
    log_and_print(log_file, "  SCAN             # scan all pages and print raw tuples\n");
    log_and_print(log_file, "  EXPORT CSV [file]             # dump current table to CSV\n");
    log_and_print(log_file, "  EXPORT JSON [file]            # dump current table to JSON\n");
    log_and_print(log_file, "  IMPORTDIR(path)  # import *.bin+.attr into workspace (--data-dir or cwd)\n");
    log_and_print(log_file, "  FINDPK(value)    # find by current primary key\n");
    log_and_print(log_file, "  DELETEPK(value)  # delete by current primary key\n");
    log_and_print(log_file, "  BEGIN           # start transaction (with table name)\n");
    log_and_print(log_file, "  COMMIT          # commit transaction\n");
    log_and_print(log_file, "  ROLLBACK        # rollback transaction\n");
    log_and_print(log_file, "  AUTOVACUUM [0|1|on|off] [ops_threshold] | AUTOVACUUM threshold <ops> | AUTOVACUUM\n");
    log_and_print(log_file, "  WALSYNC [full|normal [interval_ms]|off]  # configure WAL durability\n");
    log_and_print(log_file, "  SHOW TUNING      # print WALSYNC + AUTOVACUUM status\n");
    log_and_print(log_file, "  SHOW STORAGE     # demodb.wal size, demodb.wal_lsn, total *.bin size\n");
    log_and_print(log_file, "  HELP\n");
    log_and_print(log_file, "  EXIT\n");

    char line[512];
    while (true) {
        std::printf("demo> ");
        std::fflush(stdout);
        if (!std::fgets(line, sizeof(line), stdin)) break;
        if (!process_command_line(st, line)) {
            break;
        }
    }
}
void run_mdb_script(ShellState& st, const char* script_file) {
    logging_bind_shell(&st);
    st.txn.set_workspace_root(st.data_dir);
    std::string& current_table = st.session.table_name;
    std::string& current_file = st.session.data_path;
    const char* log_file = st.log_file_path.c_str();
    if (!current_table.empty()) {
        current_file = current_table + ".bin";
        reload_schema_from_data_path(st, current_file);
        demo_verbose(st, "mdb script: table=%s file=%s\n", current_table.c_str(), current_file.c_str());
    } else {
        current_file.clear();
        demo_verbose(st, "mdb script: no default table preselected\n");
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
