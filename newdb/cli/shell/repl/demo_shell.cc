#include <waterfall/config.h>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>
#include <algorithm>

#include <newdb/schema.h>
#include <newdb/schema_io.h>

#include "cli/shell/dispatch/router/dispatch.h"
#include "cli/shell/diag/demo_diag.h"
#include "cli/shell/repl/demo_shell.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/shell/state/shell_state_ops.h"
#include "cli/shell/state/shell_state_facade.h"
#include "cli/modules/txn/coordinator/txn_manager.h"
#include "cli/modules/common/util/utils.h"

void demo_autopick_initial_table_if_empty(ShellState& st) {
    ShellStateFacade f(st);
    std::string& current_table = f.table_name();
    std::string& current_file = f.data_path();
    if (!current_table.empty() || !current_file.empty()) {
        return;
    }
    f.schema() = newdb::TableSchema{};
    namespace fs = std::filesystem;
    std::error_code ec;
    const fs::path cwd = f.workspace_directory();
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
            (filename == std::filesystem::path(f.log_file_path()).filename().string());
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
    ShellStateFacade f(st);
    const char* log_file = f.log_file_path().c_str();
    f.bind_logging();
    f.txn().set_workspace_root(f.data_dir());

    // When stdout is redirected to a pipe (e.g. GUI embedded terminal),
    // stdio becomes fully-buffered and prompts like "demo> " won't show up
    // unless we flush explicitly. Make interactive stdout unbuffered.
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    std::string& current_table = f.table_name();
    std::string& current_file = f.data_path();
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

    demo_verbose(f, "workspace directory: %s\n", f.workspace_directory().string().c_str());

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
    log_and_print(log_file, "  CONFIRM_REORDER  # after confirming deletes need no WAL recovery: renumber ids to 1..N (pk=id only)\n");
    log_and_print(log_file, "  SCAN             # scan all pages and print raw tuples\n");
    log_and_print(log_file, "  EXPORT CSV [file]             # dump current table to CSV\n");
    log_and_print(log_file, "  EXPORT JSON [file]            # dump current table to JSON\n");
    log_and_print(log_file, "  IMPORTDIR(path)  # import *.bin+.attr into workspace (--data-dir or cwd)\n");
    log_and_print(log_file, "  FINDPK(value)    # find by current primary key\n");
    log_and_print(log_file, "  DELETEPK(value)  # delete by current primary key\n");
    log_and_print(log_file, "  BEGIN           # start transaction (with table name)\n");
    log_and_print(log_file, "  COMMIT          # commit transaction\n");
    log_and_print(log_file, "  ROLLBACK        # rollback transaction\n");
    log_and_print(log_file, "  AUTOVACUUM [0|1|on|off] [ops_threshold] | AUTOVACUUM threshold <ops> | AUTOVACUUM interval <sec> | AUTOVACUUM\n");
    log_and_print(log_file, "  WALSYNC [full|normal [interval_ms]|off]  # configure WAL durability\n");
    log_and_print(log_file, "  WALADAPTIVE [on|off]                     # adaptive WAL sync under pressure\n");
    log_and_print(log_file, "  GROUPCOMMIT <window_ms> [max_batch]      # batch commit flush window\n");
    log_and_print(log_file, "  TXNISOLATION [snapshot|read_committed]   # transaction isolation mode\n");
    log_and_print(log_file, "  WRITECONFLICT [reject|wait [timeout_ms]] # write-key conflict strategy\n");
    log_and_print(log_file, "  HOTINDEX [on|off]                        # in-memory hot index switch\n");
    log_and_print(log_file, "  SEGMENT <target_bytes>                   # logical segment size target\n");
    log_and_print(log_file, "  SHOW TUNING      # print WALSYNC + AUTOVACUUM + WRITECONFLICT status\n");
    log_and_print(log_file, "  SHOW TUNING JSON # print structured tuning/runtime stats JSON\n");
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
