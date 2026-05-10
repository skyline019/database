#include <waterfall/config.h>

#include "cli/modules/common/logging/logging.h"
#include "cli/shell/dispatch/shared/dispatch_internal.h"

bool handle_session_commands(const char* line, const char* log_file, bool& handled) {
    handled = false;
    if (strcasecmp_ascii(line, "EXIT") == 0) {
        log_and_print(log_file, "bye.\n");
        handled = true;
        return false;
    }
    if (strcasecmp_ascii(line, "HELP") == 0) {
        log_and_print(log_file,
                      "Commands: DEFATTR/CREATE/USE/RENAME/INSERT/BULKINSERT/BULKINSERTFAST/UPDATE/UPDATEWHERE/SETATTR/SETATTRMULTI/RENATTR/DELATTR/DELETE/DELETEWHERE/DELETEPK/FIND/FINDPK/QBAL/WHERE/COUNT/PAGE/EXPORT/LIST TABLES/SHOW TABLES/DROP TABLE/SHOW ATTR/DESCRIBE/SHOW KEY/SHOW PRIMARY KEY/SET PRIMARY KEY/SHOWLOG/SHOW TUNING/SHOW STORAGE/RESET/VACUUM/CONFIRM_REORDER/SCAN/IMPORTDIR/BEGIN/COMMIT/ROLLBACK/AUTOVACUUM/WALSYNC/HELP/EXIT\n");
        log_and_print(log_file,
                      "CLI: run `newdb_demo --help` for --data-dir, --table, --log-file, --verbose, etc.\n");
        handled = true;
        return true;
    }
    return true;
}




