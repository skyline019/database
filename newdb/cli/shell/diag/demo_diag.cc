#include <waterfall/config.h>

#include <cstdarg>
#include <cstdio>

#include "cli/shell/diag/demo_diag.h"
#include "cli/modules/common/logging/logging.h"
#include "cli/shell/state/shell_state.h"

void demo_verbose(const ShellState& st, const char* fmt, ...) {
    if (!st.verbose) {
        return;
    }
    char buf[4096];
    va_list ap;
    va_start(ap, fmt);
    (void)std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    buf[sizeof(buf) - 1] = '\0';
    logging_stderr_printf("[verbose] %s", buf);
}
