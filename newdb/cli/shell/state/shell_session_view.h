#pragma once

/// Preferred **narrow** session surface for dispatch and services: use this type name in new code
/// so call sites rely on [`ShellStateFacade`](shell_state_facade.h) accessors instead of
/// `#include <newdb/session.h>` for paths, heap counters, schema, and `heap_table()`.
#include "cli/shell/state/shell_state_facade.h"

using ShellSessionView = ShellStateFacade;
