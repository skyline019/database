#!/usr/bin/env bash
# Run newdb_tests (or ctest) with common NEWDB_* cleared so local shell exports do not leak.
# Usage:
#   ./newdb/scripts/run_newdb_tests_cleanenv.sh /path/to/build "TxnEmbeddedContract|TxnUndoMetrics"
#   ./newdb/scripts/run_newdb_tests_cleanenv.sh /path/to/build   # all newdb-labeled tests

set -euo pipefail

BUILD_DIR="${1:?build dir}"
FILTER="${2:-}"

for v in NEWDB_TXN_ISOLATION_READPATH NEWDB_TXN_STMT_SAVEPOINT NEWDB_TXN_TRACE NEWDB_VISCHK \
         NEWDB_WHERE_RESERVE_PREDICATE NEWDB_WHERE_RESERVE_RANGE NEWDB_LAZY_HEAP; do
  unset "${v}" || true
done

cd "${BUILD_DIR}"
if [[ -n "${FILTER}" ]]; then
  ctest --output-on-failure -R "${FILTER}"
else
  ctest -L newdb --output-on-failure
fi
