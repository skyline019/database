#pragma once

#include "cli/modules/where/executor/where.h"

#include <cstddef>
#include <vector>

std::size_t where_estimate_scan_rows(const newdb::HeapTable& tbl,
                                     const newdb::TableSchema& schema,
                                     const std::vector<WhereCond>& conds,
                                     WhereQueryContext* ctx_ptr);
