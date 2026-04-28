#pragma once

#include <optional>
#include <string>
#include <vector>

#include <newdb/row.h>

struct ShellState;

struct LsmFindResult {
    bool found{false};
    bool deleted{false};
    newdb::Row row;
};

void lsm_lite_record_writes(ShellState& st,
                            const std::string& data_file,
                            const std::vector<newdb::Row>& rows,
                            bool deleted_flag);

std::optional<LsmFindResult> lsm_lite_find_by_id(ShellState& st,
                                                 const std::string& data_file,
                                                 int id);

void lsm_lite_on_txn_commit(ShellState& st, const std::string& data_file, std::int64_t txn_id);
void lsm_lite_on_txn_rollback(ShellState& st, const std::string& data_file, std::int64_t txn_id);
void lsm_lite_clear_txn_views(ShellState& st, const std::string& data_file);

