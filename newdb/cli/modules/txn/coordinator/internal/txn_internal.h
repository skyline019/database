#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include <newdb/error.h>

extern std::mutex g_write_intent_mu;
extern std::unordered_map<std::string, std::uint64_t> g_write_intent_owner;
extern std::unordered_map<std::uint64_t, std::uint64_t> g_txn_wait_for_owner;
extern std::atomic<std::int64_t> g_txn_id_seed;

newdb::Status compact_table_file_default(const std::string& data_file, const std::string& table_name);
std::uint64_t file_size_or_zero(const std::string& path);
std::uint64_t now_ms_steady();
bool detect_wait_cycle(std::uint64_t start, std::uint64_t& cycle_owner);
