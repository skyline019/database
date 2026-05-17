#include "structdb_capi.h"

#include "structdb/client/embed_client.hpp"
#include "structdb/client/mdb_runner.hpp"
#include "structdb/facade/config.hpp"
#include "structdb/facade/engine.hpp"
#include "structdb/infra/long_task_progress.hpp"
#include "structdb/storage/checkpoint_chain.hpp"
#include "structdb/storage/storage_engine.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

struct structdb_engine {
  structdb::facade::Engine engine;
  bool started{false};
  structdb_embed_client* active_embed{nullptr};
  std::string data_dir_utf8;
  structdb_long_task_control* bound_long_task{nullptr};
  structdb_long_task_control* script_batch_long_task{nullptr};
  std::uint64_t script_batch_lines_total{0};
  std::uint64_t script_batch_lines_done{0};
};

struct structdb_long_task_control {
  std::shared_ptr<structdb::infra::LongTaskCancelToken> cancel =
      std::make_shared<structdb::infra::LongTaskCancelToken>();
  structdb::infra::LongTaskReporter reporter;
  structdb_long_task_progress_fn progress_cb{nullptr};
  void* progress_user{nullptr};
  std::string kind_buf;
  std::string status_buf;
  std::string detail_buf;
  std::string task_id_buf;

  structdb_long_task_control() { reporter.bind_cancel_token(cancel); }

  void wire_progress_callback() {
    reporter.set_progress_callback([this](const structdb::infra::LongTaskProgressSnapshot& s) { emit_progress(s); });
  }

  void emit_progress(const structdb::infra::LongTaskProgressSnapshot& s) {
    if (!progress_cb) return;
    kind_buf = structdb::infra::long_task_kind_api_string(s.kind);
    status_buf = structdb::infra::long_task_status_api_string(s.status);
    detail_buf = s.detail;
    task_id_buf = s.task_id;
    structdb_long_task_progress p{};
    p.kind = kind_buf.c_str();
    p.status = status_buf.c_str();
    p.units_done = s.units_done;
    p.units_total = s.units_total;
    p.bytes_done = s.bytes_done;
    p.bytes_total = s.bytes_total;
    p.detail = detail_buf.empty() ? nullptr : detail_buf.c_str();
    p.task_id = task_id_buf.empty() ? nullptr : task_id_buf.c_str();
    progress_cb(progress_user, &p);
  }

  structdb::infra::LongTaskReporter* reporter_ptr() { return &reporter; }
};

struct structdb_embed_client {
  structdb_engine* engine{nullptr};
  std::unique_ptr<structdb::client::EmbedClient> client;
  std::string session_dir_utf8;
  std::string session_log_utf8;
};

struct structdb_mdb_session {
  structdb::client::mdb::MdbInteractiveSession impl;
};

namespace {

#define STRUCTDB_CAPI_S1(x) #x
#define STRUCTDB_CAPI_S(x) STRUCTDB_CAPI_S1(x)

const char kVersionStr[] = STRUCTDB_CAPI_S(STRUCTDB_CAPI_VERSION_MAJOR) "." STRUCTDB_CAPI_S(
    STRUCTDB_CAPI_VERSION_MINOR) "." STRUCTDB_CAPI_S(STRUCTDB_CAPI_VERSION_PATCH);

static_assert(sizeof(structdb_mdb_run_options) == STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3, "capi: update STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3");
static_assert(offsetof(structdb_mdb_run_options, repl_exit_requested_out) == STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1,
              "capi: update STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1");
static_assert(offsetof(structdb_mdb_run_options, long_task_control) == STRUCTDB_MDB_RUN_OPTIONS_SIZE_V2,
              "capi: update STRUCTDB_MDB_RUN_OPTIONS_SIZE_V2");

std::filesystem::path utf8_path_to_fs(const char* utf8) {
  if (!utf8) return {};
#if defined(_WIN32)
  return std::filesystem::u8path(std::string_view(utf8));
#else
  return std::filesystem::path(utf8);
#endif
}

bool utf8_nonempty(const char* s) { return s != nullptr && s[0] != '\0'; }

std::string path_to_utf8(const std::filesystem::path& p) {
  // UTF-8 narrow string; matches `Engine::startup` + `u8path` on Windows.
  return std::string(p.u8string());
}

size_t copy_utf8_cstr(char* out, size_t out_cap, const std::string& utf8) {
  if (!out || out_cap == 0) return utf8.size();
  const size_t n = (std::min)(utf8.size(), out_cap - 1);
  std::memcpy(out, utf8.data(), n);
  out[n] = '\0';
  return utf8.size();
}

bool resolve_default_path_triple(const char* workspace_utf8, std::filesystem::path* workspace_fs,
                                 std::filesystem::path* data_fs, std::filesystem::path* session_fs, std::string* err_s) {
  try {
    std::filesystem::path ws =
        !utf8_nonempty(workspace_utf8) ? std::filesystem::current_path()
                                       : std::filesystem::absolute(utf8_path_to_fs(workspace_utf8));
    const std::filesystem::path data =
        std::filesystem::absolute(ws / STRUCTDB_CAPI_DEFAULT_STORE_DIR / "_data");
    const std::filesystem::path sess = data / "embed_session";
    if (workspace_fs) *workspace_fs = ws;
    if (data_fs) *data_fs = data;
    if (session_fs) *session_fs = sess;
    return true;
  } catch (const std::exception& ex) {
    if (err_s) *err_s = ex.what();
    return false;
  } catch (...) {
    if (err_s) *err_s = "unknown filesystem exception";
    return false;
  }
}

void write_err_buf(char* err, size_t err_len, const char* msg) {
  if (!err || err_len == 0) return;
  std::size_t i = 0;
  for (; msg[i] && i + 1 < err_len; ++i) err[i] = msg[i];
  err[i] = '\0';
}

void bind_long_task_storage(structdb_engine* engine, structdb_long_task_control* ctrl) {
  if (!engine || !engine->started) return;
  engine->bound_long_task = ctrl;
  structdb::storage::StorageEngine* st = engine->engine.storage();
  if (!st) return;
  st->set_active_long_task(ctrl ? ctrl->reporter_ptr() : nullptr);
}

void script_batch_report_line(structdb_engine* engine, bool after_success) {
  if (!engine || !engine->script_batch_long_task) return;
  if (after_success) ++engine->script_batch_lines_done;
  auto* r = engine->script_batch_long_task->reporter_ptr();
  r->set_kind(structdb::infra::LongTaskKind::MdbScript);
  r->report(structdb::infra::LongTaskStatus::Running, engine->script_batch_lines_done,
            engine->script_batch_lines_total);
}

bool long_task_cancelled(const structdb_mdb_run_options& eff, structdb_engine* engine) {
  structdb_long_task_control* ctrl = eff.long_task_control;
  if (!ctrl && engine) ctrl = engine->script_batch_long_task;
  if (!ctrl) return false;
  return ctrl->reporter_ptr()->poll_cancel_and_report_cancelling();
}

bool effective_mdb_options(const structdb_mdb_run_options* opts, structdb_mdb_run_options* out) {
  static const structdb_mdb_run_options kDefault = {
      STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3,
      0,
      0,
      0,
      0,
      1,
      nullptr,
      nullptr,
      nullptr,
      nullptr,
      nullptr,
  };
  *out = kDefault;
  if (!opts) return true;
  if (opts->struct_size == STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1) {
    std::memcpy(out, opts, STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1);
    out->repl_exit_requested_out = nullptr;
    out->long_task_control = nullptr;
    out->struct_size = STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3;
    return true;
  }
  if (opts->struct_size == STRUCTDB_MDB_RUN_OPTIONS_SIZE_V2) {
    std::memcpy(out, opts, STRUCTDB_MDB_RUN_OPTIONS_SIZE_V2);
    out->long_task_control = nullptr;
    out->struct_size = STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3;
    return true;
  }
  if (opts->struct_size != STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3) return false;
  *out = *opts;
  out->struct_size = STRUCTDB_MDB_RUN_OPTIONS_SIZE_V3;
  return true;
}

void emit_logs(const structdb_mdb_run_options& eff, const std::vector<std::string>& lines, bool truncate_log_file) {
  try {
    std::ofstream file_out;
    if (eff.log_file_path && eff.log_file_path[0] != '\0') {
      const auto mode = std::ios::binary | (truncate_log_file ? std::ios::trunc : std::ios::app);
      file_out.open(utf8_path_to_fs(eff.log_file_path), mode);
    }
    for (const auto& ln : lines) {
      try {
        if (eff.log_line_cb) eff.log_line_cb(eff.log_user_data, ln.c_str());
        if (file_out.is_open()) {
          file_out.write(ln.data(), static_cast<std::streamsize>(ln.size()));
          file_out.put('\n');
        }
      } catch (const std::exception&) {
      } catch (...) {
      }
    }
  } catch (const std::exception&) {
  } catch (...) {
  }
}

int run_mdb_impl(const char* data_dir, const char* session_dir, const char* mdb_path, char* err, size_t err_len,
                 const structdb_mdb_run_options* opts) {
  structdb_mdb_run_options eff{};
  if (!effective_mdb_options(opts, &eff)) {
    write_err_buf(err, err_len,
                  "structdb_mdb_run_options: struct_size must be STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1, V2, or V3");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }

  if (!utf8_nonempty(mdb_path)) {
    write_err_buf(err, err_len, "mdb_path is null or empty");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }

  std::filesystem::path data_fs;
  std::filesystem::path session_fs;
  try {
    const auto cwd = std::filesystem::current_path();
    data_fs = !utf8_nonempty(data_dir) ? std::filesystem::absolute(cwd / STRUCTDB_CAPI_DEFAULT_STORE_DIR / "_data")
                                       : std::filesystem::absolute(utf8_path_to_fs(data_dir));
    session_fs = !utf8_nonempty(session_dir) ? data_fs / "embed_session"
                                               : std::filesystem::absolute(utf8_path_to_fs(session_dir));
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_ENGINE_STARTUP;
  } catch (...) {
    write_err_buf(err, err_len, "path resolution failed");
    return STRUCTDB_CAPI_ERR_ENGINE_STARTUP;
  }

  structdb::facade::Engine engine;
  bool engine_started = false;
  structdb::client::EmbedClient client(engine);
  bool embed_opened = false;
  try {
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = path_to_utf8(data_fs);
    snap.version = 1;
    engine.config().update(1, snap);

    std::string e;
    if (!engine.startup(&e)) {
      write_err_buf(err, err_len, e.c_str());
      return STRUCTDB_CAPI_ERR_ENGINE_STARTUP;
    }
    engine_started = true;

    if (!client.open(session_fs, &e)) {
      write_err_buf(err, err_len, e.c_str());
      try {
        engine.shutdown();
      } catch (...) {
      }
      engine_started = false;
      return STRUCTDB_CAPI_ERR_EMBED_OPEN;
    }
    embed_opened = true;

    std::vector<std::string> line_log;
    structdb::client::mdb::MdbRunOptions opt;
    opt.script_path = utf8_path_to_fs(mdb_path);
    opt.log_sink = (eff.log_file_path && eff.log_file_path[0]) || eff.log_line_cb ? &line_log : nullptr;
    opt.fsync_each_batch = eff.fsync_each_batch != 0;
    opt.fsync_each_session_txn_op = eff.fsync_each_session_txn_op != 0;
    opt.fail_if_unclosed_txn = eff.fail_if_unclosed_txn != 0;
    opt.allow_persist_while_txn_active_experimental = eff.allow_persist_while_txn_active_experimental != 0;
    if (eff.long_task_control) {
      eff.long_task_control->reporter.set_kind(structdb::infra::LongTaskKind::MdbScript);
      opt.long_task = eff.long_task_control->reporter_ptr();
      if (engine.storage()) engine.storage()->set_active_long_task(opt.long_task);
    }

    const auto r = structdb::client::mdb::run_mdb_script(engine, client, opt);
    if (engine.storage()) engine.storage()->set_active_long_task(nullptr);
    if (eff.repl_exit_requested_out) *eff.repl_exit_requested_out = r.repl_exit_requested ? 1 : 0;
    emit_logs(eff, line_log, true);
    try {
      client.close();
    } catch (...) {
    }
    embed_opened = false;
    try {
      engine.shutdown();
    } catch (...) {
    }
    engine_started = false;

    if (r.cancelled) {
      write_err_buf(err, err_len, "cancelled");
      return STRUCTDB_CAPI_ERR_CANCELLED;
    }
    if (!r.ok) {
      const std::string msg = r.last_error.empty() ? "mdb failed" : r.last_error;
      write_err_buf(err, err_len, msg.c_str());
      return STRUCTDB_CAPI_ERR_MDB_RUN;
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    if (embed_opened) {
      try {
        client.close();
      } catch (...) {
      }
      embed_opened = false;
    }
    if (engine_started) {
      try {
        engine.shutdown();
      } catch (...) {
      }
      engine_started = false;
    }
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_ENGINE_STARTUP;
  } catch (...) {
    if (embed_opened) {
      try {
        client.close();
      } catch (...) {
      }
      embed_opened = false;
    }
    if (engine_started) {
      try {
        engine.shutdown();
      } catch (...) {
      }
      engine_started = false;
    }
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_ENGINE_STARTUP;
  }
}

}  // namespace

extern "C" {

const char* structdb_capi_version_string(void) { return kVersionStr; }

uint32_t structdb_capi_version(void) {
  return (static_cast<uint32_t>(STRUCTDB_CAPI_VERSION_MAJOR) << 16) |
         (static_cast<uint32_t>(STRUCTDB_CAPI_VERSION_MINOR) << 8) |
         static_cast<uint32_t>(STRUCTDB_CAPI_VERSION_PATCH);
}

int structdb_run_mdb_file_ex(const char* data_dir, const char* session_dir, const char* mdb_path, char* err,
                             size_t err_len, const structdb_mdb_run_options* opts) {
  return run_mdb_impl(data_dir, session_dir, mdb_path, err, err_len, opts);
}

int structdb_run_mdb_file(const char* data_dir, const char* session_dir, const char* mdb_path, char* err,
                          size_t err_len) {
  return structdb_run_mdb_file_ex(data_dir, session_dir, mdb_path, err, err_len, nullptr);
}

size_t structdb_engine_get_data_dir_utf8(structdb_engine* engine, char* out, size_t out_cap) {
  if (!engine) return 0;
  return copy_utf8_cstr(out, out_cap, engine->data_dir_utf8);
}

int structdb_engine_wal_sync(structdb_engine* engine, char* err, size_t err_len) {
  if (!engine || !engine->started) {
    write_err_buf(err, err_len, !engine ? "null engine" : "engine not started");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  try {
    structdb::storage::StorageEngine* st = engine->engine.storage();
    if (!st) {
      write_err_buf(err, err_len, "storage unavailable");
      return STRUCTDB_CAPI_ERR_NULL_ARG;
    }
    std::string e;
    if (!st->wal_sync(&e)) {
      write_err_buf(err, err_len, e.empty() ? "wal_sync failed" : e.c_str());
      return STRUCTDB_CAPI_ERR_STORAGE_IO;
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
}

int structdb_engine_flush_memtable(structdb_engine* engine, char* err, size_t err_len) {
  if (!engine || !engine->started) {
    write_err_buf(err, err_len, !engine ? "null engine" : "engine not started");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  try {
    structdb::storage::StorageEngine* st = engine->engine.storage();
    if (!st) {
      write_err_buf(err, err_len, "storage unavailable");
      return STRUCTDB_CAPI_ERR_NULL_ARG;
    }
    std::string e;
    if (!st->flush_memtable(&e)) {
      write_err_buf(err, err_len, e.empty() ? "flush_memtable failed" : e.c_str());
      return STRUCTDB_CAPI_ERR_STORAGE_IO;
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
}

int structdb_engine_checkpoint(structdb_engine* engine, char* err, size_t err_len) {
  if (!engine || !engine->started) {
    write_err_buf(err, err_len, !engine ? "null engine" : "engine not started");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  try {
    structdb::storage::StorageEngine* st = engine->engine.storage();
    if (!st) {
      write_err_buf(err, err_len, "storage unavailable");
      return STRUCTDB_CAPI_ERR_NULL_ARG;
    }
    std::string e;
    if (!st->checkpoint(&e)) {
      write_err_buf(err, err_len, e.empty() ? "checkpoint failed" : e.c_str());
      return STRUCTDB_CAPI_ERR_STORAGE_IO;
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
}

uint64_t structdb_engine_latest_commit_seq(structdb_engine* engine) {
  if (!engine || !engine->started) return 0;
  try {
    return engine->engine.latest_commit_seq();
  } catch (...) {
    return 0;
  }
}

int structdb_capi_get_default_paths(const char* workspace_utf8, char* workspace_out, size_t workspace_cap,
                                    char* data_dir_utf8_out, size_t data_dir_cap, char* session_dir_utf8_out,
                                    size_t session_dir_cap, char* err, size_t err_len) {
  if (!workspace_out && !data_dir_utf8_out && !session_dir_utf8_out) {
    write_err_buf(err, err_len, "no output buffer");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  std::filesystem::path ws, dd, ss;
  std::string er;
  if (!resolve_default_path_triple(workspace_utf8, &ws, &dd, &ss, &er)) {
    write_err_buf(err, err_len, er.c_str());
    return STRUCTDB_CAPI_ERR_ENGINE_STARTUP;
  }
  const std::string ws8 = path_to_utf8(ws);
  const std::string dd8 = path_to_utf8(dd);
  const std::string ss8 = path_to_utf8(ss);
  auto too_small = [](const char* p, size_t cap, const std::string& s) { return p && cap > 0 && s.size() + 1 > cap; };
  if (too_small(workspace_out, workspace_cap, ws8) || too_small(data_dir_utf8_out, data_dir_cap, dd8) ||
      too_small(session_dir_utf8_out, session_dir_cap, ss8)) {
    write_err_buf(err, err_len, "output buffer too small");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  if (workspace_out && workspace_cap > 0) copy_utf8_cstr(workspace_out, workspace_cap, ws8);
  if (data_dir_utf8_out && data_dir_cap > 0) copy_utf8_cstr(data_dir_utf8_out, data_dir_cap, dd8);
  if (session_dir_utf8_out && session_dir_cap > 0) copy_utf8_cstr(session_dir_utf8_out, session_dir_cap, ss8);
  return STRUCTDB_CAPI_OK;
}

structdb_engine* structdb_engine_open_ex(const char* data_dir, uint32_t open_flags, char* err, size_t err_len) {
  auto* box = new (std::nothrow) structdb_engine();
  if (!box) {
    write_err_buf(err, err_len, "allocation failed");
    return nullptr;
  }
  try {
    const auto cwd = std::filesystem::current_path();
    const std::filesystem::path data_fs = !utf8_nonempty(data_dir)
                                              ? std::filesystem::absolute(cwd / STRUCTDB_CAPI_DEFAULT_STORE_DIR / "_data")
                                              : std::filesystem::absolute(utf8_path_to_fs(data_dir));
    box->data_dir_utf8 = path_to_utf8(data_fs);
    structdb::facade::EngineConfigSnapshot snap;
    snap.data_dir = box->data_dir_utf8;
    snap.version = 1;
    if ((open_flags & STRUCTDB_ENGINE_OPEN_FLAG_EXCLUSIVE_DIR_LOCK) != 0) {
      snap.exclusive_data_dir_lock = true;
    }
    box->engine.config().update(1, snap);
    std::string e;
    if (!box->engine.startup(&e)) {
      write_err_buf(err, err_len, e.c_str());
      delete box;
      return nullptr;
    }
    box->started = true;
    return box;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    delete box;
    return nullptr;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    delete box;
    return nullptr;
  }
}

structdb_engine* structdb_engine_open(const char* data_dir, char* err, size_t err_len) {
  return structdb_engine_open_ex(data_dir, 0, err, err_len);
}

void structdb_engine_shutdown(structdb_engine* engine) {
  if (!engine) return;
  if (engine->active_embed) {
    structdb_embed_close(engine->active_embed);
  }
  if (engine->started) {
    try {
      engine->engine.shutdown();
    } catch (...) {
    }
    engine->started = false;
  }
  delete engine;
}

int structdb_recover_data_dir_to_checkpoint_seq(const char* data_dir_utf8, uint64_t checkpoint_seq, char* err,
                                              size_t err_len) {
  if (!utf8_nonempty(data_dir_utf8)) {
    write_err_buf(err, err_len, "null or empty data_dir");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  if (checkpoint_seq == 0) {
    write_err_buf(err, err_len, "recover: checkpoint_seq must be > 0");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  try {
    std::string e;
    if (!structdb::storage::recover_data_dir_to_checkpoint_seq(utf8_path_to_fs(data_dir_utf8), checkpoint_seq, &e)) {
      write_err_buf(err, err_len, e.empty() ? "recover failed" : e.c_str());
      return STRUCTDB_CAPI_ERR_STORAGE_IO;
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
}

int structdb_engine_recover_to_checkpoint_seq(structdb_engine* engine, uint64_t checkpoint_seq, char* err,
                                              size_t err_len) {
  if (!engine) {
    write_err_buf(err, err_len, "null engine");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  if (engine->started) {
    write_err_buf(err, err_len, "recover: call structdb_engine_shutdown() first");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
  if (checkpoint_seq == 0) {
    write_err_buf(err, err_len, "recover: checkpoint_seq must be > 0");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  try {
    std::string e;
    if (!engine->engine.recover_to_checkpoint_seq(checkpoint_seq, &e)) {
      write_err_buf(err, err_len, e.empty() ? "recover failed" : e.c_str());
      return STRUCTDB_CAPI_ERR_STORAGE_IO;
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
}

int structdb_backup_bundle(const char* data_dir_utf8, const char* session_dir_utf8, const char* out_dir_utf8, char* err,
                           size_t err_len) {
  if (!utf8_nonempty(data_dir_utf8) || !utf8_nonempty(session_dir_utf8) || !utf8_nonempty(out_dir_utf8)) {
    write_err_buf(err, err_len, "null or empty path");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  namespace fs = std::filesystem;
  const fs::path data = utf8_path_to_fs(data_dir_utf8);
  const fs::path sess = utf8_path_to_fs(session_dir_utf8);
  const fs::path out_root = utf8_path_to_fs(out_dir_utf8);
  if (!fs::exists(data)) {
    write_err_buf(err, err_len, "backup: data_dir not found");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
  if (!fs::exists(sess)) {
    write_err_buf(err, err_len, "backup: session_dir not found");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
  try {
    std::error_code ec;
    fs::create_directories(out_root, ec);
    const fs::path out_data = out_root / "data_dir";
    const fs::path out_sess = out_root / "session_dir";
    auto copy_tree = [&](const fs::path& src, const fs::path& dst) -> bool {
      if (!fs::exists(src)) return true;
      fs::create_directories(dst, ec);
      for (fs::recursive_directory_iterator it(src, fs::directory_options::skip_permission_denied);
           it != fs::recursive_directory_iterator(); ++it) {
        const auto rel = fs::relative(it->path(), src, ec);
        if (ec) return false;
        const fs::path target = dst / rel;
        if (it->is_directory()) {
          fs::create_directories(target, ec);
        } else if (it->is_regular_file()) {
          fs::create_directories(target.parent_path(), ec);
          fs::copy_file(it->path(), target, fs::copy_options::overwrite_existing, ec);
          if (ec) return false;
        }
      }
      return true;
    };
    if (!copy_tree(data, out_data) || !copy_tree(sess, out_sess)) {
      write_err_buf(err, err_len, "backup copy failed");
      return STRUCTDB_CAPI_ERR_STORAGE_IO;
    }
    std::vector<structdb::storage::CheckpointChainEntry> chain_entries;
    if (structdb::storage::checkpoint_chain_read_all(data, &chain_entries, nullptr) && !chain_entries.empty()) {
      std::ofstream manifest(out_root / "backup_manifest.json", std::ios::trunc);
      if (manifest) {
        manifest << "{\"last_checkpoint_seq\":" << chain_entries.back().checkpoint_seq << "}\n";
      }
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
}

structdb_embed_client* structdb_embed_open(structdb_engine* engine, const char* session_dir, char* err,
                                           size_t err_len) {
  if (!engine) {
    write_err_buf(err, err_len, "null engine");
    return nullptr;
  }
  if (!engine->started) {
    write_err_buf(err, err_len, "engine not started");
    return nullptr;
  }
  if (engine->active_embed) {
    write_err_buf(err, err_len, "embed session already open on this engine");
    return nullptr;
  }
  auto* box = new (std::nothrow) structdb_embed_client();
  if (!box) {
    write_err_buf(err, err_len, "allocation failed");
    return nullptr;
  }
  box->engine = engine;
  try {
    box->client = std::make_unique<structdb::client::EmbedClient>(engine->engine);
    std::string e;
    const std::filesystem::path session_fs =
        utf8_nonempty(session_dir) ? utf8_path_to_fs(session_dir)
                                   : utf8_path_to_fs(engine->data_dir_utf8.c_str()) / "embed_session";
    if (!box->client->open(session_fs, &e)) {
      write_err_buf(err, err_len, e.c_str());
      box->client.reset();
      delete box;
      return nullptr;
    }
    box->session_dir_utf8 = path_to_utf8(box->client->session_directory());
    box->session_log_utf8 = path_to_utf8(box->client->embed_session_log_path());
    engine->active_embed = box;
    return box;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    delete box;
    return nullptr;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    delete box;
    return nullptr;
  }
}

size_t structdb_embed_get_session_dir_utf8(structdb_embed_client* client, char* out, size_t out_cap) {
  if (!client || !client->client) return 0;
  return copy_utf8_cstr(out, out_cap, client->session_dir_utf8);
}

size_t structdb_embed_get_session_log_path_utf8(structdb_embed_client* client, char* out, size_t out_cap) {
  if (!client || !client->client) return 0;
  return copy_utf8_cstr(out, out_cap, client->session_log_utf8);
}

int structdb_embed_save_checkpoint(structdb_embed_client* client, char* err, size_t err_len) {
  if (!client || !client->client) {
    write_err_buf(err, err_len, "null embed or not open");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  try {
    std::string e;
    if (!client->client->save_checkpoint(&e)) {
      write_err_buf(err, err_len, e.empty() ? "save_checkpoint failed" : e.c_str());
      return STRUCTDB_CAPI_ERR_STORAGE_IO;
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  } catch (...) {
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_STORAGE_IO;
  }
}

uint64_t structdb_embed_last_ack_seq(structdb_embed_client* client) {
  if (!client || !client->client) return 0;
  try {
    return client->client->last_ack_seq();
  } catch (...) {
    return 0;
  }
}

uint64_t structdb_embed_next_seq(structdb_embed_client* client) {
  if (!client || !client->client) return 0;
  try {
    return client->client->next_seq();
  } catch (...) {
    return 0;
  }
}

uint64_t structdb_embed_read_snapshot_seq(structdb_embed_client* client) {
  if (!client || !client->client) return 0;
  try {
    return client->client->read_snapshot_seq();
  } catch (...) {
    return 0;
  }
}

void structdb_embed_close(structdb_embed_client* client) {
  if (!client) return;
  if (client->engine && client->engine->active_embed == client) {
    client->engine->active_embed = nullptr;
  }
  if (client->client) {
    try {
      client->client->close();
    } catch (...) {
    }
    client->client.reset();
  }
  delete client;
}

structdb_mdb_session* structdb_mdb_session_create(void) {
  return new (std::nothrow) structdb_mdb_session();
}

void structdb_mdb_session_destroy(structdb_mdb_session* session) { delete session; }

void structdb_mdb_session_reset(structdb_mdb_session* session) {
  if (!session) return;
  structdb::client::mdb::mdb_repl_reset(session->impl);
}

int structdb_mdb_session_set_durability(structdb_mdb_session* session, int level) {
  if (!session) return STRUCTDB_CAPI_ERR_NULL_ARG;
  if (level < 0 || level > 2) return STRUCTDB_CAPI_ERR_NULL_ARG;
  session->impl.set_session_durability_level(level);
  return STRUCTDB_CAPI_OK;
}

int structdb_mdb_execute_line(structdb_engine* engine, structdb_embed_client* client, structdb_mdb_session* session,
                              const char* line, char* err, size_t err_len) {
  return structdb_mdb_execute_line_ex(engine, client, session, line, err, err_len, nullptr);
}

int structdb_mdb_execute_line_ex(structdb_engine* engine, structdb_embed_client* client, structdb_mdb_session* session,
                                 const char* line, char* err, size_t err_len, const structdb_mdb_run_options* opts) {
  structdb_mdb_run_options eff{};
  if (!effective_mdb_options(opts, &eff)) {
    write_err_buf(err, err_len,
                  "structdb_mdb_run_options: struct_size must be STRUCTDB_MDB_RUN_OPTIONS_SIZE_V1, V2, or V3");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  if (!engine || !client || !session || !line) {
    write_err_buf(err, err_len, "null argument");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  if (client->engine != engine) {
    write_err_buf(err, err_len, "embed client does not belong to this engine");
    return STRUCTDB_CAPI_ERR_NULL_ARG;
  }
  if (!engine->started || !client->client) {
    write_err_buf(err, err_len, "engine or embed not ready");
    return STRUCTDB_CAPI_ERR_EMBED_OPEN;
  }
  if (engine->active_embed != client) {
    write_err_buf(err, err_len, "embed client is not the active open session for this engine");
    return STRUCTDB_CAPI_ERR_EMBED_OPEN;
  }
  if (long_task_cancelled(eff, engine)) {
    write_err_buf(err, err_len, "cancelled");
    return STRUCTDB_CAPI_ERR_CANCELLED;
  }
  const bool had_batch = engine->script_batch_long_task != nullptr;
  if (eff.long_task_control && !had_batch) bind_long_task_storage(engine, eff.long_task_control);
  try {
    std::vector<std::string> line_log;
    std::string err_s;
    const bool want_log = (eff.log_file_path && eff.log_file_path[0] != '\0') || eff.log_line_cb;
    const auto r = structdb::client::mdb::mdb_repl_execute_line(
        engine->engine, *client->client, session->impl, line, want_log ? &line_log : nullptr,
        eff.fsync_each_batch != 0, eff.fsync_each_session_txn_op != 0, &err_s,
        eff.allow_persist_while_txn_active_experimental != 0);
    if (eff.repl_exit_requested_out) *eff.repl_exit_requested_out = r.repl_exit_requested ? 1 : 0;
    emit_logs(eff, line_log, false);
    if (had_batch && r.ok) script_batch_report_line(engine, true);
    if (eff.long_task_control && !had_batch) bind_long_task_storage(engine, nullptr);
    if (!r.ok) {
      std::string msg = r.last_error;
      if (msg.empty()) msg = err_s;
      if (msg.empty()) msg = "mdb failed";
      write_err_buf(err, err_len, msg.c_str());
      return STRUCTDB_CAPI_ERR_MDB_RUN;
    }
    return STRUCTDB_CAPI_OK;
  } catch (const std::exception& ex) {
    if (eff.long_task_control && !had_batch) bind_long_task_storage(engine, nullptr);
    write_err_buf(err, err_len, ex.what());
    return STRUCTDB_CAPI_ERR_ENGINE_STARTUP;
  } catch (...) {
    if (eff.long_task_control && !had_batch) bind_long_task_storage(engine, nullptr);
    write_err_buf(err, err_len, "unknown C++ exception");
    return STRUCTDB_CAPI_ERR_ENGINE_STARTUP;
  }
}

structdb_long_task_control* structdb_long_task_control_create() {
  auto* box = new (std::nothrow) structdb_long_task_control();
  if (box) box->wire_progress_callback();
  return box;
}

void structdb_long_task_control_destroy(structdb_long_task_control* ctrl) { delete ctrl; }

void structdb_long_task_control_reset(structdb_long_task_control* ctrl) {
  if (!ctrl) return;
  ctrl->cancel = std::make_shared<structdb::infra::LongTaskCancelToken>();
  ctrl->reporter.bind_cancel_token(ctrl->cancel);
  ctrl->wire_progress_callback();
}

void structdb_long_task_control_request_cancel(structdb_long_task_control* ctrl) {
  if (!ctrl) return;
  ctrl->cancel->request_cancel();
}

int structdb_long_task_control_cancel_requested(const structdb_long_task_control* ctrl) {
  if (!ctrl) return 0;
  return ctrl->cancel->cancel_requested() ? 1 : 0;
}

void structdb_long_task_control_set_progress_callback(structdb_long_task_control* ctrl,
                                                        structdb_long_task_progress_fn cb, void* user_data) {
  if (!ctrl) return;
  ctrl->progress_cb = cb;
  ctrl->progress_user = user_data;
  ctrl->wire_progress_callback();
}

void structdb_engine_bind_long_task(structdb_engine* engine, structdb_long_task_control* ctrl) {
  bind_long_task_storage(engine, ctrl);
}

void structdb_engine_begin_mdb_script_batch(structdb_engine* engine, structdb_long_task_control* ctrl,
                                            uint64_t executable_line_total) {
  if (!engine || !ctrl) return;
  ctrl->reporter.set_kind(structdb::infra::LongTaskKind::MdbScript);
  engine->script_batch_long_task = ctrl;
  engine->script_batch_lines_total = executable_line_total;
  engine->script_batch_lines_done = 0;
  bind_long_task_storage(engine, ctrl);
  script_batch_report_line(engine, false);
}

void structdb_engine_end_mdb_script_batch(structdb_engine* engine) {
  if (!engine) return;
  if (engine->script_batch_long_task) {
    const bool cancelled = engine->script_batch_long_task->cancel->cancel_requested();
    engine->script_batch_long_task->reporter.report(
        cancelled ? structdb::infra::LongTaskStatus::Cancelled : structdb::infra::LongTaskStatus::Completed,
        engine->script_batch_lines_done, engine->script_batch_lines_total);
  }
  engine->script_batch_long_task = nullptr;
  engine->script_batch_lines_total = 0;
  engine->script_batch_lines_done = 0;
  bind_long_task_storage(engine, nullptr);
}

}  // extern "C"
