    switch (pl.verb) {
      case MdbVerb::CountBare: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        {
          std::ostringstream o;
          o << "[COUNT] rows=" << current.rows.size();
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::CountPred: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        auto pr = split_csv_paren_content(pl.paren_inner);
        if (pr.size() != 3) {
          res.ok = false;
          res.last_error = "COUNT: need (col,op,val)";
          return res;
        }
        const std::size_t matched =
            count_matching_row_ids(current, pr[0], pr[1], pr[2], &ports, storage_read_seq);
        {
          std::ostringstream o;
          o << "[COUNT] matched " << matched << " / " << current.rows.size() << " rows";
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::Where: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        auto pr = split_csv_paren_content(pl.paren_inner);
        if (pr.size() != 3) {
          res.ok = false;
          res.last_error = "WHERE: need (col,op,val)";
          return res;
        }
        const std::size_t matched =
            count_matching_row_ids(current, pr[0], pr[1], pr[2], &ports, storage_read_seq);
        {
          std::ostringstream o;
          o << "[WHERE] matched " << matched << " / " << current.rows.size() << " rows";
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::WhereP: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        auto pr = split_csv_paren_content(pl.paren_inner);
        if (pr.size() < 5) {
          res.ok = false;
          res.last_error = "WHEREP: need (proj,WHERE,key,=,val)";
          return res;
        }
        if (!col_name_eq(pr[1], "WHERE")) {
          res.ok = false;
          res.last_error = "WHEREP: expected WHERE";
          return res;
        }
        if (pr[3] != "=") {
          res.ok = false;
          res.last_error = "WHEREP: only '=' supported";
          return res;
        }
        if (col_name_eq(pr[2], "id")) {
          res.ok = false;
          res.last_error = "WHEREP: key_attr must be non-id";
          return res;
        }
        const auto ids = collect_matching_row_ids(current, pr[2], pr[3], pr[4], &ports, storage_read_seq);
        const int pix = schema_col_index(current, pr[0]);
        if (pix < 0) {
          res.ok = false;
          res.last_error = "WHEREP: unknown proj column";
          return res;
        }
        {
          std::ostringstream o;
          o << "[WHEREP] key=" << pr[2] << "=" << pr[4] << " proj=" << pr[0] << " rows=" << ids.size();
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        std::size_t n = 0;
        for (const std::string& rid : ids) {
          if (n++ >= 50) break;
          const auto& cells = current.rows.at(rid);
          std::ostringstream r;
          r << "  id=" << rid << " " << pr[0] << "=" << cells[static_cast<std::size_t>(pix)];
          log_line(log_sink ? &line_log : nullptr, r.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::Find: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        const std::string id = trim_copy(pl.paren_inner);
        auto it = current.rows.find(id);
        if (it == current.rows.end()) {
          std::ostringstream o;
          o << "[FIND] id=" << id << " not found";
          log_line(log_sink ? &line_log : nullptr, o.str());
        } else {
          std::ostringstream o;
          o << "[FIND] id=" << id;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::FindPk: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "FINDPK: no table/schema";
          return res;
        }
        const std::string pkcol = std::string(effective_pk_column_name(current));
        const int pki = schema_col_index(current, pkcol);
        if (pki < 0) {
          res.ok = false;
          res.last_error = "FINDPK: bad PK column";
          return res;
        }
        const std::string val = trim_copy(pl.paren_inner);
        bool found = false;
        for (const auto& kv : current.rows) {
          if (kv.first == val) {
            std::ostringstream o;
            o << "[FINDPK] id=" << val;
            log_line(log_sink ? &line_log : nullptr, o.str());
            found = true;
            break;
          }
        }
        if (!found) {
          for (const auto& kv : current.rows) {
            if (static_cast<std::size_t>(pki) < kv.second.size() && kv.second[static_cast<std::size_t>(pki)] == val) {
              std::ostringstream o;
              o << "[FINDPK] " << pkcol << "=" << val << " id=" << kv.first;
              log_line(log_sink ? &line_log : nullptr, o.str());
              found = true;
              break;
            }
          }
        }
        if (!found) {
          std::ostringstream o;
          o << "[FINDPK] not found: " << pkcol << "=" << val;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::Sum:
      case MdbVerb::Avg:
      case MdbVerb::Min:
      case MdbVerb::Max: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        const std::string col = trim_copy(pl.paren_inner);
        const int ci = schema_col_index(current, col);
        if (ci < 0) {
          res.ok = false;
          res.last_error = "aggregate: unknown column";
          return res;
        }
        const std::string_view typ = current.schema[static_cast<std::size_t>(ci)].second;
        const bool is_int_col = ascii_starts_with_ci(typ, "int");
        const bool is_fp_col = ascii_starts_with_ci(typ, "float") || ascii_starts_with_ci(typ, "double");
        const bool is_dt_only = ascii_starts_with_ci(typ, "datetime");
        const bool is_ts = ascii_starts_with_ci(typ, "timestamp");
        if (!is_int_col && !is_fp_col && !is_dt_only && !is_ts) {
          res.ok = false;
          res.last_error = "aggregate: column type not supported (use int/float/double/datetime/timestamp)";
          return res;
        }
        long double acc = 0;
        int n = 0;
        long double smin = 0;
        long double smax = 0;
        bool have_minmax = false;
        int ymin = 0;
        int momin = 0;
        int dmin = 0;
        int hmin = 0;
        int mimin = 0;
        int sminsec = 0;
        int ymax = 0;
        int momax = 0;
        int dmax = 0;
        int hmax = 0;
        int mimax = 0;
        int smaxsec = 0;
        bool min_from_calendar = false;
        bool max_from_calendar = false;
        if (pl.verb == MdbVerb::Sum && is_int_col) {
          long long cached_sum = 0;
          int cached_n = 0;
          if (logical_agg_try_int_sum(current, col, &cached_sum, &cached_n)) {
            std::ostringstream o;
            o << "[SUM] " << col << "=" << cached_sum;
            log_line(log_sink ? &line_log : nullptr, o.str());
            flush_line_log();
            break;
          }
        }
        for (const auto& kv : current.rows) {
          const std::string& cell = kv.second[static_cast<std::size_t>(ci)];
          if (pl.verb == MdbVerb::Sum || pl.verb == MdbVerb::Avg) {
            if (is_dt_only) {
              res.ok = false;
              res.last_error = "SUM/AVG: datetime column not supported";
              return res;
            }
            if (is_ts) {
              if (!is_int_literal(cell)) continue;
              acc += static_cast<long double>(std::stoll(cell));
              ++n;
            } else if (is_int_col) {
              if (!is_int_literal(cell)) continue;
              acc += static_cast<long double>(std::stoll(cell));
              ++n;
            } else if (is_fp_col) {
              std::string t(cell);
              double v = 0.0;
              if (!parse_full_double_str(t, &v)) continue;
              acc += static_cast<long double>(v);
              ++n;
            }
          } else if (pl.verb == MdbVerb::Min) {
            if (is_fp_col) {
              std::string t(cell);
              double v = 0.0;
              if (!parse_full_double_str(t, &v)) continue;
              const long double lv = static_cast<long double>(v);
              if (!have_minmax || lv < smin) {
                smin = lv;
                have_minmax = true;
              }
            } else if (is_dt_only) {
              int y = 0;
              int mo = 0;
              int d = 0;
              int hh = 0;
              int mi = 0;
              int ss = 0;
              bool ht = false;
              if (!parse_datetime_calendar(cell, &y, &mo, &d, &hh, &mi, &ss, &ht)) continue;
              if (!have_minmax || cmp_datetime_parts(y, mo, d, hh, mi, ss, ymin, momin, dmin, hmin, mimin, sminsec) <
                                     0) {
                ymin = y;
                momin = mo;
                dmin = d;
                hmin = hh;
                mimin = mi;
                sminsec = ss;
                have_minmax = true;
                min_from_calendar = true;
              }
            } else if (is_ts) {
              if (is_int_literal(cell)) {
                const long double v = static_cast<long double>(std::stoll(cell));
                if (!have_minmax || v < smin) {
                  smin = v;
                  have_minmax = true;
                  min_from_calendar = false;
                }
              } else {
                int y = 0;
                int mo = 0;
                int d = 0;
                int hh = 0;
                int mi = 0;
                int ss = 0;
                bool ht = false;
                if (!parse_datetime_calendar(cell, &y, &mo, &d, &hh, &mi, &ss, &ht)) continue;
                if (!have_minmax || cmp_datetime_parts(y, mo, d, hh, mi, ss, ymin, momin, dmin, hmin, mimin, sminsec) <
                                       0) {
                  ymin = y;
                  momin = mo;
                  dmin = d;
                  hmin = hh;
                  mimin = mi;
                  sminsec = ss;
                  have_minmax = true;
                  min_from_calendar = true;
                }
              }
            } else if (is_int_col) {
              if (!is_int_literal(cell)) continue;
              const long double v = static_cast<long double>(std::stoll(cell));
              if (!have_minmax || v < smin) {
                smin = v;
                have_minmax = true;
                min_from_calendar = false;
              }
            }
          } else {
            if (is_fp_col) {
              std::string t(cell);
              double v = 0.0;
              if (!parse_full_double_str(t, &v)) continue;
              const long double lv = static_cast<long double>(v);
              if (!have_minmax || lv > smax) {
                smax = lv;
                have_minmax = true;
              }
            } else if (is_dt_only) {
              int y = 0;
              int mo = 0;
              int d = 0;
              int hh = 0;
              int mi = 0;
              int ss = 0;
              bool ht = false;
              if (!parse_datetime_calendar(cell, &y, &mo, &d, &hh, &mi, &ss, &ht)) continue;
              if (!have_minmax || cmp_datetime_parts(y, mo, d, hh, mi, ss, ymax, momax, dmax, hmax, mimax, smaxsec) >
                                     0) {
                ymax = y;
                momax = mo;
                dmax = d;
                hmax = hh;
                mimax = mi;
                smaxsec = ss;
                have_minmax = true;
                max_from_calendar = true;
              }
            } else if (is_ts) {
              if (is_int_literal(cell)) {
                const long double v = static_cast<long double>(std::stoll(cell));
                if (!have_minmax || v > smax) {
                  smax = v;
                  have_minmax = true;
                  max_from_calendar = false;
                }
              } else {
                int y = 0;
                int mo = 0;
                int d = 0;
                int hh = 0;
                int mi = 0;
                int ss = 0;
                bool ht = false;
                if (!parse_datetime_calendar(cell, &y, &mo, &d, &hh, &mi, &ss, &ht)) continue;
                if (!have_minmax || cmp_datetime_parts(y, mo, d, hh, mi, ss, ymax, momax, dmax, hmax, mimax, smaxsec) >
                                       0) {
                  ymax = y;
                  momax = mo;
                  dmax = d;
                  hmax = hh;
                  mimax = mi;
                  smaxsec = ss;
                  have_minmax = true;
                  max_from_calendar = true;
                }
              }
            } else if (is_int_col) {
              if (!is_int_literal(cell)) continue;
              const long double v = static_cast<long double>(std::stoll(cell));
              if (!have_minmax || v > smax) {
                smax = v;
                have_minmax = true;
                max_from_calendar = false;
              }
            }
          }
        }
        std::ostringstream o;
        if (pl.verb == MdbVerb::Sum) {
          o << "[SUM] " << col << "=";
          if (is_fp_col)
            o << static_cast<double>(acc);
          else
            o << static_cast<long long>(acc);
        }
        if (pl.verb == MdbVerb::Avg) {
          o << "[AVG] " << col << "=" << (n ? static_cast<double>(acc / static_cast<long double>(n)) : 0.0);
        }
        if (pl.verb == MdbVerb::Min) {
          o << "[MIN] " << col << "=";
          if (!have_minmax) {
          } else if (is_dt_only || (is_ts && min_from_calendar)) {
            mdb_append_iso_datetime(o, ymin, momin, dmin, hmin, mimin, sminsec);
          } else if (is_fp_col) {
            o << static_cast<double>(smin);
          } else {
            o << static_cast<long long>(smin);
          }
        }
        if (pl.verb == MdbVerb::Max) {
          o << "[MAX] " << col << "=";
          if (!have_minmax) {
          } else if (is_dt_only || (is_ts && max_from_calendar)) {
            mdb_append_iso_datetime(o, ymax, momax, dmax, hmax, mimax, smaxsec);
          } else if (is_fp_col) {
            o << static_cast<double>(smax);
          } else {
            o << static_cast<long long>(smax);
          }
        }
        log_line(log_sink ? &line_log : nullptr, o.str());
        flush_line_log();
        break;
      }
      case MdbVerb::Qbal: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "QBAL: no table/schema";
          return res;
        }
        const auto args = split_csv_paren_content(pl.paren_inner);
        if (args.size() != 2) {
          res.ok = false;
          res.last_error = "QBAL: need (col,min)";
          return res;
        }
        const std::string col = args[0];
        const std::string smin = args[1];
        const int ci = schema_col_index(current, col);
        if (ci < 0) {
          res.ok = false;
          res.last_error = "QBAL: unknown column";
          return res;
        }
        const std::string_view typ = current.schema[static_cast<std::size_t>(ci)].second;
        const bool col_int = ascii_starts_with_ci(typ, "int");
        const bool col_fp = ascii_starts_with_ci(typ, "float") || ascii_starts_with_ci(typ, "double");
        if (!col_int && !col_fp) {
          res.ok = false;
          res.last_error = "QBAL: column must be int or float/double";
          return res;
        }
        double minv = 0.0;
        if (col_int) {
          if (!is_int_literal(smin)) {
            res.ok = false;
            res.last_error = "QBAL: min must be int for int column";
            return res;
          }
          minv = static_cast<double>(std::stoll(smin));
        } else {
          std::string ms(smin);
          if (!parse_full_double_str(ms, &minv)) {
            res.ok = false;
            res.last_error = "QBAL: min must be numeric for float/double column";
            return res;
          }
        }
        long long sum_i = 0;
        double sum_f = 0.0;
        std::size_t cnt = 0;
        if (col_int) {
          const long long minv_i = static_cast<long long>(minv);
          long long cached_sum = 0;
          std::size_t cached_matched = 0;
          if (logical_agg_try_qbal_int_ge(current, col, minv_i, &cached_sum, &cached_matched)) {
            std::ostringstream o;
            o << "[QBAL] col=" << col << " min=" << smin << " matched=" << cached_matched << " sum=" << cached_sum;
            log_line(log_sink ? &line_log : nullptr, o.str());
            flush_line_log();
            break;
          }
        }
        for (const auto& kv : current.rows) {
          const std::string& cell = kv.second[static_cast<std::size_t>(ci)];
          if (col_int) {
            if (!is_int_literal(cell)) continue;
            const long long v = std::stoll(cell);
            if (static_cast<double>(v) >= minv - 1e-12) {
              sum_i += v;
              ++cnt;
            }
          } else {
            std::string tc(cell);
            double v = 0.0;
            if (!parse_full_double_str(tc, &v)) continue;
            const double mag = (std::max)(std::abs(v), std::abs(minv));
            const double eps = 1e-9 * (1.0 + mag);
            if (v >= minv - eps) {
              sum_f += v;
              ++cnt;
            }
          }
        }
        {
          std::ostringstream o;
          o << "[QBAL] col=" << col << " min=" << smin << " matched=" << cnt << " sum=";
          if (col_int)
            o << sum_i;
          else
            o << sum_f;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::UpdateWhere: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        auto pr = split_csv_paren_content(pl.paren_inner);
        if (pr.size() < 6) {
          res.ok = false;
          res.last_error = "UPDATEWHERE: bad args";
          return res;
        }
        if (!col_name_eq(pr[2], "WHERE")) {
          res.ok = false;
          res.last_error = "UPDATEWHERE: expected WHERE";
          return res;
        }
        const std::string& ucol = pr[0];
        const std::string& newval = pr[1];
        const int uix = schema_col_index(current, ucol);
        if (uix < 0) {
          res.ok = false;
          res.last_error = "UPDATEWHERE: unknown update column";
          return res;
        }
        if (!type_matches(current.schema[static_cast<std::size_t>(uix)].second, newval)) {
          const std::string msg =
              type_mismatch_msg(current.schema[static_cast<std::size_t>(uix)].first,
                                current.schema[static_cast<std::size_t>(uix)].second, newval);
          log_line(log_sink ? &line_log : nullptr, msg);
          flush_line_log();
          res.ok = false;
          res.last_error = msg;
          return res;
        }
        const auto ids = collect_matching_row_ids(current, pr[3], pr[4], pr[5], &ports, storage_read_seq);
        for (const std::string& rid : ids) {
          auto it = current.rows.find(rid);
          if (it == current.rows.end()) continue;
          std::vector<std::string> prev = it->second;
          logical_persist_mark_update(&current, rid, prev);
          logical_str_index_remove_row(&current, rid, it->second);
          it->second[static_cast<std::size_t>(uix)] = newval;
          logical_str_index_add_row(&current, rid, it->second);
        }
        {
          std::ostringstream o;
          o << "[UPDATEWHERE] ok: updated " << ids.size() << " rows";
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        if (txn_active) {
          std::string pay;
          pay.append(pr[0]);
          pay.push_back('\t');
          pay.append(pr[1]);
          pay.push_back('\t');
          pay.append(pr[3]);
          pay.push_back('\t');
          pay.append(pr[4]);
          pay.push_back('\t');
          pay.append(pr[5]);
          if (!txn_v2_append("UPDATEWHERE", pay)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::DeleteWhere: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        auto pr = split_csv_paren_content(pl.paren_inner);
        if (pr.size() != 3) {
          res.ok = false;
          res.last_error = "DELETEWHERE: need (col,op,val)";
          return res;
        }
        const auto ids = collect_matching_row_ids(current, pr[0], pr[1], pr[2], &ports, storage_read_seq);
        for (const std::string& rid : ids) {
          auto it = current.rows.find(rid);
          if (it == current.rows.end()) continue;
          std::vector<std::string> prev = it->second;
          logical_persist_mark_delete_before_erase(&current, rid, prev);
          logical_str_index_remove_row(&current, rid, it->second);
          current.rows.erase(it);
        }
        {
          std::ostringstream o;
          o << "[DELETEWHERE] ok: removed " << ids.size() << " rows";
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        if (txn_active) {
          std::string pay;
          pay.append(pr[0]);
          pay.push_back('\t');
          pay.append(pr[1]);
          pay.push_back('\t');
          pay.append(pr[2]);
          if (!txn_v2_append("DELETEWHERE", pay)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::SetAttrMulti: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        auto pr = split_csv_paren_content(pl.paren_inner);
        if (pr.size() < 3) {
          res.ok = false;
          res.last_error = "SETATTRMULTI: need col,val,ids...";
          return res;
        }
        const std::string& ucol = pr[0];
        const std::string& newval = pr[1];
        const int uix = schema_col_index(current, ucol);
        if (uix < 0) {
          res.ok = false;
          res.last_error = "SETATTRMULTI: unknown column";
          return res;
        }
        if (!type_matches(current.schema[static_cast<std::size_t>(uix)].second, newval)) {
          const std::string msg =
              type_mismatch_msg(current.schema[static_cast<std::size_t>(uix)].first,
                                current.schema[static_cast<std::size_t>(uix)].second, newval);
          log_line(log_sink ? &line_log : nullptr, msg);
          flush_line_log();
          res.ok = false;
          res.last_error = msg;
          return res;
        }
        std::size_t nupd = 0;
        for (std::size_t i = 2; i < pr.size(); ++i) {
          auto it = current.rows.find(pr[i]);
          if (it == current.rows.end()) continue;
          std::vector<std::string> prev_row = it->second;
          logical_persist_mark_update(&current, it->first, std::move(prev_row));
          logical_str_index_remove_row(&current, it->first, it->second);
          it->second[static_cast<std::size_t>(uix)] = newval;
          logical_str_index_add_row(&current, it->first, it->second);
          ++nupd;
        }
        {
          std::ostringstream o;
          o << "[SETATTRMULTI] ok: updated " << nupd << " rows";
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        if (txn_active) {
          std::string pay;
          pay.append(ucol);
          pay.push_back('\t');
          pay.append(newval);
          for (std::size_t i = 2; i < pr.size(); ++i) {
            pay.push_back('\t');
            pay.append(pr[i]);
          }
          if (!txn_v2_append("SETATTRMULTI", pay)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::SetAttr: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        auto pr = split_csv_paren_content(pl.paren_inner);
        if (pr.size() != 3) {
          res.ok = false;
          res.last_error = "SETATTR: need (id,col,val)";
          return res;
        }
        const std::string& rid = pr[0];
        const std::string& ucol = pr[1];
        const std::string& newval = pr[2];
        auto it = current.rows.find(rid);
        if (it == current.rows.end()) {
          res.ok = false;
          res.last_error = "SETATTR: missing row";
          return res;
        }
        const int uix = schema_col_index(current, ucol);
        if (uix < 0) {
          res.ok = false;
          res.last_error = "SETATTR: unknown column";
          return res;
        }
        if (!type_matches(current.schema[static_cast<std::size_t>(uix)].second, newval)) {
          const std::string msg =
              type_mismatch_msg(current.schema[static_cast<std::size_t>(uix)].first,
                                current.schema[static_cast<std::size_t>(uix)].second, newval);
          log_line(log_sink ? &line_log : nullptr, msg);
          flush_line_log();
          res.ok = false;
          res.last_error = msg;
          return res;
        }
        std::vector<std::string> prev_row = it->second;
        logical_persist_mark_update(&current, rid, prev_row);
        logical_str_index_remove_row(&current, rid, it->second);
        it->second[static_cast<std::size_t>(uix)] = newval;
        logical_str_index_add_row(&current, rid, it->second);
        if (txn_active) {
          std::string pay;
          pay.append(rid);
          pay.push_back('\t');
          pay.append(ucol);
          pay.push_back('\t');
          pay.append(newval);
          if (!txn_v2_append("SETATTR", pay)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::DeleteRow: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        const std::string id = trim_copy(pl.paren_inner);
        auto it = current.rows.find(id);
        if (it == current.rows.end()) {
          res.ok = false;
          res.last_error = "DELETE: missing row";
          return res;
        }
        std::vector<std::string> prev_cells = it->second;
        logical_persist_mark_delete_before_erase(&current, id, prev_cells);
        logical_str_index_remove_row(&current, id, it->second);
        current.rows.erase(it);
        if (txn_active && !txn_v2_append("DELETE", id)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::DeletePk: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "DELETEPK: no table/schema";
          return res;
        }
        const std::string val = trim_copy(pl.paren_inner);
        std::string found_id;
        for (const auto& kv : current.rows) {
          if (kv.first == val) {
            found_id = kv.first;
            break;
          }
        }
        if (found_id.empty()) {
          for (const auto& kv : current.rows) {
            if (!kv.second.empty() && kv.second[0] == val) {
              found_id = kv.first;
              break;
            }
          }
        }
        if (found_id.empty()) {
          res.ok = false;
          res.last_error = "DELETEPK: not found";
          return res;
        }
        auto it = current.rows.find(found_id);
        std::vector<std::string> prev_cells = it->second;
        logical_persist_mark_delete_before_erase(&current, found_id, prev_cells);
        logical_str_index_remove_row(&current, found_id, it->second);
        current.rows.erase(it);
        if (txn_active && !txn_v2_append("DELETE", found_id)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::RenAttr: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "RENATTR: no table/schema";
          return res;
        }
        const auto comma = pl.paren_inner.find(',');
        if (comma == std::string_view::npos) {
          res.ok = false;
          res.last_error = "RENATTR: need (old,new)";
          return res;
        }
        const std::string oldn = trim_copy(pl.paren_inner.substr(0, comma));
        const std::string newn = trim_copy(pl.paren_inner.substr(comma + 1));
        if (oldn.empty() || newn.empty()) {
          res.ok = false;
          res.last_error = "RENATTR: empty name";
          return res;
        }
        const int ci = schema_col_index(current, oldn);
        if (ci < 0) {
          res.ok = false;
          res.last_error = "RENATTR: unknown column";
          return res;
        }
        current.schema[static_cast<std::size_t>(ci)].first = newn;
        logical_rebuild_str_index(&current);
        logical_persist_mark_schema_change(&current);
        if (txn_active) {
          std::string pay;
          pay.append(oldn);
          pay.push_back('\t');
          pay.append(newn);
          if (!txn_v2_append("RENATTR", pay)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr, std::string("[RENATTR] ok ") + oldn + " -> " + newn);
        flush_line_log();
        break;
      }
      case MdbVerb::BulkInsert: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "BULKINSERT: no table/schema";
          return res;
        }
        const auto chunks = split_pipe_rows(pl.paren_inner);
        if (chunks.empty()) {
          res.ok = false;
          res.last_error = "BULKINSERT: empty";
          return res;
        }
        for (const std::string_view chunk : chunks) {
          const auto cells = split_csv_paren_content(chunk);
          if (cells.size() != 1 + current.schema.size()) {
            res.ok = false;
            res.last_error = "BULKINSERT: arity";
            return res;
          }
          const std::string& id = cells[0];
          if (current.rows.count(id)) {
            res.ok = false;
            res.last_error = "BULKINSERT: duplicate id";
            return res;
          }
          std::vector<std::string> vals(cells.begin() + 1, cells.end());
          for (std::size_t c = 0; c < current.schema.size(); ++c) {
            if (!type_matches(current.schema[c].second, vals[c])) {
              const std::string msg =
                  type_mismatch_msg(current.schema[c].first, current.schema[c].second, vals[c]);
              log_line(log_sink ? &line_log : nullptr, msg);
              flush_line_log();
              res.ok = false;
              res.last_error = msg;
              return res;
            }
          }
          current.rows[id] = vals;
          logical_str_index_add_row(&current, id, current.rows[id]);
          logical_persist_mark_insert(&current, id);
        }
        if (txn_active && !txn_v2_append("BULKINSERT", std::string_view(pl.paren_inner))) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!script_amortize_bulk_dml) {
          if (!persist_now()) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        {
          std::ostringstream o;
          o << "[BULKINSERT] rows=" << chunks.size();
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::BulkInsertFast: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "BULKINSERTFAST: no table/schema";
          return res;
        }
        const auto chunks = split_pipe_rows(pl.paren_inner);
        if (chunks.empty()) {
          res.ok = false;
          res.last_error = "BULKINSERTFAST: empty";
          return res;
        }
        for (const std::string_view chunk : chunks) {
          const auto cells = split_csv_paren_content(chunk);
          if (cells.size() != 1 + current.schema.size()) {
            res.ok = false;
            res.last_error = "BULKINSERTFAST: arity";
            return res;
          }
          const std::string& id = cells[0];
          if (current.rows.count(id)) {
            res.ok = false;
            res.last_error = "BULKINSERTFAST: duplicate id";
            return res;
          }
          std::vector<std::string> vals(cells.begin() + 1, cells.end());
          for (std::size_t c = 0; c < current.schema.size(); ++c) {
            if (!type_matches(current.schema[c].second, vals[c])) {
              const std::string msg =
                  type_mismatch_msg(current.schema[c].first, current.schema[c].second, vals[c]);
              log_line(log_sink ? &line_log : nullptr, msg);
              flush_line_log();
              res.ok = false;
              res.last_error = msg;
              return res;
            }
          }
          current.rows[id] = vals;
          logical_str_index_add_row(&current, id, current.rows[id]);
          logical_persist_mark_insert(&current, id);
        }
        if (txn_active && !txn_v2_append("BULKINSERT", std::string_view(pl.paren_inner))) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!script_amortize_bulk_dml) {
          if (!persist_now()) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        {
          std::ostringstream o;
          o << "[BULKINSERTFAST] rows=" << chunks.size();
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        if (current.rows.size() >= 4096 && !script_amortize_bulk_dml) {
          logical_table_refresh_analytics_caches(&current);
        }
        flush_line_log();
        break;
      }
      case MdbVerb::ListTables:
      case MdbVerb::ShowTables: {
        std::set<std::string> names;
        namespace mdb_keyspace = structdb::storage::mdb_keyspace;
        engine.kv_visit_prefix(mdb_keyspace::kCatalog,
                               [&](std::string_view k, std::string_view v) {
                                 (void)v;
                                 const auto pfx = mdb_keyspace::kCatalog;
                                 if (k.size() > pfx.size()) names.insert(std::string(k.substr(pfx.size())));
                                 return true;
                               },
                               storage_read_seq);
        for (const auto& n : names) {
          std::ostringstream o;
          o << "[TABLE] " << n;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::Export: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "EXPORT: no table";
          return res;
        }
        std::filesystem::path out_path;
        if (!parse_export_json_path(pl.tail, &out_path, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        std::error_code ec;
        const auto parent = out_path.parent_path();
        if (!parent.empty()) std::filesystem::create_directories(parent, ec);
        std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
        if (!out) {
          res.ok = false;
          res.last_error = "EXPORT: open failed";
          return res;
        }
        out << "[\n";
        bool first = true;
        for (const auto& kv : current.rows) {
          if (!first) out << ",\n";
          first = false;
          out << "  { \"id\": " << json_quote_cell(kv.first);
          for (std::size_t c = 0; c < current.schema.size(); ++c) {
            out << ", " << json_quote_cell(current.schema[c].first) << ": "
                << json_quote_cell(kv.second[c]);
          }
          out << " }";
        }
        out << "\n]\n";
        if (!out.flush()) {
          res.ok = false;
          res.last_error = "EXPORT: write failed";
          return res;
        }
        {
          std::ostringstream o;
          o << "[EXPORT] json rows=" << current.rows.size() << " path=" << out_path.string();
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::TxnIsolation: {
        if (txn_active) {
          res.ok = false;
          res.last_error =
              "TXNISOLATION: cannot change while a transaction is active; COMMIT or ROLLBACK first";
          return res;
        }
        std::string tl = pl.tail;
        for (char& c : tl) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        trim_inplace(tl);
        txn_read_committed =
            (tl.find("read_committed") != std::string::npos) || (tl.find("read committed") != std::string::npos);
        log_line(log_sink ? &line_log : nullptr,
                 txn_read_committed ? "[TXNISOLATION] read_committed" : "[TXNISOLATION] snapshot");
        flush_line_log();
        break;
      }
      case MdbVerb::ShowTxn: {
        const auto cfg_txn = engine.config().snapshot();
        const char* rollback_pol = cfg_txn.mdb_persist_in_begin
                                       ? (cfg_txn.mdb_chain_rollback_on_mdb_rollback ? "chain_undo" : "session_only")
                                       : "no_persist_in_txn";
        std::ostringstream o;
        o << "[SHOW TXN] active=" << (txn_active ? "yes" : "no")
          << " storage_rollback_policy=" << rollback_pol;
        if (txn_active) {
          o << " read_committed=" << (txn_read_committed ? "yes" : "no") << " snap_seq=" << txn_snap_seq
            << " engine_latest_commit_seq=" << engine.latest_commit_seq()
            << " base_rows=" << txn_base.rows.size();
        }
        log_line(log_sink ? &line_log : nullptr, o.str());
        flush_line_log();
        break;
      }
      case MdbVerb::ShowSnapshot: {
        const auto cfg_snap = engine.config().snapshot();
        const char* rollback_pol = cfg_snap.mdb_persist_in_begin
                                       ? (cfg_snap.mdb_chain_rollback_on_mdb_rollback ? "chain_undo" : "session_only")
                                       : "no_persist_in_txn";
        std::ostringstream o;
        o << "[SHOW SNAPSHOT] embed_read_snapshot_seq=" << client.read_snapshot_seq()
          << " engine_latest_commit_seq=" << engine.latest_commit_seq()
          << " storage_rollback_policy=" << rollback_pol;
        if (txn_active) {
          o << " txn_storage_read_seq=" << storage_read_seq;
        }
        log_line(log_sink ? &line_log : nullptr, o.str());
        flush_line_log();
        break;
      }
      case MdbVerb::Help: {
        log_line(log_sink ? &line_log : nullptr,
                 "[HELP] CREATE/DROP/RENAME/RESET/USE/DEFATTR/ADDATTR([idx,]name:type types:int,string,varchar,text,char,float,double,datetime,"
                 "timestamp)/SET PRIMARY KEY/INSERT/BULKINSERT/BULKINSERTFAST/"
                 "UPDATE/DELETE/DELETEPK/UPDATEWHERE/DELETEWHERE/SETATTR/SETATTRMULTI/DELATTR/RENATTR/WHERE/WHEREP/"
                 "GROUP BY/SCAN INDEX/SET DURABILITY/CREATE INDEX/DROP INDEX/IMPORT SEGMENT/ALTER TABLE ADD|RENAME COLUMN/COUNT/PAGE/PAGE_JSON/FIND/FINDPK/SUM/AVG/MIN/MAX/QBAL/"
                 "SHOW PLAN/EXPLAIN WHERE/EXPORT [JSON] <path>/IMPORTDIR/"
                 "TXNISOLATION/BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE SAVEPOINT/LIST TABLES/SHOW TABLES/SHOW ATTR/"
                 "SHOW KEY/CONFIRM_REORDER+JSON/SHOW TXN/SHOW SNAPSHOT/SHOW TUNING/SHOW STORAGE/SHOWLOG/VACUUM/SCAN[/RESET/MORE]/EXIT + "
                 "[NOT_SUPPORTED] heap-only/newdb-txn verbs [see Docs/PHASE25.md]; CONFIRM_REORDER supported per PHASE38");
        flush_line_log();
        break;
      }
      case MdbVerb::Begin: {
        if (!txn_active) {
          txn_base = clone_table(current);
          txn_active = true;
          txn_snap_seq = engine.latest_commit_seq();
          savepoints.clear();
          if (!txn_log_write_begin(client, txn_base, txn_read_committed, txn_snap_seq, &err)) {
            txn_active = false;
            res.ok = false;
            res.last_error = err;
            return res;
          }
          txn_undo_stack_depth_at_begin = engine.embed_undo_stack_depth();
          {
            const auto cfg = engine.config().snapshot();
            if (cfg.mdb_chain_rollback_on_mdb_rollback && cfg.mdb_persist_in_begin) {
              engine.set_mdb_chain_txn_active_hint(true);
            }
          }
        }
        log_line(log_sink ? &line_log : nullptr, "[BEGIN] ok");
        flush_line_log();
        break;
      }
      case MdbVerb::Commit: {
        if (!txn_active) {
          log_line(log_sink ? &line_log : nullptr, "[COMMIT] no active txn");
          flush_line_log();
          break;
        }
        engine.set_mdb_chain_txn_active_hint(false);
        txn_active = false;
        savepoints.clear();
        txn_undo_stack_depth_at_begin.reset();
        {
          MdbEnginePorts commit_ports = ports;
          mdb_ports_set_bulk_import_persist(&commit_ports, bulk_import_mode);
          if (!persist_table(commit_ports, current, idem, fsync_batch, &err)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
          if (bulk_import_mode &&
              !rebuild_secondary_index_storage(commit_ports, current, idem + ":rebuild", fsync_batch, &err)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        txn_log_remove_if_exists(client);
        log_line(log_sink ? &line_log : nullptr, "[COMMIT] ok");
        flush_line_log();
        break;
      }
      case MdbVerb::Rollback: {
        if (!txn_active) {
          log_line(log_sink ? &line_log : nullptr, "[ROLLBACK] no active txn");
          flush_line_log();
          break;
        }
        {
          const auto cfg = engine.config().snapshot();
          if (txn_undo_stack_depth_at_begin.has_value() && cfg.mdb_chain_rollback_on_mdb_rollback &&
              cfg.mdb_persist_in_begin) {
            if (!engine.rollback_embed_undo_until(*txn_undo_stack_depth_at_begin, &err)) {
              res.ok = false;
              res.last_error = err;
              return res;
            }
          }
        }
        txn_undo_stack_depth_at_begin.reset();
        current = clone_table(txn_base);
        logical_rebuild_str_index(&current);
        txn_active = false;
        savepoints.clear();
        txn_log_remove_if_exists(client);
        log_line(log_sink ? &line_log : nullptr, "[ROLLBACK] rolled back");
        flush_line_log();
        engine.set_mdb_chain_txn_active_hint(false);
        break;
      }
      case MdbVerb::Savepoint: {
        if (!txn_active) {
          res.ok = false;
          res.last_error = "SAVEPOINT: no active txn";
          return res;
        }
        if (pl.tail.empty()) {
          res.ok = false;
          res.last_error = "SAVEPOINT: empty name";
          return res;
        }
        savepoints[pl.tail] = clone_table(current);
        log_line(log_sink ? &line_log : nullptr, std::string("[SAVEPOINT] ") + pl.tail);
        flush_line_log();
        break;
      }
      case MdbVerb::RollbackToSavepoint: {
        if (!txn_active) {
          res.ok = false;
          res.last_error = "ROLLBACK TO SAVEPOINT: no txn";
          return res;
        }
        if (pl.tail.empty()) {
          res.ok = false;
          res.last_error = "ROLLBACK TO SAVEPOINT: empty name";
          return res;
        }
        auto it = savepoints.find(pl.tail);
        if (it == savepoints.end()) {
          res.ok = false;
          res.last_error = "unknown savepoint";
          return res;
        }
        current = clone_table(it->second);
        logical_rebuild_str_index(&current);
        txn_log_remove_if_exists(client);
        if (!txn_log_write_begin(client, current, txn_read_committed, txn_snap_seq, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr, std::string("[ROLLBACK TO SAVEPOINT] ") + pl.tail);
        flush_line_log();
        break;
      }
      case MdbVerb::Page: {
        if (!handle_page(current, pl.paren_inner, log_sink ? &line_log : nullptr, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::PageJson: {
        if (!handle_page_json(current, pl.paren_inner, log_sink ? &line_log : nullptr, &err, &paging)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::CreateTable: {
        const std::string tname = trim_copy(pl.paren_inner);
        if (tname.empty()) {
          res.ok = false;
          res.last_error = "empty table name";
          return res;
        }
        if (table_exists_in_storage(ports, tname, storage_read_seq)) {
          res.ok = false;
          res.last_error = "table exists";
          return res;
        }
        current.name = tname;
        current.schema.clear();
        current.rows.clear();
        current.str_idx.clear();
        if (txn_active && !txn_v2_append("CREATETABLE", tname)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        break;
      }
      case MdbVerb::Use: {
        const std::string tname = trim_copy(pl.paren_inner);
        if (!load_table_from_storage(ports, tname, &current, storage_read_seq, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (current.rows.size() >= 4096) {
          logical_table_refresh_analytics_caches(&current);
        }
        scan_cursor_ordinal = 0;
        mdb_paging_reset_table_caches(&paging);
        if (txn_active && !txn_v2_append("USE", tname)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        break;
      }
      case MdbVerb::DefAttr: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "DEFATTR: no table";
          return res;
        }
        std::vector<std::pair<std::string, std::string>> attrs;
        if (!parse_defattrs(pl.paren_inner, &attrs, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!current.schema.empty()) {
          res.ok = false;
          res.last_error = "DEFATTR: schema already set";
          return res;
        }
        current.schema = std::move(attrs);
        logical_rebuild_str_index(&current);
        logical_persist_mark_schema_change(&current);
        if (txn_active && !txn_v2_append("DEFATTR", std::string_view(pl.paren_inner))) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        break;
      }
      case MdbVerb::AddAttr: {
        if (!mdb_apply_addattr_inner(&current, pl.paren_inner, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        logical_rebuild_str_index(&current);
        logical_persist_mark_schema_change(&current);
        std::string addattr_txn_payload;
        if (!mdb_addattr_paren_inner_to_txn_payload(pl.paren_inner, &addattr_txn_payload, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (txn_active && !txn_v2_append("ADDATTR", std::string_view(addattr_txn_payload))) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        {
          std::ostringstream o;
          o << "[ADDATTR] ok: " << pl.paren_inner;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::Insert: {
        auto cells = split_csv_paren_content(pl.paren_inner);
        if (cells.size() != 1 + current.schema.size()) {
          res.ok = false;
          res.last_error = "INSERT: arity";
          return res;
        }
        const std::string id = cells[0];
        if (current.rows.count(id)) {
          res.ok = false;
          res.last_error = "INSERT: duplicate id";
          return res;
        }
        std::vector<std::string> vals(cells.begin() + 1, cells.end());
        for (std::size_t c = 0; c < current.schema.size(); ++c) {
          if (!type_matches(current.schema[c].second, vals[c])) {
            const std::string msg = type_mismatch_msg(current.schema[c].first, current.schema[c].second, vals[c]);
            log_line(log_sink ? &line_log : nullptr, msg);
            flush_line_log();
            res.ok = false;
            res.last_error = msg;
            return res;
          }
        }
        if (!named_index_check_row_unique(current, id, vals, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        current.rows[id] = vals;
        logical_str_index_add_row(&current, id, current.rows[id]);
        logical_persist_mark_insert(&current, id);
        if (txn_active) {
          std::string pay;
          pay.append(id);
          for (const auto& c : vals) {
            pay.push_back('\t');
            pay.append(c);
          }
          if (!txn_v2_append("INSERT", pay)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        break;
      }
      case MdbVerb::Update: {
        auto cells = split_csv_paren_content(pl.paren_inner);
        if (cells.size() != 1 + current.schema.size()) {
          res.ok = false;
          res.last_error = "UPDATE: arity";
          return res;
        }
        const std::string id = cells[0];
        auto it = current.rows.find(id);
        if (it == current.rows.end()) {
          res.ok = false;
          res.last_error = "UPDATE: missing row";
          return res;
        }
        std::vector<std::string> vals(cells.begin() + 1, cells.end());
        for (std::size_t c = 0; c < current.schema.size(); ++c) {
          if (!type_matches(current.schema[c].second, vals[c])) {
            const std::string msg = type_mismatch_msg(current.schema[c].first, current.schema[c].second, vals[c]);
            log_line(log_sink ? &line_log : nullptr, msg);
            flush_line_log();
            res.ok = false;
            res.last_error = msg;
            return res;
          }
        }
        std::vector<std::string> prev_cells = it->second;
        logical_persist_mark_update(&current, id, prev_cells);
        logical_str_index_remove_row(&current, id, it->second);
        it->second = std::move(vals);
        logical_str_index_add_row(&current, id, it->second);
        if (txn_active) {
          std::string pay;
          pay.append(id);
          for (const auto& c : it->second) {
            pay.push_back('\t');
            pay.append(c);
          }
          if (!txn_v2_append("UPDATE", pay)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        break;
      }
      case MdbVerb::DelAttr: {
        const std::string col = trim_copy(pl.paren_inner);
        int idx = -1;
        for (std::size_t j = 0; j < current.schema.size(); ++j) {
          if (col_name_eq(current.schema[j].first, col)) {
            idx = static_cast<int>(j);
            break;
          }
        }
        if (idx < 0) {
          res.ok = false;
          res.last_error = "DELATTR: unknown column";
          return res;
        }
        current.schema.erase(current.schema.begin() + idx);
        for (auto& kv : current.rows) {
          if (static_cast<int>(kv.second.size()) > idx) kv.second.erase(kv.second.begin() + idx);
        }
        logical_rebuild_str_index(&current);
        {
          std::ostringstream o;
          o << "[DELATTR] ok: key=" << col;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        if (txn_active && !txn_v2_append("DELATTR", col)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::ConfirmReorder: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "CONFIRM_REORDER: not allowed inside BEGIN";
          return res;
        }
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "CONFIRM_REORDER: no table/schema (USE first)";
          return res;
        }
        std::string reorder_line;
        LogicalTable nxt = clone_table(current);
        if (!mdb_confirm_reorder_execute(&nxt, pl.paren_inner, &reorder_line, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!persist_table(MdbEnginePorts::from(engine, client), nxt, idem, fsync_batch, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        current = std::move(nxt);
        client.refresh_read_snapshot();
        mdb_paging_reset_table_caches(&paging);
        log_line(log_sink ? &line_log : nullptr, reorder_line);
        flush_line_log();
        break;
      }
      case MdbVerb::FlushPersist: {
        if (!mdb_flush_coalesced_persist(ports, current, idem, fsync_batch, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (current.rows.size() >= 4096) {
          logical_table_refresh_analytics_caches(&current);
        }
        log_line(log_sink ? &line_log : nullptr, "[FLUSH PERSIST] ok");
        flush_line_log();
        break;
      }
      case MdbVerb::ImportMode: {
        std::string mode = pl.tail;
        trim_inplace(mode);
        for (char& c : mode) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (mode == "on" || mode == "1" || mode == "true") {
          if (bulk_import_session_override) *bulk_import_session_override = true;
          log_line(log_sink ? &line_log : nullptr, "[IMPORT MODE] on");
        } else if (mode == "off" || mode == "0" || mode == "false" || mode.empty()) {
          if (bulk_import_session_override) *bulk_import_session_override = false;
          log_line(log_sink ? &line_log : nullptr, "[IMPORT MODE] off");
        } else {
          res.ok = false;
          res.last_error = "IMPORT MODE: use ON or OFF";
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::RebuildIndex: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "REBUILD INDEX: no table";
          return res;
        }
        if (!pl.paren_inner.empty()) {
          const std::string idx_name = trim_copy(pl.paren_inner);
          structdb::client::CommandBatch b;
          b.idempotency_token = idem + ":rebuild:" + idx_name;
          if (!rebuild_named_index_postings(ports, current, idx_name, &b)) {
            res.ok = false;
            res.last_error = "REBUILD INDEX: unknown index";
            return res;
          }
          if (!submit_persist_command_batch(ports, std::move(b), fsync_batch, &err)) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        } else if (!rebuild_secondary_index_storage(ports, current, idem, fsync_batch, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr, "[REBUILD INDEX] ok");
        flush_line_log();
        break;
      }
      case MdbVerb::SetDurability: {
        if (!session_durability_level) {
          res.ok = false;
          res.last_error = "SET DURABILITY: no session";
          return res;
        }
        const std::string lvl = trim_copy(pl.tail);
        if (lvl.size() != 1 || lvl[0] < '0' || lvl[0] > '2') {
          res.ok = false;
          res.last_error = "SET DURABILITY: level must be 0, 1, or 2";
          return res;
        }
        *session_durability_level = lvl[0] - '0';
        log_line(log_sink ? &line_log : nullptr,
                 std::string("[SET DURABILITY] level=") + lvl + " (see TXN_INNODB_MAP.md)");
        flush_line_log();
        break;
      }
      case MdbVerb::AlterTableAddColumn: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "ALTER TABLE ADD COLUMN: not allowed inside BEGIN";
          return res;
        }
        if (!mdb_apply_addattr_inner(&current, pl.paren_inner, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        logical_rebuild_str_index(&current);
        logical_persist_mark_schema_change(&current);
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr,
                 std::string("[ALTER TABLE] ADD COLUMN ok ") + std::string(pl.paren_inner));
        flush_line_log();
        break;
      }
      case MdbVerb::AlterTableRenameColumn: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "ALTER TABLE RENAME COLUMN: not allowed inside BEGIN";
          return res;
        }
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "ALTER TABLE RENAME COLUMN: no table/schema";
          return res;
        }
        const auto comma_rc = pl.paren_inner.find(',');
        if (comma_rc == std::string_view::npos) {
          res.ok = false;
          res.last_error = "ALTER TABLE RENAME COLUMN: need (old,new)";
          return res;
        }
        const std::string oldn_rc = trim_copy(pl.paren_inner.substr(0, comma_rc));
        const std::string newn_rc = trim_copy(pl.paren_inner.substr(comma_rc + 1));
        if (oldn_rc.empty() || newn_rc.empty()) {
          res.ok = false;
          res.last_error = "ALTER TABLE RENAME COLUMN: empty name";
          return res;
        }
        const int ci_rc = schema_col_index(current, oldn_rc);
        if (ci_rc < 0) {
          res.ok = false;
          res.last_error = "ALTER TABLE RENAME COLUMN: unknown column";
          return res;
        }
        current.schema[static_cast<std::size_t>(ci_rc)].first = newn_rc;
        logical_rebuild_str_index(&current);
        logical_persist_mark_schema_change(&current);
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr,
                 std::string("[ALTER TABLE] RENAME COLUMN ok ") + oldn_rc + " -> " + newn_rc);
        flush_line_log();
        break;
      }
      case MdbVerb::CreateIndex: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "CREATE INDEX: not allowed inside BEGIN";
          return res;
        }
        std::string idx_name, tbl, col;
        bool unique_idx = false;
        if (!parse_create_index_spec(pl.tail, &idx_name, &tbl, &col, &err, &unique_idx)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!col_name_eq(current.name, tbl)) {
          res.ok = false;
          res.last_error = "CREATE INDEX: USE target table first";
          return res;
        }
        if (!create_named_index_storage(ports, current, idx_name, col, unique_idx, idem + ":cidx:" + idx_name,
                                        fsync_batch, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr,
                 std::string(unique_idx ? "[CREATE UNIQUE INDEX] ok " : "[CREATE INDEX] ok ") + idx_name + " ON " +
                     tbl + "(" + col + ")");
        flush_line_log();
        break;
      }
      case MdbVerb::DropIndex: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "DROP INDEX: not allowed inside BEGIN";
          return res;
        }
        std::string idx_name, tbl;
        if (!parse_drop_index_spec(pl.tail, &idx_name, &tbl, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!col_name_eq(current.name, tbl)) {
          res.ok = false;
          res.last_error = "DROP INDEX: USE target table first";
          return res;
        }
        if (!drop_named_index_storage(ports, current, idx_name, idem + ":didx:" + idx_name, fsync_batch, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr,
                 std::string("[DROP INDEX] ok ") + idx_name + " ON " + tbl);
        flush_line_log();
        break;
      }
      case MdbVerb::GroupBy: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "GROUP BY: not allowed inside BEGIN";
          return res;
        }
        if (!mdb_execute_group_by(current, pl.tail, log_sink ? &line_log : nullptr, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        flush_line_log();
        break;
      }
      case MdbVerb::ScanIndex: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "SCAN INDEX: not allowed inside BEGIN";
          return res;
        }
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "SCAN INDEX: no table";
          return res;
        }
        auto scan_args = split_csv_paren_content(pl.paren_inner);
        MdbScanIndexSpec scan_spec;
        if (!parse_scan_index_spec(scan_args, &scan_spec, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        const std::string& idx_name = scan_spec.index_name;
        const std::size_t kMax = scan_spec.max_rows;
        const MdbScanIndexEmit scan_emit = scan_spec.emit;
        std::size_t keys_scanned = 0;
        std::size_t rows_shown = 0;
        std::function<bool(const std::string&)> on_row;
        if (scan_emit == MdbScanIndexEmit::IdsOnly) {
          on_row = [&](const std::string& rid) {
            log_line(log_sink ? &line_log : nullptr, std::string("  id=") + rid);
            return true;
          };
        } else if (scan_emit == MdbScanIndexEmit::FullRow) {
          on_row = [&](const std::string& rid) {
            const auto it = current.rows.find(rid);
            if (it == current.rows.end()) return true;
            std::ostringstream r;
            r << "  id=" << rid;
            for (std::size_t c = 0; c < it->second.size(); ++c) {
              r << " " << current.schema[c].first << "=" << it->second[c];
            }
            log_line(log_sink ? &line_log : nullptr, r.str());
            return true;
          };
        }
        if (!mdb_scan_named_index_rows(ports, current, idx_name, kMax, scan_emit,
                                       scan_emit == MdbScanIndexEmit::StatsOnly ? nullptr : on_row, &keys_scanned,
                                       &rows_shown, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (scan_emit != MdbScanIndexEmit::StatsOnly && rows_shown >= kMax &&
            kMax < std::numeric_limits<std::size_t>::max()) {
          log_line(log_sink ? &line_log : nullptr,
                   "[SCAN INDEX] ... truncated at " + std::to_string(kMax) + " rows");
        }
        {
          std::ostringstream o;
          o << "[SCAN INDEX] index=" << idx_name << " rows_shown=" << rows_shown << " keys_scanned=" << keys_scanned
            << " order=index_key";
          if (scan_emit == MdbScanIndexEmit::StatsOnly) {
            o << " mode=stats";
          } else if (scan_emit == MdbScanIndexEmit::IdsOnly) {
            o << " mode=ids";
          }
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::ShowCheckpoints: {
        std::vector<structdb::storage::CheckpointChainEntry> entries;
        const auto data_dir = std::filesystem::path(engine.config().snapshot().data_dir);
        if (!structdb::storage::checkpoint_chain_read_all(data_dir, &entries, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        for (const auto& ckent : entries) {
          std::ostringstream o;
          o << "[CHECKPOINT] seq=" << ckent.checkpoint_seq << " wal_offset=" << ckent.wal_offset
            << " manifest=" << ckent.manifest_version;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        {
          std::ostringstream o;
          o << "[SHOW CHECKPOINTS] count=" << entries.size();
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::RecoverToCheckpointSeq: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "RECOVER TO CHECKPOINT_SEQ: not allowed inside BEGIN";
          return res;
        }
        std::uint64_t target = 0;
        try {
          target = std::stoull(trim_copy(pl.tail));
        } catch (...) {
          res.ok = false;
          res.last_error = "RECOVER TO CHECKPOINT_SEQ: bad seq";
          return res;
        }
        client.close();
        if (!engine.recover_to_checkpoint_seq(target, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr,
                 std::string("[RECOVER TO CHECKPOINT_SEQ] ok seq=") + std::to_string(target) +
                     " (restart engine/session to continue)");
        flush_line_log();
        res.ok = true;
        return res;
      }
      case MdbVerb::ImportSegment: {
        const std::string tok = trim_copy(pl.paren_inner);
        if (tok.empty()) {
          res.ok = false;
          res.last_error = "IMPORT SEGMENT: empty token";
          return res;
        }
        if (import_segment_token && import_segment_token->has_value() && import_segment_token->value() != tok) {
          if (!persist_now()) {
            res.ok = false;
            res.last_error = err;
            return res;
          }
        }
        if (import_segment_token) *import_segment_token = tok;
        log_line(log_sink ? &line_log : nullptr,
                 std::string("[IMPORT SEGMENT] token=") + tok + " idem=idem:import:" +
                     (current.name.empty() ? "<table>" : current.name) + ":seg:" + tok);
        flush_line_log();
        break;
      }
      case MdbVerb::NotPortable: {
        {
          std::string low = pl.tail;
          for (char& c : low) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
          if (low.rfind("recover to", 0) == 0) {
            log_line(log_sink ? &line_log : nullptr,
                     "[NOT_SUPPORTED] use RECOVER TO CHECKPOINT_SEQ n (not TIME/LSN; see PHASE43.md)");
            flush_line_log();
            break;
          }
          if (low.rfind("walsync", 0) == 0 || low.rfind("groupcommit", 0) == 0) {
            log_line(log_sink ? &line_log : nullptr,
                     "[NOT_SUPPORTED] use SET DURABILITY 0|1|2 and SHOW TUNING (see TXN_INNODB_MAP.md, PHASE41)");
            flush_line_log();
            break;
          }
          if (low.rfind("hotindex", 0) == 0) {
            log_line(log_sink ? &line_log : nullptr,
                     "[NOT_SUPPORTED] use CREATE INDEX name ON table(col) and REBUILD INDEX (PHASE41)");
            flush_line_log();
            break;
          }
        }
        log_line(log_sink ? &line_log : nullptr,
                 std::string("[NOT_SUPPORTED] StructDB KV/LSM has no heap/newdb-txn equivalent for: ") + pl.tail +
                     " (see Docs/PHASE25.md)");
        flush_line_log();
        break;
      }
      case MdbVerb::Exit: {
        log_line(log_sink ? &line_log : nullptr, "[EXIT] bye");
        flush_line_log();
        res.repl_exit_requested = true;
        return res;
      }
      case MdbVerb::ShowLog: {
        append_embed_journal_tail(client, 40, log_sink ? &line_log : nullptr);
        flush_line_log();
        break;
      }
      case MdbVerb::ShowAttr: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "SHOW ATTR: no table/schema (USE first)";
          return res;
        }
        log_line(log_sink ? &line_log : nullptr, std::string("[ATTR] table=") + current.name);
        for (const auto& sc : current.schema) {
          std::ostringstream o;
          o << "  " << sc.first << ":" << sc.second;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::ShowKey: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "SHOW KEY: no table/schema";
          return res;
        }
        {
          std::ostringstream o;
          o << "[KEY] logical_pk_column=" << effective_pk_column_name(current)
            << " (row id is INSERT first cell; see PHASE25)";
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::SetPrimaryKey: {
        if (current.name.empty() || current.schema.empty()) {
          res.ok = false;
          res.last_error = "SET PRIMARY KEY: no table/schema";
          return res;
        }
        if (txn_active) {
          res.ok = false;
          res.last_error = "SET PRIMARY KEY: not allowed inside BEGIN";
          return res;
        }
        const std::string want = trim_copy(pl.paren_inner);
        const int ci = schema_col_index(current, want);
        if (ci < 0) {
          res.ok = false;
          res.last_error = "SET PRIMARY KEY: unknown column";
          return res;
        }
        std::set<std::string> seen;
        for (const auto& kv : current.rows) {
          const std::string& v = kv.second[static_cast<std::size_t>(ci)];
          if (!seen.insert(v).second) {
            res.ok = false;
            res.last_error = "SET PRIMARY KEY: duplicate values in column";
            return res;
          }
        }
        current.pk_column = want;
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr, std::string("[KEY] primary column set to ") + want);
        flush_line_log();
        break;
      }
      case MdbVerb::ShowPlan:
      case MdbVerb::ExplainWhere: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "no table";
          return res;
        }
        auto pr = split_csv_paren_content(pl.paren_inner);
        if (pr.size() != 3) {
          res.ok = false;
          res.last_error = "SHOW PLAN/EXPLAIN WHERE: need (col,op,val)";
          return res;
        }
        const std::size_t matched = count_matching_row_ids(current, pr[0], pr[1], pr[2], &ports, storage_read_seq);
        {
          std::ostringstream o;
          o << "[" << (pl.verb == MdbVerb::ShowPlan ? "SHOW PLAN" : "EXPLAIN WHERE") << "] pred=(" << pr[0] << " "
            << pr[1] << " " << pr[2] << ") matched=" << matched
            << " index_hint=" << explain_where_index_hint(current, pr[0], pr[1], pr[2]);
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::ShowTuning: {
        const auto c = engine.config().snapshot();
        std::ostringstream o;
        o << "[TUNING] data_dir=" << c.data_dir << " memtable_limit_bytes=" << c.memtable_limit_bytes
          << " fsync_every_write=" << (c.fsync_every_write ? "on" : "off")
          << " wal_fsync_min_interval_ms=" << c.wal_fsync_min_interval_ms
          << " compaction_merge_min_interval_ms=" << c.compaction_merge_min_interval_ms
          << " compaction_merge_max_bps=" << c.compaction_merge_max_bytes_per_second
          << " compaction_seq_sst_read=" << (c.compaction_sequential_sst_read ? "on" : "off")
          << " compaction_worker_bg_prio=" << (c.compaction_worker_low_priority_thread ? "on" : "off")
          << " compaction_dedicated_io=" << (c.compaction_dedicated_io_executor ? "on" : "off")
          << " compaction_io_pool_threads=" << c.compaction_io_pool_threads
          << " compaction_parallel_sst_reads=" << (c.compaction_parallel_sst_reads ? "on" : "off")
          << " compaction_io_chunk_bytes=" << c.compaction_io_chunk_bytes
          << " wal_append_max_bps=" << c.wal_append_max_bytes_per_second
          << " wal_sched_bytes_per_slot=" << c.wal_scheduler_bytes_per_depth_slot
          << " storage_pressure_wal_bytes_soft_start=" << c.storage_pressure_wal_bytes_soft_start
          << " wal_auto_trim_prefix_after_flush=" << (c.wal_auto_trim_prefix_after_flush ? "on" : "off")
          << " l0_compact_trigger_threshold=" << c.l0_compact_trigger_threshold
          << " mdb_persist_in_begin=" << (c.mdb_persist_in_begin ? "on" : "off")
          << " mdb_incremental_persist=" << (c.mdb_incremental_persist ? "on" : "off")
          << " mdb_wire_encoding=" << (c.mdb_wire_encoding == structdb::facade::MdbWireEncoding::Wire2 ? "wire2" : "hex")
          << " mdb_persist_coalesce=" << (c.mdb_persist_coalesce ? "on" : "off")
          << " mdb_bulk_import_mode=" << (c.mdb_bulk_import_mode ? "on" : "off")
          << " mdb_script_amortize_bulk_dml=" << (c.mdb_script_amortize_bulk_dml ? "on" : "off")
          << " mdb_persist_chunk_max_puts=" << c.mdb_persist_chunk_max_puts
          << " mdb_persist_chunk_max_frame_bytes=" << c.mdb_persist_chunk_max_frame_bytes
          << " storage_import_store_raw_logical=" << (c.storage_import_store_raw_logical ? "on" : "off")
          << " storage_embed_batch_max_frame_bytes=" << c.storage_embed_batch_max_frame_bytes
          << " memtable_bulk_put_enabled=" << (c.memtable_bulk_put_enabled ? "on" : "off")
          << " storage_batch_undo_lookup=" << (c.storage_batch_undo_lookup ? "on" : "off");
        if (session_durability_level && session_durability_level->has_value()) {
          o << " session_durability_level=" << session_durability_level->value();
        } else {
          o << " session_durability_level=default";
        }
        log_line(log_sink ? &line_log : nullptr, o.str());
        flush_line_log();
        break;
      }
      case MdbVerb::ShowTuningJson: {
        const auto c = engine.config().snapshot();
        structdb::storage::StoragePressureSnapshot p{};
        engine.storage_pressure_snapshot(&p);
        auto esc = [](std::string_view s) {
          std::string o;
          for (unsigned char uc : s) {
            const char ch = static_cast<char>(uc);
            if (ch == '"' || ch == '\\') o.push_back('\\');
            if (ch == '\n' || ch == '\r' || ch == '\t') {
              o.push_back(' ');
              continue;
            }
            o.push_back(ch);
          }
          return o;
        };
        std::ostringstream j;
        j << "{\"kind\":\"structdb_tuning\",\"data_dir\":\"" << esc(c.data_dir) << "\",\"memtable_limit_bytes\":"
          << c.memtable_limit_bytes << ",\"fsync_every_write\":" << (c.fsync_every_write ? "true" : "false")
          << ",\"wal_fsync_min_interval_ms\":" << c.wal_fsync_min_interval_ms
          << ",\"compaction_merge_min_interval_ms\":" << c.compaction_merge_min_interval_ms
          << ",\"compaction_merge_max_bytes_per_second\":" << c.compaction_merge_max_bytes_per_second
          << ",\"compaction_merge_burst_bytes\":" << c.compaction_merge_burst_bytes
          << ",\"compaction_sequential_sst_read\":" << (c.compaction_sequential_sst_read ? "true" : "false")
          << ",\"compaction_worker_low_priority_thread\":" << (c.compaction_worker_low_priority_thread ? "true" : "false")
          << ",\"compaction_dedicated_io_executor\":" << (c.compaction_dedicated_io_executor ? "true" : "false")
          << ",\"compaction_io_pool_threads\":" << c.compaction_io_pool_threads
          << ",\"compaction_parallel_sst_reads\":" << (c.compaction_parallel_sst_reads ? "true" : "false")
          << ",\"compaction_io_chunk_bytes\":" << c.compaction_io_chunk_bytes
          << ",\"wal_append_max_bytes_per_second\":" << c.wal_append_max_bytes_per_second
          << ",\"wal_append_burst_bytes\":" << c.wal_append_burst_bytes
          << ",\"storage_pressure_wal_bytes_soft_start\":" << c.storage_pressure_wal_bytes_soft_start
          << ",\"storage_pressure_wal_bytes_soft_step_bytes\":" << c.storage_pressure_wal_bytes_soft_step_bytes
          << ",\"wal_scheduler_bytes_per_depth_slot\":" << c.wal_scheduler_bytes_per_depth_slot
          << ",\"wal_scheduler_max_slots_per_op\":" << c.wal_scheduler_max_slots_per_op
          << ",\"wal_auto_trim_prefix_after_flush\":" << (c.wal_auto_trim_prefix_after_flush ? "true" : "false")
          << ",\"l0_compact_trigger_threshold\":" << c.l0_compact_trigger_threshold
          << ",\"mdb_persist_in_begin\":" << (c.mdb_persist_in_begin ? "true" : "false")
          << ",\"mdb_incremental_persist\":" << (c.mdb_incremental_persist ? "true" : "false")
          << ",\"mdb_wire_encoding\":\"" << (c.mdb_wire_encoding == structdb::facade::MdbWireEncoding::Wire2 ? "wire2" : "hex")
          << "\",\"mdb_persist_coalesce\":" << (c.mdb_persist_coalesce ? "true" : "false")
          << ",\"mdb_bulk_import_mode\":" << (c.mdb_bulk_import_mode ? "true" : "false")
          << ",\"mdb_persist_chunk_max_puts\":" << c.mdb_persist_chunk_max_puts
          << ",\"mdb_persist_chunk_max_frame_bytes\":" << c.mdb_persist_chunk_max_frame_bytes
          << ",\"storage_import_store_raw_logical\":" << (c.storage_import_store_raw_logical ? "true" : "false")
          << ",\"storage_embed_batch_max_frame_bytes\":" << c.storage_embed_batch_max_frame_bytes
          << ",\"memtable_bulk_put_enabled\":" << (c.memtable_bulk_put_enabled ? "true" : "false")
          << ",\"storage_batch_undo_lookup\":" << (c.storage_batch_undo_lookup ? "true" : "false")
          << ",\"latest_commit_seq\":" << engine.latest_commit_seq() << ",\"storage_l0_files\":" << p.l0_files
          << ",\"wal_bytes\":" << p.wal_bytes << ",\"undo_bytes\":" << p.undo_bytes
          << ",\"manifest_version\":" << p.manifest_version           << ",\"wal_append_record_calls_total\":"
          << p.wal_append_record_calls_total << ",\"wal_fsync_calls_total\":" << p.wal_fsync_calls_total
          << ",\"wal_append_frame_bytes_committed_total\":" << p.wal_append_frame_bytes_committed_total
          << ",\"wal_append_throttle_sleep_ns_total\":" << p.wal_append_throttle_sleep_ns_total
          << ",\"compaction_merge_throttle_sleep_ns_total\":" << p.compaction_merge_throttle_sleep_ns_total
          << ",\"compaction_merge_byte_throttle_sleep_ns_total\":" << p.compaction_merge_byte_throttle_sleep_ns_total
          << ",\"flush_memtable_success_total\":" << p.flush_memtable_success_total
          << ",\"checkpoint_success_total\":" << p.checkpoint_success_total << ",\"compaction_merge_success_total\":"
          << p.compaction_merge_success_total << ",\"compaction_worker_tasks_submitted_total\":"
          << p.compaction_worker_tasks_submitted_total << ",\"compaction_worker_tasks_completed_total\":"
          << p.compaction_worker_tasks_completed_total << "}";
        log_line(log_sink ? &line_log : nullptr, j.str());
        flush_line_log();
        break;
      }
      case MdbVerb::ShowStorage: {
        structdb::storage::StoragePressureSnapshot p{};
        engine.storage_pressure_snapshot(&p);
        std::ostringstream o;
        o << "[STORAGE] l0_files=" << p.l0_files << " wal_bytes=" << p.wal_bytes << " undo_bytes=" << p.undo_bytes
          << " manifest_version=" << p.manifest_version
          << " compaction_queue=" << p.compaction_worker_queue_depth << "/" << p.compaction_worker_queue_cap
          << " pending_deferred_l0=" << (p.pending_deferred_l0_compact ? "yes" : "no")
          << " compaction_merge=" << p.compaction_merge_success_total
          << " worker_tasks=" << p.compaction_worker_tasks_submitted_total << "/" << p.compaction_worker_tasks_completed_total
          << " wal_append=" << p.wal_append_record_calls_total << " wal_fsync=" << p.wal_fsync_calls_total
          << " compact_throttle_sleep_ns=" << p.compaction_merge_throttle_sleep_ns_total
          << " compact_byte_throttle_sleep_ns=" << p.compaction_merge_byte_throttle_sleep_ns_total
          << " flush_ok=" << p.flush_memtable_success_total << " ckpt_ok=" << p.checkpoint_success_total;
        log_line(log_sink ? &line_log : nullptr, o.str());
        flush_line_log();
        break;
      }
      case MdbVerb::ShowStorageJson: {
        structdb::storage::StoragePressureSnapshot p{};
        engine.storage_pressure_snapshot(&p);
        std::ostringstream j;
        j << "{\"kind\":\"structdb_storage\",\"l0_files\":" << p.l0_files << ",\"wal_bytes\":" << p.wal_bytes
          << ",\"undo_bytes\":" << p.undo_bytes << ",\"manifest_version\":" << p.manifest_version
          << ",\"compaction_worker_queue_depth\":" << p.compaction_worker_queue_depth
          << ",\"compaction_worker_queue_cap\":" << p.compaction_worker_queue_cap
          << ",\"pending_deferred_l0_compact\":" << (p.pending_deferred_l0_compact ? "true" : "false")
          << ",\"wal_append_record_calls_total\":" << p.wal_append_record_calls_total
          << ",\"wal_fsync_calls_total\":" << p.wal_fsync_calls_total
          << ",\"flush_memtable_success_total\":" << p.flush_memtable_success_total
          << ",\"checkpoint_success_total\":" << p.checkpoint_success_total << ",\"compaction_merge_success_total\":"
          << p.compaction_merge_success_total << ",\"compaction_merge_throttle_sleep_ns_total\":"
          << p.compaction_merge_throttle_sleep_ns_total << ",\"compaction_merge_byte_throttle_sleep_ns_total\":"
          << p.compaction_merge_byte_throttle_sleep_ns_total << ",\"compaction_worker_tasks_submitted_total\":"
          << p.compaction_worker_tasks_submitted_total << ",\"compaction_worker_tasks_completed_total\":"
          << p.compaction_worker_tasks_completed_total << "}";
        log_line(log_sink ? &line_log : nullptr, j.str());
        flush_line_log();
        break;
      }
      case MdbVerb::Vacuum: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "VACUUM: finish or roll back the active transaction first";
          return res;
        }
        auto* st_eng = engine.storage();
        if (!st_eng) {
          res.ok = false;
          res.last_error = "VACUUM: no storage";
          return res;
        }
        if (!st_eng->flush_memtable(&err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (!engine.drain_l0_compaction_queue(8, &err, 0, 1)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr, "[VACUUM] flush_memtable ok; drain_l0_compaction_queue(max=8) ok");
        flush_line_log();
        break;
      }
      case MdbVerb::Scan: {
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "SCAN: no table";
          return res;
        }
        const std::string scan_tail = trim_copy(pl.tail);
        if (scan_tail == "RESET") {
          scan_cursor_ordinal = 0;
          log_line(log_sink ? &line_log : nullptr, std::string("[SCAN] cursor reset table=") + current.name);
          flush_line_log();
          break;
        }
        auto emit_row = [&](const std::string& rid, const std::vector<std::string>& cells) {
          std::ostringstream r;
          r << "  id=" << rid;
          for (std::size_t c = 0; c < cells.size(); ++c) {
            r << " " << current.schema[c].first << "=" << cells[c];
          }
          log_line(log_sink ? &line_log : nullptr, r.str());
        };
        if (scan_tail == "MORE") {
          std::size_t batch = 500;
          if (pl.has_paren) {
            const std::string inner = trim_copy(pl.paren_inner);
            if (!inner.empty()) {
              try {
                const unsigned long v = std::stoul(inner);
                if (v == 0 || v > 5000) {
                  res.ok = false;
                  res.last_error = "SCAN MORE: batch must be 1..5000";
                  return res;
                }
                batch = static_cast<std::size_t>(v);
              } catch (...) {
                res.ok = false;
                res.last_error = "SCAN MORE: bad batch number";
                return res;
              }
            }
          }
          std::size_t n = 0;
          std::size_t skipped = 0;
          for (const auto& kv : current.rows) {
            if (skipped < scan_cursor_ordinal) {
              ++skipped;
              continue;
            }
            if (n >= batch) break;
            emit_row(kv.first, kv.second);
            ++n;
          }
          scan_cursor_ordinal += n;
          {
            std::ostringstream o;
            o << "[SCAN] table=" << current.name << " mode=MORE rows_shown=" << n << " cursor=" << scan_cursor_ordinal
              << " total_rows=" << current.rows.size();
            log_line(log_sink ? &line_log : nullptr, o.str());
          }
          flush_line_log();
          break;
        }
        scan_cursor_ordinal = 0;
        constexpr std::size_t kMax = 5000;
        std::size_t n = 0;
        for (const auto& kv : current.rows) {
          if (n >= kMax) {
            log_line(log_sink ? &line_log : nullptr, "[SCAN] ... truncated at 5000 rows (PHASE25 cap)");
            break;
          }
          emit_row(kv.first, kv.second);
          ++n;
        }
        scan_cursor_ordinal = n;
        {
          std::ostringstream o;
          o << "[SCAN] table=" << current.name << " rows_shown=" << n << " cursor=" << scan_cursor_ordinal;
          log_line(log_sink ? &line_log : nullptr, o.str());
        }
        flush_line_log();
        break;
      }
      case MdbVerb::ImportDir: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "IMPORTDIR: not allowed inside BEGIN";
          return res;
        }
        if (!import_mdb_directory(ports, pl.paren_inner, log_sink ? &line_log : nullptr, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr, "[IMPORTDIR] done");
        flush_line_log();
        break;
      }
      case MdbVerb::DropTable: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "DROP TABLE: not allowed inside BEGIN";
          return res;
        }
        const std::string tn = trim_copy(pl.paren_inner);
        if (tn.empty()) {
          res.ok = false;
          res.last_error = "DROP TABLE: empty name";
          return res;
        }
        if (!table_exists_in_storage(ports, tn, storage_read_seq)) {
          res.ok = false;
          res.last_error = "DROP TABLE: not found";
          return res;
        }
        structdb::client::CommandBatch b;
        b.idempotency_token = idem + ":drop:" + tn;
        gather_drop_table_keys(ports, tn, storage_read_seq, &b);
        if (!client.submit(b, fsync_batch, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        if (current.name == tn) {
          current.clear_data_keep_name();
          current.name.clear();
        }
        client.refresh_read_snapshot();
        log_line(log_sink ? &line_log : nullptr, std::string("[DROP] table=") + tn);
        flush_line_log();
        break;
      }
      case MdbVerb::RenameTable: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "RENAME TABLE: not allowed inside BEGIN";
          return res;
        }
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "RENAME TABLE: no current table";
          return res;
        }
        const std::string newn = trim_copy(pl.paren_inner);
        if (newn.empty()) {
          res.ok = false;
          res.last_error = "RENAME TABLE: empty new name";
          return res;
        }
        const std::string oldn = current.name;
        if (!rename_table_storage(ports, oldn, newn, idem + ":rn_put:" + newn, idem + ":rn_del:" + oldn,
                                  fsync_batch, &err)) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        current.name = newn;
        client.refresh_read_snapshot();
        log_line(log_sink ? &line_log : nullptr, std::string("[RENAME] ") + oldn + " -> " + newn);
        flush_line_log();
        break;
      }
      case MdbVerb::ResetTable: {
        if (txn_active) {
          res.ok = false;
          res.last_error = "RESET: not allowed inside BEGIN";
          return res;
        }
        if (current.name.empty()) {
          res.ok = false;
          res.last_error = "RESET: no current table";
          return res;
        }
        current.schema.clear();
        current.rows.clear();
        current.str_idx.clear();
        current.pk_column.clear();
        if (!persist_now()) {
          res.ok = false;
          res.last_error = err;
          return res;
        }
        log_line(log_sink ? &line_log : nullptr, std::string("[RESET] table=") + current.name);
        flush_line_log();
        break;
      }
      case MdbVerb::ReleaseSavepoint: {
        if (!txn_active) {
          res.ok = false;
          res.last_error = "RELEASE SAVEPOINT: no active txn";
          return res;
        }
        auto it = savepoints.find(pl.tail);
        if (it == savepoints.end()) {
          res.ok = false;
          res.last_error = "RELEASE SAVEPOINT: unknown name";
          return res;
        }
        savepoints.erase(it);
        log_line(log_sink ? &line_log : nullptr, std::string("[RELEASE SAVEPOINT] ") + pl.tail);
        flush_line_log();
        break;
      }
      default: {
        log_line(log_sink ? &line_log : nullptr, "[ERR] unknown command. Type HELP.");
        flush_line_log();
        res.ok = false;
        res.last_error = "unknown command";
        return res;
      }
    }
