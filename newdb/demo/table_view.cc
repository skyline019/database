#include "table_view.h"

#include <newdb/row.h>

#include "logging.h"

#include <algorithm>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

namespace table_view {

namespace {

bool row_at_slot(const newdb::HeapTable& tbl, const std::size_t slot, newdb::Row& out) {
    if (tbl.is_heap_storage_backed()) {
        return tbl.decode_heap_slot(slot, out);
    }
    out = tbl.rows[slot];
    return true;
}

} // namespace

void print_page_indexed(const newdb::TableSchema& schema,
                         const newdb::HeapTable& tbl,
                         const std::vector<std::size_t>& row_indices,
                         const std::size_t page_no,
                         const std::size_t page_size) {
    if (page_size == 0) {
        logging_console_printf("[PAGE] page_size must > 0\n");
        return;
    }
    const std::size_t total = row_indices.size();
    const std::size_t total_pages = (total + page_size - 1) / page_size;
    if (page_no == 0 || page_no > total_pages) {
        logging_console_printf("[PAGE] invalid page_no=%zu, total_pages=%zu\n", page_no, total_pages);
        return;
    }
    const std::size_t begin = (page_no - 1) * page_size;
    const std::size_t end = std::min(begin + page_size, total);
    std::vector<std::string> attr_names;
    if (!schema.attrs.empty()) {
        for (const auto& m : schema.attrs) attr_names.push_back(m.name);
    } else {
        std::set<std::string> all_keys;
        newdb::Row probe;
        for (const std::size_t idx : row_indices) {
            if (!row_at_slot(tbl, idx, probe)) {
                continue;
            }
            for (const auto& kv : probe.attrs) {
                all_keys.insert(kv.first);
            }
        }
        attr_names.assign(all_keys.begin(), all_keys.end());
    }
    const std::size_t col_count = 2 + attr_names.size();
    std::vector<std::size_t> widths(col_count, 0);
    auto update_width = [&](const std::size_t idx, const std::string& s) {
        if (s.size() > widths[idx]) widths[idx] = s.size();
    };
    update_width(0, "#");
    update_width(1, "id");
    for (std::size_t i = 0; i < attr_names.size(); ++i) update_width(2 + i, attr_names[i]);
    newdb::Row r_probe;
    for (std::size_t i = begin; i < end; ++i) {
        if (!row_at_slot(tbl, row_indices[i], r_probe)) {
            continue;
        }
        update_width(0, std::to_string(i));
        update_width(1, std::to_string(r_probe.id));
        for (std::size_t j = 0; j < attr_names.size(); ++j) {
            const auto it = r_probe.attrs.find(attr_names[j]);
            if (it != r_probe.attrs.end()) {
                update_width(2 + j, it->second);
            }
        }
    }
    constexpr std::size_t kMaxColWidth = 32;
    for (auto& w : widths) {
        if (w > kMaxColWidth) w = kMaxColWidth;
        if (w == 0) w = 1;
    }
    auto print_border = [&](const char* left, const char* mid, const char* right) {
        logging_console_printf("%s", left);
        for (std::size_t c = 0; c < col_count; ++c) {
            const std::size_t w = widths[c] + 2;
            for (std::size_t k = 0; k < w; ++k) logging_console_printf("─");
            if (c + 1 < col_count) logging_console_printf("%s", mid);
        }
        logging_console_printf("%s\n", right);
    };
    auto print_cell_row = [&](const std::vector<std::string>& cols) {
        logging_console_printf("│");
        for (std::size_t c = 0; c < col_count; ++c) {
            std::string v = (c < cols.size() ? cols[c] : "");
            if (v.size() > widths[c]) v.resize(widths[c]);
            logging_console_printf(" %-*s │", static_cast<int>(widths[c]), v.c_str());
        }
        logging_console_printf("\n");
    };
    logging_console_printf("Page %zu / %zu  Rows %zu..%zu of %zu\n",
                page_no, total_pages, begin, end ? end - 1 : 0, total);
    print_border("┌", "┬", "┐");
    {
        std::vector<std::string> header;
        header.push_back("#");
        header.push_back("id");
        for (const auto& name : attr_names) header.push_back(name);
        print_cell_row(header);
    }
    print_border("├", "┼", "┤");
    for (std::size_t i = begin; i < end; ++i) {
        if (!row_at_slot(tbl, row_indices[i], r_probe)) {
            continue;
        }
        std::vector<std::string> cols;
        cols.push_back(std::to_string(i));
        cols.push_back(std::to_string(r_probe.id));
        for (const auto& name : attr_names) {
            const auto it = r_probe.attrs.find(name);
            cols.push_back(it != r_probe.attrs.end() ? it->second : "");
        }
        print_cell_row(cols);
        if (i + 1 < end) print_border("├", "┼", "┤");
    }
    print_border("└", "┴", "┘");
}

} // namespace table_view
