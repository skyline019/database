#include "cli/modules/sidecar/common/index_catalog.h"

#include "cli/modules/where/executor/stats/table_stats.h"

#include <chrono>
#include <cctype>
#include <cstddef>
#include <iomanip>
#include <sstream>
#include "cli/modules/sidecar/covering/covering_index_sidecar.h"
#include "cli/modules/sidecar/eq/equality_index_sidecar.h"
#include "cli/modules/sidecar/page/page_index_sidecar.h"
#include "cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.h"

#include <atomic>

namespace {
std::atomic<std::uint64_t> g_sidecar_invalidate_requests{0};
}  // namespace

std::uint64_t index_catalog_sidecar_invalidate_request_count() {
    return g_sidecar_invalidate_requests.load(std::memory_order_relaxed);
}

std::uint64_t index_catalog_built_at_wall_ms() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

namespace {

bool extract_semicolon_token_value(const std::string& hdr, const char* key, std::string& raw_out) {
    const std::string pref = std::string(";") + key + "=";
    const auto p = hdr.find(pref);
    if (p == std::string::npos) {
        return false;
    }
    std::size_t i = p + pref.size();
    raw_out.clear();
    while (i < hdr.size() && hdr[i] != ';') {
        raw_out.push_back(hdr[i]);
        ++i;
    }
    return true;
}

bool map_idx_kind_string(const std::string& raw, IndexKind* out_kind) {
    if (out_kind == nullptr) {
        return false;
    }
    if (raw == "eq") {
        *out_kind = IndexKind::Eq;
        return true;
    }
    if (raw == "range") {
        *out_kind = IndexKind::Range;
        return true;
    }
    if (raw == "covering") {
        *out_kind = IndexKind::Covering;
        return true;
    }
    if (raw == "page") {
        *out_kind = IndexKind::PageOrder;
        return true;
    }
    if (raw == "visibility") {
        *out_kind = IndexKind::Visibility;
        return true;
    }
    return false;
}

std::uint64_t parse_u64_token(const std::string& hdr, const char* key) {
    const std::string tok = std::string(";") + key + "=";
    const auto pos = hdr.find(tok);
    if (pos == std::string::npos) {
        return 0;
    }
    std::uint64_t v = 0;
    for (std::size_t i = pos + tok.size(); i < hdr.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(hdr[i]);
        if (c < '0' || c > '9') {
            break;
        }
        v = v * 10ULL + static_cast<std::uint64_t>(c - '0');
    }
    return v;
}

}  // namespace

std::string index_catalog_infer_table_plain_from_data_file(const std::string& data_file) {
    static constexpr char kSuf[] = ".bin";
    constexpr std::size_t kSufLen = sizeof(kSuf) - 1;
    if (data_file.size() > kSufLen &&
        data_file.compare(data_file.size() - kSufLen, kSufLen, kSuf) == 0) {
        return data_file.substr(0, data_file.size() - kSufLen);
    }
    return {};
}

std::string index_catalog_pct_encode(const std::string& s) {
    std::ostringstream os;
    os << std::uppercase << std::hex;
    for (const unsigned char uc : s) {
        if (std::isalnum(uc) != 0 || uc == '_' || uc == '-' || uc == '.') {
            os << static_cast<char>(uc);
        } else {
            os << '%' << std::setw(2) << std::setfill('0') << static_cast<int>(uc);
        }
    }
    return os.str();
}

std::string index_catalog_pct_decode(const std::string& enc) {
    std::string out;
    out.reserve(enc.size());
    for (std::size_t i = 0; i < enc.size(); ++i) {
        if (enc[i] != '%' || i + 2 >= enc.size()) {
            out.push_back(enc[i]);
            continue;
        }
        int hi = std::tolower(static_cast<unsigned char>(enc[i + 1]));
        int lo = std::tolower(static_cast<unsigned char>(enc[i + 2]));
        auto hexv = [](int c) -> int {
            if (c >= '0' && c <= '9') {
                return c - '0';
            }
            if (c >= 'a' && c <= 'f') {
                return 10 + (c - 'a');
            }
            return -1;
        };
        const int vh = hexv(hi);
        const int vl = hexv(lo);
        if (vh < 0 || vl < 0) {
            out.push_back(enc[i]);
            continue;
        }
        out.push_back(static_cast<char>((vh << 4) | vl));
        i += 2;
    }
    return out;
}

std::uint64_t index_catalog_fnv1a64(const std::string& s) {
    constexpr std::uint64_t kOffset = 1469598103934665603ULL;
    constexpr std::uint64_t kPrime = 1099511628211ULL;
    std::uint64_t h = kOffset;
    for (const unsigned char uc : s) {
        h ^= static_cast<std::uint64_t>(uc);
        h *= kPrime;
    }
    return h;
}

void index_catalog_parse_header_tail(const std::string& hdr, IndexCatalogParsedTail& out) {
    out.idx_sv = parse_u64_token(hdr, "idx_sv");
    out.idx_dl = parse_u64_token(hdr, "idx_dl");
    out.tbl_fnv = parse_u64_token(hdr, "tbl_fnv");
    out.inx_fnv = parse_u64_token(hdr, "inx_fnv");
    out.built_ms = parse_u64_token(hdr, "built_ms");
    const std::uint64_t bld = parse_u64_token(hdr, "bld");
    out.catalog_build_state = static_cast<std::uint8_t>(std::min<std::uint64_t>(bld, 255ULL));
}

void index_catalog_parse_plaintext_names(const std::string& hdr, IndexCatalogPlaintextNames& out) {
    out = IndexCatalogPlaintextNames{};
    std::string t_raw;
    std::string i_raw;
    const bool has_t = extract_semicolon_token_value(hdr, "tbl_n", t_raw);
    const bool has_i = extract_semicolon_token_value(hdr, "inx_n", i_raw);
    if (has_t != has_i) {
        out.malformed_plaintext = true;
        return;
    }
    if (!has_t) {
        return;
    }
    out.table_name = index_catalog_pct_decode(t_raw);
    out.index_name = index_catalog_pct_decode(i_raw);
    out.has_plaintext = true;
}

bool index_catalog_plaintext_matches(const IndexCatalogPlaintextNames& parsed,
                                     const std::string& expected_table_plain,
                                     const std::string& expected_index_plain) {
    if (parsed.malformed_plaintext) {
        return false;
    }
    if (!parsed.has_plaintext) {
        return true;
    }
    return parsed.table_name == expected_table_plain && parsed.index_name == expected_index_plain;
}

bool index_catalog_tail_identity_matches(const IndexCatalogParsedTail& tail,
                                         const std::string& table_identity_path,
                                         const std::string& index_identity_key) {
    if (tail.tbl_fnv != 0 && tail.tbl_fnv != index_catalog_fnv1a64(table_identity_path)) {
        return false;
    }
    if (tail.inx_fnv != 0 && tail.inx_fnv != index_catalog_fnv1a64(index_identity_key)) {
        return false;
    }
    return true;
}

std::string index_catalog_sidecar_meta_suffix(const IndexKind kind,
                                              const std::uint64_t idx_schema_sig,
                                              const std::uint64_t idx_data_lsn,
                                              const std::string& table_identity_path,
                                              const std::string& index_identity_key,
                                              IndexCatalogParsedTail* out_written,
                                              const std::string& table_plain_name,
                                              const std::string& index_plain_name) {
    const char* tag = "eq";
    switch (kind) {
    case IndexKind::Eq:
        tag = "eq";
        break;
    case IndexKind::Range:
        tag = "range";
        break;
    case IndexKind::Covering:
        tag = "covering";
        break;
    case IndexKind::PageOrder:
        tag = "page";
        break;
    case IndexKind::Visibility:
        tag = "visibility";
        break;
    }
    const std::uint64_t built_ms = index_catalog_built_at_wall_ms();
    const std::uint64_t tf = index_catalog_fnv1a64(table_identity_path);
    const std::uint64_t ix = index_catalog_fnv1a64(index_identity_key);
    if (out_written != nullptr) {
        out_written->idx_sv = idx_schema_sig;
        out_written->idx_dl = idx_data_lsn;
        out_written->tbl_fnv = tf;
        out_written->inx_fnv = ix;
        out_written->built_ms = built_ms;
        out_written->catalog_build_state = 2;
    }
    std::ostringstream os;
    os << ";idx_kind=" << tag << ";built_ms=" << built_ms << ";idx_sv=" << idx_schema_sig << ";idx_dl=" << idx_data_lsn
       << ";tbl_fnv=" << tf << ";inx_fnv=" << ix << ";bld=2";
    if (!table_plain_name.empty() && !index_plain_name.empty()) {
        os << ";tbl_n=" << index_catalog_pct_encode(table_plain_name) << ";inx_n=" << index_catalog_pct_encode(index_plain_name);
    }
    return os.str();
}

bool index_catalog_header_identity_ok(const std::string& hdr,
                                      const IndexCatalogParsedTail& tail,
                                      const std::string& table_identity_path,
                                      const std::string& index_identity_key,
                                      const std::string& expected_table_plain,
                                      const std::string& expected_index_plain,
                                      const IndexKind expected_kind) {
    if (!index_catalog_tail_identity_matches(tail, table_identity_path, index_identity_key)) {
        return false;
    }
    IndexCatalogPlaintextNames plain{};
    index_catalog_parse_plaintext_names(hdr, plain);
    if (!index_catalog_plaintext_matches(plain, expected_table_plain, expected_index_plain)) {
        return false;
    }
    std::string kind_raw;
    if (extract_semicolon_token_value(hdr, "idx_kind", kind_raw)) {
        IndexKind hk = expected_kind;
        if (!map_idx_kind_string(kind_raw, &hk) || hk != expected_kind) {
            return false;
        }
    }
    return true;
}

bool index_descriptor_matches_runtime(const IndexDescriptor& d,
                                      const std::uint64_t table_schema_version,
                                      const std::uint64_t table_data_lsn) {
    if (!d.valid) {
        return false;
    }
    if (d.schema_version != 0 && d.schema_version != table_schema_version) {
        return false;
    }
    if (d.data_lsn != 0 && table_data_lsn != 0 && d.data_lsn > table_data_lsn) {
        return false;
    }
    return true;
}

std::string explain_index_descriptor_mismatch(const IndexDescriptor& d,
                                               const std::uint64_t table_schema_version,
                                               const std::uint64_t table_data_lsn) {
    if (!d.valid) {
        return "descriptor_invalid";
    }
    if (d.schema_version != 0 && d.schema_version != table_schema_version) {
        return "schema_version_mismatch(descriptor=" + std::to_string(d.schema_version) +
               ",table=" + std::to_string(table_schema_version) + ")";
    }
    if (d.data_lsn != 0 && table_data_lsn != 0 && d.data_lsn > table_data_lsn) {
        return "data_lsn_ahead(descriptor=" + std::to_string(d.data_lsn) + ",table=" +
               std::to_string(table_data_lsn) + ")";
    }
    return {};
}

void sidecar_invalidate_all_indexes_for_data_file(const std::string& eff_data) {
    if (eff_data.empty()) {
        return;
    }
    g_sidecar_invalidate_requests.fetch_add(1, std::memory_order_relaxed);
    invalidate_table_stats_for_data_file(eff_data);
    invalidate_eq_index_sidecars_for_data_file(eff_data);
    invalidate_page_index_sidecars_for_data_file(eff_data);
    invalidate_covering_sidecars_for_data_file(eff_data);
    invalidate_visibility_checkpoint_sidecars_for_data_file(eff_data);
}

void sidecar_invalidate_all_indexes_for_data_file(const std::string& eff_data,
                                                 const std::set<std::string>& attrs) {
    if (eff_data.empty()) {
        return;
    }
    if (attrs.empty()) {
        sidecar_invalidate_all_indexes_for_data_file(eff_data);
        return;
    }
    g_sidecar_invalidate_requests.fetch_add(1, std::memory_order_relaxed);
    invalidate_table_stats_for_data_file(eff_data);
    invalidate_eq_index_sidecars_for_attrs(eff_data, attrs);
    invalidate_page_index_sidecars_for_order_attrs(eff_data, attrs);
    invalidate_covering_sidecars_for_attrs(eff_data, attrs);
    invalidate_visibility_checkpoint_sidecars_for_attrs(eff_data, attrs);
}
