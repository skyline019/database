#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>

namespace newdb::where_cost {

/// Unified heuristic weights for lightweight WHERE planning (not full optimizer).
inline double heap_full_scan_cost(std::size_t logical_rows) {
    return static_cast<double>(logical_rows <= 1 ? 1 : logical_rows);
}

inline double pk_point_lookup_cost() { return 1.0; }

inline double eq_sidecar_probe_cost(std::size_t logical_rows, double selectivity01) {
    const double s = std::clamp(selectivity01, 1e-9, 1.0);
    return std::max(4.0, static_cast<double>(logical_rows) * s);
}

inline double visibility_filter_multiplier(std::size_t visible_fraction_of_heap) {
    if (visible_fraction_of_heap == 0) return 1.0;
    return std::max(0.5, static_cast<double>(visible_fraction_of_heap));
}

} // namespace newdb::where_cost
