#include <waterfall/config.h>

#include <set>
#include <string>

#include "cli/modules/sidecar/common/index_catalog.h"

void invalidate_eq_sidecars_after_write(const std::string& eff_data) {
    sidecar_invalidate_all_indexes_for_data_file(eff_data);
}

void invalidate_eq_sidecars_after_write(const std::string& eff_data,
                                        const std::set<std::string>& attrs) {
    sidecar_invalidate_all_indexes_for_data_file(eff_data, attrs);
}
