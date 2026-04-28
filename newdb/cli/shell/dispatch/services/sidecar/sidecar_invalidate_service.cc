#include <waterfall/config.h>

#include <set>
#include <string>

#include "cli/modules/sidecar/covering/covering_index_sidecar.h"
#include "cli/shell/dispatch/internal/dispatch_internal.h"
#include "cli/modules/sidecar/eq/equality_index_sidecar.h"
#include "cli/modules/sidecar/page/page_index_sidecar.h"
#include "cli/modules/sidecar/visibility/visibility_checkpoint_sidecar.h"

void invalidate_eq_sidecars_after_write(const std::string& eff_data) {
    if (eff_data.empty()) {
        return;
    }
    invalidate_eq_index_sidecars_for_data_file(eff_data);
    invalidate_page_index_sidecars_for_data_file(eff_data);
    invalidate_covering_sidecars_for_data_file(eff_data);
    invalidate_visibility_checkpoint_sidecars_for_data_file(eff_data);
}

void invalidate_eq_sidecars_after_write(const std::string& eff_data,
                                        const std::set<std::string>& attrs) {
    if (eff_data.empty()) {
        return;
    }
    if (attrs.empty()) {
        invalidate_eq_index_sidecars_for_data_file(eff_data);
        invalidate_page_index_sidecars_for_data_file(eff_data);
        invalidate_covering_sidecars_for_data_file(eff_data);
        invalidate_visibility_checkpoint_sidecars_for_data_file(eff_data);
        return;
    }
    invalidate_eq_index_sidecars_for_attrs(eff_data, attrs);
    invalidate_page_index_sidecars_for_order_attrs(eff_data, attrs);
    invalidate_covering_sidecars_for_attrs(eff_data, attrs);
    invalidate_visibility_checkpoint_sidecars_for_attrs(eff_data, attrs);
}




