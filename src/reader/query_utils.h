//
// Created by Ankush on 5/21/2021.
//

#pragma once

#include "carp/manifest.h"

namespace pdlfs {
namespace plfsio {
class QueryUtils {
 public:
  static Status SummarizeManifest(PartitionManifest& manifest) {
    Status s = Status::OK();

    manifest.SortByKey();

    int epcnt;
    s = manifest.GetEpochCount(epcnt);
    if (!s.ok()) return s;

    logf(LOG_INFO, "Total Epochs: %d\n", epcnt);

    for (int ep = 0; ep < epcnt; ep++) {
      Range ep_range;
      s = manifest.GetEpochRange(ep, ep_range);
      if (!s.ok()) return s;

      uint64_t ep_itemcnt;
      s = manifest.GetEpochMass(ep, ep_itemcnt);
      if (!s.ok()) return s;

      if (ep_itemcnt == 0u) continue;

      logf(LOG_INFO, "Epoch %d: %.1f to %.1f (%" PRIu64 " items)", ep,
           ep_range.range_min, ep_range.range_max, ep_itemcnt);

      std::string print_buf_concat;
      for (float qpnt = 0.01; qpnt < 2; qpnt += 0.25) {
        PartitionManifestMatch match;
        manifest.GetOverLappingEntries(ep, qpnt, match);

        char print_buf[64];
        snprintf(print_buf, 64, "%.3f (%.3%%), ", qpnt, match.TotalMass(),
                 match.TotalMass() * 100.0 / ep_itemcnt);

        print_buf_concat += print_buf;
        if (print_buf_concat.size() > 60u) {
          logf(LOG_INFO, "%s", print_buf_concat.c_str());
          print_buf_concat = "";
        }
      }

      if (print_buf_concat.size()) {
        logf(LOG_INFO, "%s", print_buf_concat.c_str());
        print_buf_concat = "";
      }
    }

    return s;
  }

  static void QueryPlan(PartitionManifest& manifest) { return; }
};
}  // namespace plfsio
}  // namespace pdlfs