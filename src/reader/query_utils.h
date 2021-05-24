//
// Created by Ankush on 5/21/2021.
//

#pragma once

#include "carp/manifest.h"

#define PCT(x) ((x) / 100.0f)

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
        manifest.GetOverlappingEntries(ep, qpnt, match);

        char print_buf[64];
        snprintf(print_buf, 64, "%.3f (%.3f%%), ", qpnt,
                 match.GetSelectivity() * 100);

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

  static Status GenQueryPlan(PartitionManifest& manifest,
                             std::vector<Query>& queries) {
    Status s = Status::OK();

    int epcnt;
    s = manifest.GetEpochCount(epcnt);
    if (!s.ok()) return s;

    std::vector<float> overlaps;

    for (int e = 0; e < epcnt; e++) {
      uint64_t epmass;
      s = manifest.GetEpochMass(e, epmass);
      if (epmass < 1000ull) continue;

      s = GenQueries(manifest, e, queries, overlaps, /* olapmax */ PCT(2),
                     /* dist */ PCT(0.2),
                     /* count */ 3);
      if (!s.ok()) return s;
    }

    return s;
  }

 private:
  static Status GenQueries(PartitionManifest& manifest, int epoch,
                           std::vector<Query>& queries,
                           std::vector<float>& overlaps, float max_overlap,
                           float min_dist, int count_within_dist) {
    Status s = Status::OK();
    Range r;
    s = manifest.GetEpochRange(epoch, r);
    if (!s.ok()) return s;

    float rbeg = r.range_min;
    float rend = rbeg + 0.001f;

    while (rend < r.range_max) {
      Query q(epoch, rbeg, rend);
      PartitionManifestMatch match;

      manifest.GetOverlappingEntries(q, match);

      float query_sel = match.GetSelectivity();

      int num_in_range = ItemsWithinRange(overlaps, query_sel, min_dist);
      bool query_useful = query_sel < max_overlap;
      bool query_needed = num_in_range < count_within_dist;

      if (query_needed and query_useful) {
        queries.push_back(q);
        overlaps.push_back(query_sel);
      }

      rend += 0.1;

      if (query_needed or (not query_useful)) {
        /* if current query range is not useful, it's pointless to keep
         * extending it so we move on */
        rbeg = rend + 0.1;
        rend = rbeg + 0.001;
      }
    }

    return s;
  }

  static float ClosestDistance(std::vector<float>& v, float f) {
    float dist_min = FLT_MAX;

    for (size_t i = 0; i < v.size(); i++) {
      float dist_cur = std::fabs(v[i] - f);
      dist_min = std::min(dist_min, dist_cur);
    }

    return dist_min;
  }

  static int ItemsWithinRange(std::vector<float>& v, float f, float rmax) {
    int count = 0;

    for (size_t i = 0; i < v.size(); i++) {
      if (std::fabs(v[i] - f) < rmax) {
        count++;
      }
    }

    return count;
  }
};
}  // namespace plfsio
}  // namespace pdlfs
