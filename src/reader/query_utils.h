//
// Created by Ankush on 5/21/2021.
//

#pragma once

#include "carp/manifest.h"
#include "range_reader.h"

#include <carp/coding_float.h>

#define PCT(x) ((x) / 100.0f)

namespace pdlfs {
namespace plfsio {
class QueryUtils {
 public:
  static Status SummarizeManifest(PartitionManifest& manifest);

  static Status GenQueryPlan(PartitionManifest& manifest,
                             std::vector<Query>& queries);

  template <typename T>
  static void SSTReadWorker(void* arg);

  template <typename T>
  static void RankwiseSSTReadWorker(void* arg);

  template <typename T>
  static void ThreadSafetyWarning();

 private:
  static Status GenQueries(PartitionManifest& manifest, int epoch,
                           std::vector<Query>& queries,
                           std::vector<float>& overlaps, float max_overlap,
                           float min_dist, int count_within_dist);

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
