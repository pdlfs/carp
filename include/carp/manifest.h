//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include <stdint.h>
#include <vector>

namespace pdlfs {
namespace plfsio {
typedef struct PartitionManifestItem {
  int epoch;
  int rank;
  uint64_t offset;
  float part_range_begin;
  float part_range_end;
  uint32_t part_item_count;
  uint32_t part_item_oob;

  bool Overlaps(float point) const {
    return point >= part_range_begin and point <= part_range_end;
  }
  bool Overlaps(float range_begin, float range_end) const {
    return Overlaps(range_begin) or Overlaps(range_end) or
           ((range_begin < part_range_begin) and (range_end > part_range_end));
  }
} PartitionManifestItem;

typedef struct PartitionManifestMatch {
  std::vector<PartitionManifestItem> items;
  uint64_t mass_total = 0;
  uint64_t mass_oob = 0;
} PartitionManifestMatch;
}  // namespace plfsio
}  // namespace pdlfs