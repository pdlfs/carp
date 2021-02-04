//
// Created by Ankush J on 2/3/21.
//

#include "carp/manifest.h"

#include "common.h"

namespace pdlfs {
namespace plfsio {
int PartitionManifest::GetOverLappingEntries(float point,
                                             PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].Overlaps(point)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  assert(sizes_set_);
  match.key_sz = key_sz_;
  match.val_sz = val_sz_;

  return 0;
}

int PartitionManifest::GetOverLappingEntries(int epoch, float range_begin,
                                             float range_end,
                                             PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].epoch == epoch &&
        items_[i].Overlaps(range_begin, range_end)) {
      match.items.push_back(items_[i]);
      match.mass_total += items_[i].part_item_count;
      match.mass_oob += items_[i].part_item_oob;
    }
  }

  logf(LOG_INFO, "Query Selectivity: %.4f %%\n",
       match.mass_total * 1.0 / mass_total_);

  assert(sizes_set_);
  match.key_sz = key_sz_;
  match.val_sz = val_sz_;

  return 0;
}
}  // namespace plfsio
}  // namespace pdlfs
