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
      match.AddItem(items_[i]);
    }
  }

  assert(sizes_set_);
  match.SetKVSizes(key_sz_, val_sz_);

  return 0;
}

int PartitionManifest::GetOverLappingEntries(int epoch, float range_begin,
                                             float range_end,
                                             PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].epoch == epoch &&
        items_[i].Overlaps(range_begin, range_end)) {
      match.AddItem(items_[i]);
    }
  }

  uint64_t mass_epoch = mass_epoch_[epoch];
  uint64_t mass_match = match.TotalMass();

  logf(LOG_INFO, "Query Selectivity: %.4f %% (%lu items, %lu total)\n",
      mass_match * 100.0 / mass_epoch, mass_match, mass_epoch);

  assert(sizes_set_);
  match.SetKVSizes(key_sz_, val_sz_);

  return 0;
}
}  // namespace plfsio
}  // namespace pdlfs
