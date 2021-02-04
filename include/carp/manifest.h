//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/status.h"

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
  uint64_t key_sz;
  uint64_t val_sz;
} PartitionManifestMatch;

// Public Query interface is thread-safe, write interface is not
class PartitionManifest {
 public:
  PartitionManifest()
      : mass_total_(0), sizes_set_(false), key_sz_(0), val_sz_(0) {}

  int GetOverLappingEntries(float point, PartitionManifestMatch& match);

  int GetOverLappingEntries(int epoch, float range_begin, float range_end,
                            PartitionManifestMatch& match);

  Status GetKVSizes(uint64_t& key_sz, uint64_t& val_sz) {
    Status s = Status::OK();

    if (sizes_set_) {
      key_sz = key_sz_;
      val_sz = val_sz_;
    } else {
      s = Status::NotFound("Values not set");
    }

    return s;
  }

 private:
  friend class PartitionManifestReader;

  void AddItem(PartitionManifestItem& item) {
    items_.push_back(item);
    mass_total_ += item.part_item_count;
  }

  Status UpdateKVSizes(const uint64_t key_sz, const uint64_t val_sz) {
    Status s = Status::OK();

    if (!sizes_set_) {
      key_sz_ = key_sz;
      val_sz_ = val_sz;
      sizes_set_ = true;
    } else if (key_sz != key_sz_ or val_sz != val_sz_) {
      s = Status::InvalidArgument("Key/Value sizes different from expected.");
    }

    return s;
  }

  std::vector<PartitionManifestItem> items_;
  uint64_t mass_total_;
  bool sizes_set_;
  uint64_t key_sz_;
  uint64_t val_sz_;
};
}  // namespace plfsio
}  // namespace pdlfs