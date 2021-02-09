//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/status.h"

#include <map>
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

class PartitionManifestMatch {
 public:
  PartitionManifestMatch()
      : mass_total_(0), mass_oob_(0), key_sz_(0), val_sz_(0){};

  void AddItem(PartitionManifestItem& item) {
    items_.push_back(item);
    ranks_[item.rank].push_back(items_.size() - 1);
    mass_total_ += item.part_item_count;
    mass_oob_ += item.part_item_oob;
  }

  void GetUniqueRanks(std::vector<int>& ranks) {
    std::map<int, std::vector<size_t> >::const_iterator it = ranks_.cbegin();
    for (; it != ranks_.cend(); it++) {
      ranks.push_back(it->first);
    }
  }

  uint64_t GetMatchesByRank(int rank,
                        std::vector<PartitionManifestItem>& rank_tables) {
    std::vector<size_t>& rank_items = ranks_[rank];
    uint64_t mass_rank = 0;

    for (size_t i = 0; i < rank_items.size(); i++) {
      size_t it_idx = rank_items[i];
      PartitionManifestItem& item = items_[it_idx];
      rank_tables.push_back(item);
      mass_rank += item.part_item_count;
    }

    return mass_rank;
  }

  uint64_t TotalMass() { return mass_total_; }

  void GetKVSizes(uint64_t& key_sz, uint64_t& val_sz) {
    key_sz = key_sz_;
    val_sz = val_sz_;
  }

  uint64_t Size() { return items_.size(); }

  PartitionManifestItem& operator[](size_t i) { return this->items_[i]; }

 private:
  void SetKVSizes(uint64_t key_sz, uint64_t val_sz) {
    key_sz_ = key_sz;
    val_sz_ = val_sz;
  }

  std::vector<PartitionManifestItem> items_;
  std::map<int, std::vector<size_t> > ranks_;
  uint64_t mass_total_;
  uint64_t mass_oob_;
  uint64_t key_sz_;
  uint64_t val_sz_;

  friend class PartitionManifest;
};

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

    if (item.epoch >= (int) mass_epoch_.size()) {
      mass_epoch_.resize(item.epoch + 1, 0);
    }

    mass_epoch_[item.epoch] += item.part_item_count;
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
  std::vector<uint64_t> mass_epoch_;
  bool sizes_set_;
  uint64_t key_sz_;
  uint64_t val_sz_;
};
}  // namespace plfsio
}  // namespace pdlfs
