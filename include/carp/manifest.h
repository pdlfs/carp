//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/status.h"

#include <float.h>
#include <map>
#include <sstream>
#include <stdint.h>
#include <vector>

namespace pdlfs {
namespace plfsio {
struct Range {
  float range_min, range_max;

  Range() : range_min(FLT_MAX), range_max(FLT_MIN) {}

  Range& operator=(Range& r) {
    range_min = r.range_min;
    range_max = r.range_max;
    return *this;
  }

  void Reset() {
    range_min = FLT_MAX;
    range_max = FLT_MIN;
  }

  bool Inside(float f) const { return (f >= range_min && f <= range_max); }

  bool IsValid() const {
    return ((range_min == FLT_MAX && range_max == FLT_MIN) or
            (range_min < range_max));
  }

  void Extend(float f) {
    range_min = std::min(range_min, f);
    range_max = std::max(range_min, f);
  }

  void Extend(float beg, float end) {
    assert(beg <= end);
    range_min = std::min(range_min, beg);
    range_max = std::max(range_max, end);
  }
};

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

  bool operator<(const PartitionManifestItem& rhs) const {
#define PROPLT(x) (x < rhs.x)
#define PROPEQ(x) (x == rhs.x)
    return (
        PROPLT(epoch) || (PROPEQ(epoch) && PROPLT(part_range_begin)) ||
        (PROPEQ(epoch) && PROPEQ(part_range_begin) && PROPLT(part_range_end)));
#undef PROPLT
#undef PROPEQ
  }

  std::string ToString() {
    char buf[1024];
    snprintf(buf, 1024, "[EPOCH %d][%10llu]\t%.3f -> %.3f\t(Rank %d, %u items)\n",
             epoch, offset, part_range_begin, part_range_end, rank,
             part_item_count);
    return buf;
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

  void Print();

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
      : mass_total_(0),
        num_epochs_(0),
        sizes_set_(false),
        key_sz_(0),
        val_sz_(0),
        zero_sst_cnt_(0) {}

  int GetOverLappingEntries(int epoch, float point,
                            PartitionManifestMatch& match);

  int GetOverLappingEntries(int epoch, float range_begin, float range_end,
                            PartitionManifestMatch& match);

  Status GenOverlapStats(const char* dir_path, Env* env);

  Status GetKVSizes(uint64_t& key_sz, uint64_t& val_sz) const {
    Status s = Status::OK();

    if (sizes_set_) {
      key_sz = key_sz_;
      val_sz = val_sz_;
    } else {
      s = Status::NotFound("Values not set");
    }

    return s;
  }

  Status GetEpochCount(int& num_epochs) const {
    num_epochs = num_epochs_;
    return Status::OK();
  };

  Status GetEpochRange(int epoch, Range& r) {
    Status s = Status::OK();

    if (range_epoch_.size() > epoch) {
      r = range_epoch_[epoch];
    } else {
      s = Status::InvalidArgument("Epoch not found");
    }

    return s;
  }

  Status GetEpochMass(int epoch, uint64_t& mass) {
    Status s = Status::OK();

    if (mass_epoch_.size() > epoch) {
      mass = mass_epoch_[epoch];
    } else {
      s = Status::InvalidArgument("Epoch not found");
    }

    return s;
  }

 private:
  friend class PartitionManifestReader;

  void GenEpochStatsCSV(const int epoch, WritableFile* fd);

  void AddItem(PartitionManifestItem& item) {
    items_.push_back(item);
    mass_total_ += item.part_item_count;

    if (item.part_range_begin == item.part_range_end) {
      zero_sst_cnt_++;
    }

    if (item.epoch >= num_epochs_) {
      num_epochs_ = item.epoch + 1;  // zero-indexed;
      mass_epoch_.resize(num_epochs_, 0);
      range_epoch_.resize(num_epochs_);
    }

    mass_epoch_[item.epoch] += item.part_item_count;
    range_epoch_[item.epoch].Extend(item.part_range_begin, item.part_range_end);
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
  int num_epochs_;
  std::vector<uint64_t> mass_epoch_;
  std::vector<Range> range_epoch_;
  bool sizes_set_;
  uint64_t key_sz_;
  uint64_t val_sz_;
  int zero_sst_cnt_;
};
}  // namespace plfsio
}  // namespace pdlfs
