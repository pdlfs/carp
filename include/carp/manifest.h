//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/env.h"
#include "pdlfs-common/status.h"

#include <algorithm>
#include <float.h>
#include <inttypes.h>
#include <map>
#include <sstream>
#include <stdint.h>
#include <vector>

namespace pdlfs {
namespace plfsio {
struct Range {
  float range_min, range_max;

  Range() : range_min(FLT_MAX), range_max(FLT_MIN) {}

  Range(float rmin, float rmax) : range_min(rmin), range_max(rmax) {}

  bool Overlaps(float point) const {
    return point >= range_min and point <= range_max;
  }

  bool Overlaps(float rmin, float rmax) const {
    return Overlaps(rmin) or Overlaps(rmax) or
           ((rmin < range_min) and (rmax > range_max));
  }

  bool Overlaps(const Range& r) const {
    return Overlaps(r.range_max) or Overlaps(r.range_max) or
           ((r.range_min < range_min) and (r.range_max > range_max));
  }

  bool ZeroWidth() const { return range_min == range_max; }

  void Reset() {
    range_min = FLT_MAX;
    range_max = FLT_MIN;
  }

  bool Inside(float f) const { return (f >= range_min && f <= range_max); }

  bool IsValid() const {
    return ((range_min == FLT_MAX && range_max == FLT_MIN) or
            (range_min <= range_max));
  }

  void Extend(float f) {
    range_min = std::min(range_min, f);
    range_max = std::max(range_max, f);
  }

  void Extend(const Range& r) {
    assert(r.IsValid());
    range_min = std::min(range_min, r.range_min);
    range_max = std::max(range_max, r.range_max);
  }
};

struct Query {
 public:
  int epoch;
  Range range;

  Query(int epoch, float rmin, float rmax) : epoch(epoch), range(rmin, rmax) {}

  bool Overlaps(const Query& rhs) const {
    return (epoch == rhs.epoch) and range.Overlaps(rhs.range);
  };

  std::string ToString() const {
    size_t buf_sz = 1024;
    char buf[buf_sz];
    snprintf(buf, buf_sz, "[Query] [Epoch %d] %.3f to %.3f", epoch,
             range.range_min, range.range_max);
    return std::string(buf);
  }
};

struct PartitionManifestItem {
  int epoch;
  int rank;
  uint64_t offset;
  Range observed;
  Range expected;
  uint32_t updcnt;
  uint32_t part_item_count;
  uint32_t part_item_oob;

  bool Overlaps(float point) const { return observed.Overlaps(point); }
  bool Overlaps(float range_begin, float range_end) const {
    return observed.Overlaps(Range(range_begin, range_end));
  }
  bool Overlaps(const Range& r) const { return observed.Overlaps(r); }

  std::string ToString() const {
    char buf[1024];
    // clang-format off
    snprintf(buf, 1024,
             "[EPOCH %d][%10" PRIu64 "]\t%.3f -> %.3f\t"
             "(Expected: %.3f to %.3f, Rank %d, %u items, %u OOB, Round %u)",
             epoch, offset, observed.range_min, observed.range_max,
             expected.range_min, expected.range_max, rank, part_item_count,
             part_item_oob, updcnt);
    // clang-format on
    return buf;
  }

  std::string ToCSVString() const {
    // clang-format off
    char buf[1024];
    snprintf(buf, 1024,
             "%d,%" PRIu64 ",%f,%f,%f,%f," /* ranges */
             "%u,%u,%u", /* counts */
             epoch, offset, 
             observed.range_min, observed.range_max,
             expected.range_min, expected.range_max,
             part_item_count, part_item_oob, updcnt);
    // clang-format on
    return buf;
  }
};

#define PROPLT(x) (lhs.x < rhs.x)
#define PROPEQ(x) (lhs.x == rhs.x)
struct PMIRangeComparator {
  bool operator()(const PartitionManifestItem& lhs,
                  const PartitionManifestItem& rhs) const {
    return (PROPLT(epoch) || (PROPEQ(epoch) && PROPLT(observed.range_min)) ||
            (PROPEQ(epoch) && PROPEQ(observed.range_min) &&
             PROPLT(observed.range_max)));
  }
};

struct PMIOffsetComparator {
  bool operator()(const PartitionManifestItem& lhs,
                  const PartitionManifestItem& rhs) const {
    return (PROPLT(epoch) || (PROPEQ(epoch) && PROPLT(rank)) ||
            (PROPEQ(epoch) && PROPEQ(rank) && PROPLT(offset)));
  }
};
#undef PROPLT
#undef PROPEQ

class PartitionManifestMatch {
 public:
  PartitionManifestMatch()
      : mass_data_(0), mass_total_(0), mass_oob_(0), key_sz_(0), val_sz_(0){};

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

  uint64_t TotalMass() const { return mass_total_; }

  float GetSelectivity() const {
    return mass_data_ ? mass_total_ * 1.0f / mass_data_ : 0;
  }

  void GetKVSizes(uint64_t& key_sz, uint64_t& val_sz) const {
    key_sz = key_sz_;
    val_sz = val_sz_;
  }

  uint64_t Size() const { return items_.size(); }

  uint64_t DataSize() const { return mass_data_; }

  PartitionManifestItem& operator[](size_t i) { return this->items_[i]; }

  std::string ToString() const {
    size_t buf_sz = 1024;
    char buf[buf_sz];

    snprintf(buf, buf_sz, "%.3f%% selectivity (%" PRIu64 " items)",
             GetSelectivity() * 100, TotalMass());

    return std::string(buf);
  }

  void Print();

 private:
  void SetDataSize(size_t data_sz) { mass_data_ = data_sz; }

  void SetKVSizes(uint64_t key_sz, uint64_t val_sz) {
    key_sz_ = key_sz;
    val_sz_ = val_sz;
  }

  std::vector<PartitionManifestItem> items_;
  std::map<int, std::vector<size_t> > ranks_;
  uint64_t mass_data_;
  uint64_t mass_total_;
  uint64_t mass_oob_;
  uint64_t key_sz_;
  uint64_t val_sz_;

  friend class PartitionManifest;
  template <typename U>
  friend class SimpleReader;
  friend class QueryMatchOptimizer;
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

  int GetOverlappingEntries(int epoch, float point,
                            PartitionManifestMatch& match);

  int GetOverlappingEntries(int epoch, float range_begin, float range_end,
                            PartitionManifestMatch& match);

  int GetOverlappingEntries(Query& q, PartitionManifestMatch& match);

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

  void SortByKey() {
    std::sort(items_.begin(), items_.end(), PMIRangeComparator());
  }

  void SortByOffset() {
    std::sort(items_.begin(), items_.end(), PMIOffsetComparator());
  }

  PartitionManifestItem& operator[](size_t i) { return this->items_[i]; }

  size_t Size() const { return items_.size(); }

 private:
  friend class PartitionManifestReader;

  void GenEpochStatsCSV(const int epoch, WritableFile* fd);

  void AddItem(PartitionManifestItem& item) {
    items_.push_back(item);
    mass_total_ += item.part_item_count;

    if (item.observed.ZeroWidth()) {
      zero_sst_cnt_++;
    }

    if (item.epoch >= num_epochs_) {
      num_epochs_ = item.epoch + 1;  // zero-indexed;
      mass_epoch_.resize(num_epochs_, 0);
      range_epoch_.resize(num_epochs_);
    }

    mass_epoch_[item.epoch] += item.part_item_count;
    range_epoch_[item.epoch].Extend(item.observed);
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
