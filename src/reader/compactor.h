//
// Created by Ankush on 5/11/2021.
//

#pragma once

#include "file_cache.h"
#include "reader_base.h"
#include "sliding_sorter.h"

namespace pdlfs {
namespace plfsio {
class CompactorLogger {
 public:
  explicit CompactorLogger(Env* const env) : env_(env) {}
  void MarkEpochBegin() {
    uint64_t now = env_->NowMicros();
    epoch_begins_.push_back(now);

    if (epoch_begins_.size() != epoch_ends_.size() + 1u) {
      logf(LOG_WARN, "CompactorLogger: begins/ends mismatched!");
    }
  }

  void MarkEpochEnd() {
    uint64_t now = env_->NowMicros();
    epoch_ends_.push_back(now);

    if (epoch_begins_.size() != epoch_ends_.size()) {
      logf(LOG_WARN, "CompactorLogger: begins/ends mismatched!");
    }
  }

  void PrintStats();

 private:
  Env* const env_;
  std::vector< uint64_t > epoch_begins_;
  std::vector< uint64_t > epoch_ends_;
};

class Compactor : public ReaderBase {
 public:
  explicit Compactor(const RdbOptions& options)
      : logger_(options.env), ReaderBase(options) {}

  Status Run() {
    Status s = Status::OK();

    s = ReadManifests();
    if (!s.ok()) return s;

   // s = MergeAll();
    s = MergeEpoch(options_.query_epoch);

    return s;
  }

 private:
  friend class CompactorLogger;
  CompactorLogger logger_;
  static const size_t kMemMax = MB(500);

  struct PartitionedRun {
    std::vector< PartitionManifestItem > items;
    float partition_point;

    PartitionedRun() : partition_point(0) {}

    PartitionedRun(const PartitionedRun& rhs)
        : items(rhs.items.begin(), rhs.items.end()),
          partition_point(rhs.partition_point) {}

    void SortByKey() {
      std::sort(items.begin(), items.end(), PMIRangeComparator());
    }

    void SortByOffset() {
      std::sort(items.begin(), items.end(), PMIOffsetComparator());
    }

    void Empty() { items.resize(0); }
  };

  typedef std::map< int, std::vector< PartitionedRun > > EpochRunMap;

  bool MemoryFootprintExceeded(uint64_t item_cnt) const {
    return (item_cnt * val_sz_ > kMemMax);
  }

  Status MergeAll();
  Status MergeEpoch(int epoch);
  Status ComputeRuns(EpochRunMap& run_map);
  Status ComputeRunsForEpoch(std::vector< PartitionedRun >& runs, int epoch,
                             size_t& mf_idx);
  std::string CreateDestDir(int epoch = -1) {
    std::string dest = options_.output_path;

    Status s = options_.env->CreateDir(dest.c_str());
    if (!s.ok()) {
      printf("dir create error: %s\n", s.ToString().c_str());
    }

    if (epoch != -1) {
      dest = dest + "/" + std::to_string(epoch);
      s = options_.env->CreateDir(dest.c_str());
      if (!s.ok()) {
        printf("dir create error: %s\n", s.ToString().c_str());
      }
    }

    return dest;
  }
};
}  // namespace plfsio
}  // namespace pdlfs
