//
// Created by Ankush on 5/17/2021.
//

#include "compactor.h"

namespace pdlfs {
namespace plfsio {

void CompactorLogger::PrintStats() {
  size_t num_epochs = std::min(epoch_begins_.size(), epoch_ends_.size());

  uint64_t time_total = 0;
  for (size_t i = 0; i < num_epochs; i++) {
    uint64_t time_cur = epoch_ends_[i] - epoch_begins_[i];
    time_total += time_cur;
  }

  float time_sec = time_total * 1e-6;

  logv(__LOG_ARGS__, LOG_INFO, "[Compactor] Memory Used: %.1fMB",
       Compactor::kMemMax / (1024.0 * 1024.0));

  logv(__LOG_ARGS__, LOG_INFO, "[Compactor] Total time taken: %.2f s (%.3f s/epoch)",
       time_sec, time_sec / num_epochs);
}

Status Compactor::MergeAll() {
  Status s = Status::OK();

  std::string merge_dest = CreateDestDir();

  uint64_t key_sz, val_sz;
  manifest_.GetKVSizes(key_sz, val_sz);
  SlidingSorter::SetKVSizes(key_sz, val_sz);
  SlidingSorter sorter(merge_dest, fdcache_);

  EpochRunMap run_map;
  s = ComputeRuns(run_map);
  if (!s.ok()) return s;

#define EXISTS(map, epoch) ((map).find(epoch) != (map).end())

  for (int epoch = 0; EXISTS(run_map, epoch); epoch++) {
    logv(__LOG_ARGS__, LOG_INFO, "currently sorting: epoch %d\n", epoch);
    logger_.MarkEpochBegin();

    std::vector<PartitionedRun>& runs = run_map[epoch];

    sorter.EpochBegin();

    for (size_t i = 0; i < runs.size(); i++) {
      PartitionedRun& run = runs[i];
      for (size_t ri = 0; ri < run.items.size(); ri++) {
        PartitionManifestItem& item = run.items[ri];
        sorter.AddManifestItem(item);
      }
      logv(__LOG_ARGS__, LOG_DBUG, "Sorting until: %f\n", run.partition_point);
      sorter.FlushUntil(run.partition_point);
    }

    /* PartitionedRun has a FLT_MAX entry, so this is unnecessary, but just to
     * be safe, we call it explicitly */
    sorter.FlushAll();

    logger_.MarkEpochEnd();
  }

  sorter.Close();

  logger_.PrintStats();

  return s;
}

Status Compactor::MergeEpoch(int epoch) {
  Status s = Status::OK();

  std::string merge_dest = CreateDestDir(epoch);

  uint64_t key_sz, val_sz;
  manifest_.GetKVSizes(key_sz, val_sz);
  SlidingSorter::SetKVSizes(key_sz, val_sz);
  SlidingSorter sorter(merge_dest, fdcache_);

  EpochRunMap run_map;
  s = ComputeRuns(run_map);
  if (!s.ok()) return s;

#define EXISTS(map, epoch) ((map).find(epoch) != (map).end())

  logv(__LOG_ARGS__, LOG_INFO, "currently sorting: epoch %d\n", epoch);
  logger_.MarkEpochBegin();

  std::vector<PartitionedRun>& runs = run_map[epoch];

  for (int e = 0; e <= epoch; e++) {
    sorter.EpochBegin();
  }

  for (size_t i = 0; i < runs.size(); i++) {
    PartitionedRun& run = runs[i];
    for (size_t ri = 0; ri < run.items.size(); ri++) {
      PartitionManifestItem& item = run.items[ri];
      sorter.AddManifestItem(item);
    }
    logv(__LOG_ARGS__, LOG_DBUG, "Sorting until: %f\n", run.partition_point);
    sorter.FlushUntil(run.partition_point);
  }

  /* PartitionedRun has a FLT_MAX entry, so this is unnecessary, but just to
   * be safe, we call it explicitly */
  sorter.FlushAll();

  logger_.MarkEpochEnd();

  sorter.Close();

  logger_.PrintStats();

  return s;
}

Status Compactor::ComputeRuns(Compactor::EpochRunMap& run_map) {
  Status s = Status::OK();

  manifest_.SortByKey();

  size_t mf_idx = 0;
  int last_epoch = 0;

  while (mf_idx < manifest_.Size()) {
    s = ComputeRunsForEpoch(run_map[last_epoch], last_epoch, mf_idx);
    if (!s.ok()) return s;
    last_epoch++;
  }

  return s;
}

Status Compactor::ComputeRunsForEpoch(std::vector<PartitionedRun>& runs,
                                      int epoch, size_t& mf_idx) {
  Status s = Status::OK();

  float cur_min = FLT_MIN;
  uint64_t cur_count = 0;

  PartitionedRun cur_run;

  for (; mf_idx < manifest_.Size(); mf_idx++) {
    PartitionManifestItem& item = manifest_[mf_idx];

    if (item.epoch > epoch)
      break;
    else if (item.epoch < epoch) {
      s = Status::Corruption("unexpected epoch; manifest epochs not in order!");
      return s;
    }

    if ((item.observed.range_min > cur_min) and
        MemoryFootprintExceeded(cur_count)) {
      cur_min = item.observed.range_min;
      cur_count = 0;

      cur_run.partition_point = cur_min;
      cur_run.SortByOffset();
      runs.push_back(cur_run);
      cur_run.Empty();
    }

    cur_count += item.part_item_count;
    cur_run.items.push_back(item);
  }

  if (!cur_run.items.empty()) {
    cur_run.partition_point = FLT_MAX;
    runs.push_back(cur_run);
  }

  return s;
}
}  // namespace plfsio
}  // namespace pdlfs
