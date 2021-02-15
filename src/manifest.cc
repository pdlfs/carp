//
// Created by Ankush J on 2/3/21.
//

#include "carp/manifest.h"

#include "common.h"

namespace pdlfs {
namespace plfsio {
int PartitionManifest::GetOverLappingEntries(int epoch, float point,
                                             PartitionManifestMatch& match) {
  for (size_t i = 0; i < items_.size(); i++) {
    if (items_[i].epoch == epoch && items_[i].Overlaps(point)) {
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

Status PartitionManifest::GenOverlapStats(const char* dir_path,
                                          Env* const env) {
  int num_epochs;
  GetEpochCount(num_epochs);

  char csv_path[2048];

  for (int epoch = 0; epoch < num_epochs; epoch++) {
    snprintf(csv_path, 2048, "%s/manifest.e%d.csv", dir_path, epoch);
    WritableFile* fd;
    env->NewWritableFile(csv_path, &fd);
    GenEpochStatsCSV(epoch, fd);
    delete fd;
  }

  return Status::OK();
}

void PartitionManifest::GenEpochStatsCSV(const int epoch,
                                         WritableFile* const fd) {
  Range range;
  GetEpochRange(epoch, range);

  uint64_t epoch_mass;
  GetEpochMass(epoch, epoch_mass);

  float rbeg = range.range_min;
  float rend = range.range_max;
  float rdelta = (rend - rbeg) / 100;

  const char* header = "Epoch,Point,MatchMass,TotalMass,MatchCount\n";
  fd->Append(Slice(header));

  for (float r = rbeg; r < rend; r += rdelta) {
    PartitionManifestMatch match;
    GetOverLappingEntries(epoch, r, match);
    char buf[1024];
    int buf_len = snprintf(buf, 1024, "%d,%f,%llu,%llu,%zu\n", epoch, r,
                           match.TotalMass(), epoch_mass, match.Size());
    assert(buf_len > 0 and buf_len < 1024);
    fd->Append(Slice(buf, buf_len));
  }

  fd->Close();
}

void PartitionManifestMatch::Print() {
  std::sort(items_.begin(), items_.end());
  for (size_t i = 0; i < Size(); i++) {
    logf(LOG_INFO, "%s\n", items_[i].ToString().c_str());
  }
}
}  // namespace plfsio
}  // namespace pdlfs
