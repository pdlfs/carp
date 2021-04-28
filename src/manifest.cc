//
// Created by Ankush J on 2/3/21.
//

#include "carp/manifest.h"

#include "common.h"

#include <algorithm>

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
    snprintf(csv_path, 2048, "%s/rdb.olap.e%d.csv", dir_path, epoch);
    WritableFile* fd;
    env->NewWritableFile(csv_path, &fd);
    GenEpochStatsCSV(epoch, fd);
    delete fd;
  }

  fprintf(stderr, "[Analytics] Total SSTs: %zu, zero width: %d\n",
          items_.size(), zero_sst_cnt_);

  return Status::OK();
}

static std::vector<float> GenPoints(float rbeg, float rend) {
  float cutoffs[] = {rbeg, 2, 5, 10, 20, 50, 100};
  float deltas[] = {0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0};

  std::vector<float> probe_points;

  int ni = sizeof(cutoffs) / sizeof(cutoffs[0]);
  for (int i = ni - 1; i >= 0; i--) {
    float loop_beg = cutoffs[i];
    float loop_del = deltas[i];
    if (rend <= loop_beg) continue;

    // rend > loop_beg
    for (float f = loop_beg; f < rend; f += loop_del) {
      probe_points.push_back(f);
    }

    rend = loop_beg;
  }

  std::sort(probe_points.begin(), probe_points.end());
  return probe_points;
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

  uint64_t max_match_mass = 0;

  std::vector<float> probe_points = GenPoints(rbeg, rend);

  for (size_t i = 0; i < probe_points.size(); i++) {
    float r = probe_points[i];
    PartitionManifestMatch match;
    GetOverLappingEntries(epoch, r, match);
    char buf[1024];
    int buf_len = snprintf(buf, 1024, "%d,%f,%llu,%llu,%llu\n", epoch, r,
                           match.TotalMass(), epoch_mass, match.Size());
    assert(buf_len > 0 and buf_len < 1024);
    fd->Append(Slice(buf, buf_len));

    max_match_mass = std::max(max_match_mass, match.TotalMass());
  }

  fprintf(stderr, "[Analytics] [epoch %d] Max Overlap: %.2f%%\n", epoch,
          max_match_mass * 100.0f / epoch_mass);

  fd->Close();
}

void PartitionManifestMatch::Print() {
  for (size_t i = 0; i < Size(); i++) {
    logf(LOG_DBUG, "%s\n", items_[i].ToString().c_str());
  }
}
}  // namespace plfsio
}  // namespace pdlfs
