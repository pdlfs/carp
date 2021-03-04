//
// Created by Ankush J on 2/3/21.
//

#pragma once

#include "carp/manifest.h"
#include "common.h"

#include "pdlfs-common/port.h"

namespace pdlfs {
namespace plfsio {
class PartitionManifestReader {
 public:
  PartitionManifestReader(PartitionManifest& manifest);

  void EnableManifestOutput(std::string out_path) { output_path_ = out_path; }

  Status ReadManifest(int rank, Slice& footer_data, uint64_t footer_sz);

  Status UpdateKVSizes(uint64_t key_sz, uint64_t val_sz) {
    manifest_mutex_.Lock();
    Status s = manifest_.UpdateKVSizes(key_sz, val_sz);
    manifest_mutex_.Unlock();
    return s;
  }

 private:
  void ReadFooterEpoch(int epoch, int rank, Slice& data, uint64_t epoch_offset,
                       uint64_t epoch_sz, FILE* file_out = NULL);

  static void ComputeInternalOffsets(const size_t entry_sizes[],
                                     size_t offsets[], int num_entries,
                                     size_t& item_sz) {
    item_sz = 0;
    for (int i = 0; i < num_entries; i++) {
      offsets[i] = item_sz;
      item_sz += entry_sizes[i];
    }
  }

 private:
  PartitionManifest& manifest_;
  port::Mutex manifest_mutex_;

  /* | ITEM | ITEM | ITEM | ...
   * ITEM = [IDX:8B | OFFSET:8B | RBEG:4B | REND:4B | ICNT: 4B | IOOB: 4B]
   */
  static const size_t num_entries_ = 6;
  size_t entry_sizes_[num_entries_];
  size_t offsets_[num_entries_];
  size_t item_sz_;
  std::string output_path_;
};
}  // namespace plfsio
}  // namespace pdlfs
