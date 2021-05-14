//
// Created by Ankush on 5/13/2021.
//

#pragma once

#include "common.h"
#include "file_cache.h"
#include "reader_base.h"

#include <pdlfs-common/env.h>

namespace pdlfs {
namespace plfsio {
class FmtChecker : public ReaderBase {
 public:
  explicit FmtChecker(const RdbOptions& options) : ReaderBase(options) {}

  Status Run() {
    Status s = Status::OK();
    ReadManifests();
    manifest_.SortByOffset();

#define KB(n) ((n)*1024)
#define MB(n) (1024 * KB(n))

    size_t buf_sz = MB(20);
    char* buf = new char[buf_sz];
    size_t kvp_sz = sizeof(float) + val_sz_;

    for (size_t i = 0; i < manifest_.Size(); i++) {
      PartitionManifestItem& item = manifest_[i];
      printf("%s\n", item.ToString().c_str());
      size_t sst_sz = item.part_item_count * kvp_sz;

      if (sst_sz > buf_sz) {
        logf(LOG_ERRO, "Insufficient buffer size (Reqd: %zu, Have: %zu)\n",
             sst_sz, buf_sz);
        s = Status::BufferFull("Insufficient buffer");
        return s;
      }

      Slice sl;
      ReadSST(item, sl, buf);
      bool valid = ValidateSST(item, sl);
      if (!valid) {
        LogSST(item, sl);
      }
    }

    delete buf;

    return s;
  }

  bool ValidateSST(PartitionManifestItem meta, Slice& data) {
    const float* key_blk = (const float*)data.data();
    uint64_t item_cnt = meta.part_item_count;
    float rbeg = meta.part_range_begin;
    float rend = meta.part_range_end;

    for (uint64_t i = 0; i < item_cnt; i += 1000) {
      if (key_blk[i] < rbeg || key_blk[i] > rend) {
        logf(LOG_ERRO, "Validation failed!");
        return false;
      }
    }

    return true;
  }

  void LogSST(PartitionManifestItem meta, Slice& data) {
    const float* key_blk = (const float*)data.data();

    uint64_t item_cnt = meta.part_item_count;
    float rbeg = meta.part_range_begin;
    float rend = meta.part_range_end;

    for (uint64_t i = 0; i < item_cnt; i += 1000) {
      if (key_blk[i] < rbeg || key_blk[i] > rend) {
        logf(LOG_DBUG, "key mismatch at %.1f%%: %f (%f to %f)\n",
             i * 100.0 / item_cnt, key_blk[i], rbeg, rend);
        break;
      }
    }
  }

 private:
};
}  // namespace plfsio
}  // namespace pdlfs
