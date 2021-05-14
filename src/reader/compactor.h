//
// Created by Ankush on 5/11/2021.
//

#pragma once

#include "file_cache.h"
#include "reader_base.h"
#include "sliding_sorter.h"

namespace pdlfs {
namespace plfsio {
class Compactor : public ReaderBase {
 public:
  Compactor(const RdbOptions& options) : ReaderBase(options) {}

  void Run() {
    ReadManifests();
    Merge();
  }

 private:
  void Merge() {
    std::string merge_dest = CreateDestDir();

    size_t key_sz, val_sz;
    manifest_.GetKVSizes(key_sz, val_sz);
    SlidingSorter::SetKVSizes(key_sz, val_sz);
    SlidingSorter sorter(merge_dest, fdcache_);

    float cur_min = FLT_MIN;
    uint64_t cur_count = 0;

    for (size_t i = 0; i < manifest_.Size(); i++) {
      PartitionManifestItem& item = manifest_[i];
      cur_count += item.part_item_count;
      printf("%s\n", item.ToString().c_str());
      if (item.part_range_begin > cur_min) {
        printf("partition point: %.4f (%.1f KB)\n", item.part_range_begin,
               cur_count * 60.0 / 1e6);
        cur_min = item.part_range_begin;
        cur_count = item.part_item_count;
        //        sorter.FlushUntil(cur_min);
      }

      sorter.AddItem(item);
    }

    sorter.FlushAll();
  }

  std::string CreateDestDir() {
    std::string dest = options_.data_path;
    if (dest[dest.size() - 1] == '/') dest.resize(dest.size() - 1);
    dest += ".merged";

    Status s = options_.env->CreateDir(dest.c_str());
    if (!s.ok()) {
      printf("dir create error: %s\n", s.ToString().c_str());
    }

    return dest;
  }
};
}  // namespace plfsio
}  // namespace pdlfs