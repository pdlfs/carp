//
// Created by Ankush on 5/11/2021.
//

#pragma once

#include "carp/manifest.h"
#include "file_cache.h"
#include "plfs_wrapper.h"

#include <queue>

namespace pdlfs {
namespace plfsio {
class SlidingSorter {
 public:
  explicit SlidingSorter(std::string dir_out,
                         CachingDirReader<SequentialFile>& fdcache)
      : num_ranks_(0), last_cutoff_(0), dir_out_(dir_out), fdcache_(fdcache) {}

  Status AddItem(const PartitionManifestItem& item);

  static void SetKVSizes(size_t key_sz, size_t val_sz) {
    assert(key_sz == sizeof(float));
    val_sz_ = val_sz;
  }

  Status FlushUntil(float cutoff) {
    Status s = Status::OK();
    EnsurePlfs();

    if (cutoff < last_cutoff_) {
      s = Status::InvalidArgument("cutoff greater than last cutoff");
      return s;
    }

    while ((!merge_pool_.empty()) && (merge_pool_.top().key < cutoff)) {
      // write to plfsdir
      const KVItem& item = merge_pool_.top();
      Slice sl(item.val, val_sz_);
      plfs_.Append(item.key, sl);
      merge_pool_.pop();
    }
    return s;
  }

  Status FlushAll() {
    Status s = Status::OK();
    EnsurePlfs();

    while ((!merge_pool_.empty())) {
      // write to plfsdir
      const KVItem& item = merge_pool_.top();
      Slice sl(item.val, val_sz_);
      plfs_.Append(item.key, sl);
      merge_pool_.pop();
    }

    plfs_.Flush();
    plfs_.CloseDir();

    return s;
  }

 private:
  void EnsurePlfs() {
    if (!plfs_.IsOpen()) {
      plfs_.OpenDir(dir_out_.c_str());
    }
  }

  void AddSST(const Slice& data, size_t item_sz, uint64_t num_items) {
    assert(item_sz >= sizeof(float));
    assert(data.size() >= item_sz * num_items);

    size_t val_sz = item_sz - sizeof(float);
    const float* keyblk = (float*)&data[0];
    const char* valblk = &data[num_items * sizeof(float)];

    for (uint64_t i = 0; i < num_items; i++) {
      if (i % 1000 == 0) {
        printf("key: %f\n", keyblk[i]);
      }

      Slice val = Slice(&valblk[i * val_sz], val_sz);
      AddPair(keyblk[i], val);
    }
  }

  void AddPair(float key, Slice& val) {
    merge_pool_.push(KVItem(key, val));
  }

  static const size_t kMaxValSz = 60;

  struct KVItem {
    float key;
    char val[kMaxValSz];

    KVItem(float key_arg, Slice& val_arg): key(key_arg) {
      memcpy(val, val_arg.data(), val_sz_);
    }

    KVItem(const KVItem& other) {
      key = other.key;
      memcpy(val, other.val, val_sz_);
    }
    bool operator<(const KVItem& other) const { return key < other.key; }
  };

  int num_ranks_;
  static size_t val_sz_;
  float last_cutoff_;

  std::string dir_out_;

  CachingDirReader<SequentialFile>& fdcache_;
  std::vector<size_t> rank_cursors_;
  std::priority_queue<KVItem> merge_pool_;
  PlfsWrapper plfs_;
};
}  // namespace plfsio
}  // namespace pdlfs