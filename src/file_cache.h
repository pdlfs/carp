//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/env.h"

#include <map>

namespace pdlfs {
namespace plfsio {

struct FileCacheEntry {
  int rank;
  bool is_open;
  RandomAccessFile* fh;
  uint64_t fsz;
};

class CachingDirReader {
 public:
  CachingDirReader(Env* env, int max_cache_size = 512)
      : env_(env), kMaxCacheSz(max_cache_size) {}

  Status ReadDirectory(std::string dir, int& num_ranks);

  Status GetFileHandle(int rank, RandomAccessFile** fh, uint64_t* fsz) {
    if (!cache_[rank].is_open) return Status::NotFound("File not open");

    *fh = cache_[rank].fh;
    *fsz = cache_[rank].fsz;

    return Status::OK();
  }

 private:
  std::string RdbName(const std::string& parent, int rank) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "RDB-%08x.tbl", rank);
    return parent + "/" + tmp;
  }

  Env* const env_;
  std::string dir_;
  std::map<int, FileCacheEntry> cache_;
  const int kMaxCacheSz;
};

}  // namespace plfsio
}  // namespace pdlfs