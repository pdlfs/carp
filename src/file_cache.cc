//
// Created by Ankush J on 2/1/21.
//

#include "file_cache.h"

#include "common.h"

#include <string>

namespace pdlfs {
namespace plfsio {
Status CachingDirReader::ReadDirectory(std::string dir, int& num_ranks) {
  dir_ = dir;
  uint64_t fsz;

  for (int rank = 0;; rank++) {
    std::string fname = RdbName(dir_, rank);
    bool file_exists = env_->FileExists(fname.c_str());
    if (!file_exists) break;

    Status s = env_->GetFileSize(fname.c_str(), &fsz);
    if (!s.ok()) continue;

    logf(LOG_DBUG, "File: %s, size: %u\n", fname.c_str(), fsz);

    RandomAccessFile* fp;
    s = env_->NewRandomAccessFile(fname.c_str(), &fp);
    if (!s.ok()) continue;

    FileCacheEntry fe = {rank, true, fp, fsz};
    cache_[rank] = fe;
  }

  logf(LOG_INFO, "%u files found.\n", cache_.size());
  num_ranks = cache_.size();

  return Status::OK();
}

}  // namespace plfsio
}  // namespace pdlfs