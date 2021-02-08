//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "file_cache.h"
#include "file_cache.h"

#include "pdlfs-common/env.h"

#include <map>

namespace pdlfs {
namespace plfsio {

typedef struct ParsedFooter ParsedFooter;

struct FileCacheEntry {
  int rank;
  bool is_open;
  RandomAccessFile* fh;
  uint64_t fsz;
};

class CachingDirReader {
 public:
  CachingDirReader(Env* env, int max_cache_size = 512)
      : env_(env), dir_(""), kMaxCacheSz(max_cache_size) {}

  Status ReadDirectory(std::string dir, int& num_ranks);

  Status GetFileHandle(int rank, RandomAccessFile** fh, uint64_t* fsz) {
    if (!cache_[rank].is_open) return Status::NotFound("File not open");

    *fh = cache_[rank].fh;
    *fsz = cache_[rank].fsz;

    return Status::OK();
  }

#define MAP_HAS(m, k) (m.find(k) != m.end())

#define MB(n) (n##u) << 10
  Status ReadFooter(int rank, ParsedFooter& parsed_footer,
                    uint64_t opt_rdsz = MB(4));

  Status OpenSeqFileHandle(int rank, SequentialFile** sfh, uint64_t* fsz) {
    return OpenFileHandle(rank, sfh, nullptr, fsz);
  }

  Status OpenRandomFileHandle(int rank, RandomAccessFile** rfh, uint64_t* fsz) {
    return OpenFileHandle(rank, nullptr, rfh, fsz);
  }

 private:
  Status OpenFileHandle(int rank, SequentialFile** sfh, RandomAccessFile** rfh,
                        uint64_t* fsz);
  Status OpenAndReadRandom();
  Status OpenAndReadSequential();
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