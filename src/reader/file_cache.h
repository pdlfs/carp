//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/env.h"

#include <map>

namespace pdlfs {
namespace plfsio {

typedef struct ParsedFooter ParsedFooter;

template <typename T>
struct FileCacheEntry {
  int rank;
  bool is_open;
  T* fh;
  uint64_t fsz;
};

struct ReadRequest {
  /* offset is relative to current cursor for a sequential file */
  uint64_t offset;
  uint64_t bytes;
  uint64_t item_count;
  Slice slice;
  char* scratch;

  bool operator<(const ReadRequest& rhs) const { return offset < rhs.offset; }
};

template <typename T>
class CachingDirReader {
 public:
  CachingDirReader(Env* env, int max_cache_size = 512)
      : env_(env), dir_(""), num_ranks_(0), kMaxCacheSz(max_cache_size) {}

  Status GetFileHandle(int rank, T** fh, uint64_t* fsz,
                       bool force_reopen = false);

  Status ReadDirectory(std::string dir, int& num_ranks);

  int NumRanks() const { return num_ranks_; }

#define KB(n) (n##u) << 10
  Status ReadFooter(int rank, ParsedFooter& parsed_footer,
                    uint64_t opt_rdsz = KB(4));

  Status Read(int rank, ReadRequest& request, bool force_reopen = true);

  Status ReadBatch(int rank, std::vector<ReadRequest>& requests);

 private:
  // Works even if handle is not open, files are assumed to be static
  Status GetFileSize(int rank, uint64_t* fsz);

  Status OpenFileHandle(int rank, T** fh, uint64_t* fsz);

  std::string RdbName(const std::string& parent, int rank) {
    char tmp[20];
    snprintf(tmp, sizeof(tmp), "RDB-%08x.tbl", rank);
    return parent + "/" + tmp;
  }

  Env* const env_;
  std::string dir_;
  std::map<int, FileCacheEntry<T> > cache_;
  int num_ranks_;
  const int kMaxCacheSz;
};

}  // namespace plfsio
}  // namespace pdlfs
