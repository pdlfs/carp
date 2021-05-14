//
// Created by Ankush J on 1/22/21.
//

#pragma once

#include "carp/coding_float.h"
#include "carp/manifest.h"
#include "common.h"
#include "file_cache.h"
#include "manifest_reader.h"
#include "perf.h"
#include "task_completion_tracker.h"
#include "tbb_check.h"

#include "pdlfs-common/env.h"

#if defined(PDLFS_TBB)
#include <execution>
#endif
#include <map>

namespace pdlfs {
namespace plfsio {
struct KeyPair {
  float key;
  int rank;
  size_t offset;
};

struct ManifestReadWorkItem {
  int rank;
  CachingDirReader<SequentialFile>* fdcache;
  TaskCompletionTracker* task_tracker;
  PartitionManifestReader* manifest_reader;
};

struct SSTReadWorkItem {
  PartitionManifestItem* item;
  size_t key_sz;
  size_t val_sz;

  std::vector<KeyPair>* query_results;
  uint64_t qrvec_offset;

  CachingDirReader<SequentialFile>* fdcache;
  TaskCompletionTracker* task_tracker;
};

struct RankwiseSSTReadWorkItem {
  std::vector<PartitionManifestItem> wi_vec;
  int rank;
  size_t key_sz;
  size_t val_sz;

  std::vector<KeyPair>* query_results;
  uint64_t qrvec_offset;

  CachingDirReader<SequentialFile>* fdcache;
  TaskCompletionTracker* task_tracker;
};

struct KeyPairComparator {
  inline bool operator()(const KeyPair& lhs, const KeyPair& rhs) {
    return lhs.key < rhs.key;
  }
};

class RangeReader {
 public:
  RangeReader(const RdbOptions& options)
      : options_(options),
        dir_path_(""),
        fdcache_(options.env),
        manifest_reader_(manifest_),
        num_ranks_(0),
        logger_(options.env) {
    thpool_ = ThreadPool::NewFixed(options.parallelism);
  }

  Status ReadManifest(const std::string& dir_path);

  Status QueryParallel(int epoch, float rbegin, float rend);

  Status Query(int epoch, float rbegin, float rend);

 private:
  static void ManifestReadWorker(void* arg);

  static Status ReadFooter(RandomAccessFile* fh, uint64_t fsz,
                           ParsedFooter& pf);

  Status ReadSSTs(PartitionManifestMatch& match,
                  std::vector<KeyPair>& query_results);

  Status RankwiseReadSSTs(PartitionManifestMatch& match,
                  std::vector<KeyPair>& query_results);

  static void SSTReadWorker(void* arg);

  static void RankwiseSSTReadWorker(void* arg);

  void ReadBlock(int rank, uint64_t offset, uint64_t size, Slice& slice,
                 std::string& scratch, bool preview = true);

  const RdbOptions& options_;
  std::string dir_path_;
  CachingDirReader<SequentialFile> fdcache_;
  PartitionManifest manifest_;
  PartitionManifestReader manifest_reader_;
  int num_ranks_;
  std::vector<KeyPair> query_results_;

  ThreadPool* thpool_;
  TaskCompletionTracker task_tracker_;

  RangeReaderPerfLogger logger_;
};
}  // namespace plfsio
}  // namespace pdlfs
