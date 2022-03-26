//
// Created by Ankush J on 1/22/21.
//

#pragma once

#include <carp/carp_config.h>

#include "carp/coding_float.h"
#include "carp/manifest.h"
#include "common.h"
#include "file_cache.h"
#include "manifest_reader.h"
#include "perf.h"
#include "task_completion_tracker.h"

#include "pdlfs-common/env.h"

#include <map>

namespace pdlfs {
namespace plfsio {
struct KeyPair {
  float key;
  int rank;
  size_t offset;
};

template <typename T>
struct ManifestReadWorkItem {
  int rank;
  CachingDirReader<T>* fdcache;
  TaskCompletionTracker* task_tracker;
  PartitionManifestReader* manifest_reader;
};

template <typename T>
struct SSTReadWorkItem {
  PartitionManifestItem* item;
  size_t key_sz;
  size_t val_sz;

  std::vector<KeyPair>* query_results;
  uint64_t qrvec_offset;

  CachingDirReader<T>* fdcache;
  TaskCompletionTracker* task_tracker;
};

template <typename T>
struct RankwiseSSTReadWorkItem {
  std::vector<PartitionManifestItem> wi_vec;
  int rank;
  size_t key_sz;
  size_t val_sz;

  std::vector<KeyPair>* query_results;
  uint64_t qrvec_offset;

  CachingDirReader<T>* fdcache;
  TaskCompletionTracker* task_tracker;
};

struct KeyPairComparator {
  inline bool operator()(const KeyPair& lhs, const KeyPair& rhs) const {
    return lhs.key < rhs.key;
  }
};

template <typename T>
class RangeReader {
 public:
  RangeReader(const RdbOptions& options)
      : options_(options),
        dir_path_(""),
        fdcache_(options.env),
        manifest_reader_(manifest_),
        num_ranks_(0),
        task_tracker_(options.env),
        logger_(options.env) {
    thpool_ = ThreadPool::NewFixed(options.parallelism);
  }

  ~RangeReader() {
    if (thpool_) {
      delete thpool_;
      thpool_ = nullptr;
    }
  }

  Status ReadManifest(const std::string& dir_path);

  Status QueryParallel(std::vector<Query> qvec) {
    Status s = Status::OK();

    for (size_t i = 0; i < qvec.size(); i++) {
      s = QueryParallel(qvec[i]);
      if (!s.ok()) break;
    }

    return s;
  }

  Status QueryParallel(Query q) {
    return QueryParallel(q.rank, q.epoch, q.range.range_min, q.range.range_max);
  }

  Status QueryParallel(int rank, int epoch, float rbegin, float rend);

  Status QuerySequential(int epoch, float rbegin, float rend);

  Status QueryNaive(int epoch, float rbegin, float rend);

  Status AnalyzeManifest(const std::string& dir_path, bool query = false);

 private:
  static void ManifestReadWorker(void* arg);

  /* query_results: this vector is resized according to match.GetMass()
   * and is also overwritten to, starting from zero */
  Status ReadSSTs(PartitionManifestMatch& match,
                  std::vector<KeyPair>& query_results);

  Status RankwiseReadSSTs(PartitionManifestMatch& match,
                          std::vector<KeyPair>& query_results);

  void ReadBlock(int rank, uint64_t offset, uint64_t size, Slice& slice,
                 std::string& scratch, bool preview = true);

  const RdbOptions& options_;
  std::string dir_path_;
  CachingDirReader<T> fdcache_;
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
