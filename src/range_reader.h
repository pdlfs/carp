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
  std::string value;
};

struct ParsedFooter {
  Slice manifest_data;
  std::string scratch;
  uint32_t num_epochs;
  uint64_t manifest_sz;
  uint64_t key_sz;
  uint64_t val_sz;
};

struct ManifestReadWorkItem {
  int rank;
  CachingDirReader<RandomAccessFile>* fdcache;
  TaskCompletionTracker* task_tracker;
  PartitionManifestReader* manifest_reader;
};

struct SSTReadWorkItem {
  PartitionManifestItem* item;
  size_t key_sz;
  size_t val_sz;

  std::vector<KeyPair>* query_results;
  uint64_t qrvec_offset;

  CachingDirReader<RandomAccessFile>* fdcache;
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

  Status ReadManifest(std::string dir_path);

  Status QueryParallel(int epoch, float rbegin, float rend);

  Status Query(int epoch, float rbegin, float rend);

 private:
  static void ManifestReadWorker(void* arg);

  static Status ReadFooter(RandomAccessFile* fh, uint64_t fsz,
                           ParsedFooter& pf);

  Status ReadSSTs(PartitionManifestMatch& match, std::vector<KeyPair>& query_results);

  static void SSTReadWorker(void * arg);

  void ReadBlock(int rank, uint64_t offset, uint64_t size, Slice& slice,
                 std::string& scratch, bool preview = true) {
    scratch.resize(size);
    RandomAccessFile* src;
    uint64_t src_sz;
    fdcache_.GetFileHandle(rank, &src, &src_sz);
    src->Read(offset, size, &slice, &scratch[0]);

    uint64_t num_items = size / 60;
    uint64_t vec_off = query_results_.size();

    uint64_t block_offset = 0;
    while (block_offset < size) {
      KeyPair kp;
      kp.key = DecodeFloat32(&slice[block_offset]);
      //            kp.value = std::string(&slice[block_offset + 4], 56);
      kp.value = "";
      query_results_.push_back(kp);

      block_offset += 60;
    }
  }

  const RdbOptions& options_;
  std::string dir_path_;
  CachingDirReader<RandomAccessFile> fdcache_;
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
