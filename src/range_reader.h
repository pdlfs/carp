//
// Created by Ankush J on 1/22/21.
//

#pragma once

#include "carp/coding_float.h"
#include "carp/manifest.h"
#include "common.h"
#include "file_cache.h"
#include "perf.h"
#include "tbb_check.h"

#include "pdlfs-common/env.h"

#if defined(PDLFS_TBB)
#include <execution>
#endif
#include <map>

namespace pdlfs {
namespace plfsio {
class PartitionManifestReader {
 public:
  PartitionManifestReader() : mass_total_(0) {}

  Status ReadManifest(int rank, Slice& footer_data, uint64_t footer_sz);

  int GetOverLappingEntries(float point, PartitionManifestMatch& match);

  int GetOverLappingEntries(int epoch, float range_begin, float range_end,
                            PartitionManifestMatch& match);

 private:
  void ReadFooterEpoch(int epoch, int rank, Slice& data, uint64_t epoch_offset,
                       uint64_t epoch_sz);

  static void ComputeInternalOffsets(const size_t entry_sizes[],
                                     size_t offsets[], int num_entries,
                                     size_t& item_sz) {
    item_sz = 0;
    for (int i = 0; i < num_entries; i++) {
      offsets[i] = item_sz;
      item_sz += entry_sizes[i];
    }
  }

 private:
  std::vector<PartitionManifestItem> items_;
  uint64_t mass_total_;
};

struct ParsedFooter {
  Slice manifest_data;
  uint32_t num_epochs;
  uint64_t manifest_sz;
  uint64_t key_sz;
  uint64_t val_sz;
};

struct KeyPair {
  float key;
  std::string value;
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
        reader_(options.env),
        num_ranks_(0),
        logger_(options.env) {}

  Status Read(std::string dir_path);

  Status Query(int epoch, float rbegin, float rend) {
    logger_.RegisterBegin("SSTREAD");

    PartitionManifestMatch match_obj;
    manifest_reader_.GetOverLappingEntries(epoch, rbegin, rend, match_obj);
    logf(LOG_INFO, "Query Match: %llu SSTs found (%llu items)",
         match_obj.items.size(), match_obj.mass_total);

    Slice slice;
    std::string scratch;
    for (uint32_t i = 0; i < match_obj.items.size(); i++) {
      PartitionManifestItem& item = match_obj.items[i];
      logf(LOG_DBUG, "Item Rank: %d, Offset: %llu\n", item.rank, item.offset);
      ReadBlock(item.rank, item.offset, item.part_item_count * 60, slice,
                scratch);
    }

    logger_.RegisterEnd("SSTREAD");

    logger_.RegisterBegin("SORT");
#if defined(PDLFS_TBB)
    std::sort(std::execution::par, query_results_.begin(), query_results_.end(),
              KeyPairComparator());
#else
    std::sort(query_results_.begin(), query_results_.end(),
              KeyPairComparator());
#endif
    logger_.RegisterEnd("SORT");

    logf(LOG_INFO, "Query Results: %zu elements found\n",
         query_results_.size());

    logger_.PrintStats();

    return Status::OK();
  }

  Status ReadFooter(RandomAccessFile* fh, uint64_t fsz, ParsedFooter& pf);

  void ReadBlock(int rank, uint64_t offset, uint64_t size, Slice& slice,
                 std::string& scratch, bool preview = true) {
    scratch.resize(size);
    RandomAccessFile* src;
    uint64_t src_sz;
    reader_.GetFileHandle(rank, &src, &src_sz);
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

 private:
  const RdbOptions& options_;
  std::string dir_path_;
  CachingDirReader reader_;
  PartitionManifestReader manifest_reader_;
  int num_ranks_;
  std::vector<KeyPair> query_results_;

  RangeReaderPerfLogger logger_;
};
}  // namespace plfsio
}  // namespace pdlfs