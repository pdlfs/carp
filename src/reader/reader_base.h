//
// Created by Ankush on 5/13/2021.
//

#pragma once

#include "common.h"
#include "file_cache.h"
#include "manifest_reader.h"

#include <carp/manifest.h>
#include <pdlfs-common/env.h>

namespace pdlfs {
namespace plfsio {

struct ParsedFooter {
  Slice manifest_data;
  std::string scratch;
  uint32_t num_epochs;
  uint64_t manifest_sz;
  uint64_t key_sz;
  uint64_t val_sz;
};

class ReaderBase {
 public:
  ReaderBase(const RdbOptions options)
      : options_(options),
        num_ranks_(0),
        fdcache_(options.env),
        manifest_reader_(manifest_),
        key_sz_(0),
        val_sz_(0) {}

 protected:
  void ReadManifests();

  /*
   * scratch: a buffer of item.bytes size
   */
  Status ReadSST(const PartitionManifestItem& item, Slice& sst, char* scratch);

  const RdbOptions options_;
  int num_ranks_;
  CachingDirReader<SequentialFile> fdcache_;
  PartitionManifest manifest_;
  PartitionManifestReader manifest_reader_;

  std::vector<size_t> rank_cursors_;
  size_t key_sz_;
  size_t val_sz_;
};
}  // namespace plfsio
}  // namespace pdlfs
