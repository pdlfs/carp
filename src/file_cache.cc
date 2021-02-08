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

Status CachingDirReader::ReadFooter(int rank, ParsedFooter& parsed_footer,
                                    uint64_t opt_rdsz) {
  Status s = Status::OK();

  //  RandomAccessFile* fh;
  SequentialFile* sfh;
  uint64_t fsz;
  //  s = GetFileHandle(rank, &fh, &fsz);
  s = OpenSeqFileHandle(rank, &sfh, &fsz);
  if (!s.ok()) return s;

  Slice slice;
  parsed_footer.scratch.resize(opt_rdsz);
  //  fh->Read(fsz - opt_rdsz, opt_rdsz, &slice, &parsed_footer.scratch[0]);
  sfh->Skip(fsz - opt_rdsz);
  sfh->Read(opt_rdsz, &slice, &parsed_footer.scratch[0]);

  uint64_t opt_mfsz = opt_rdsz - 28;
  parsed_footer.num_epochs = DecodeFixed32(&slice[opt_mfsz]);
  parsed_footer.manifest_sz = DecodeFixed64(&slice[opt_mfsz + 4]);
  parsed_footer.key_sz = DecodeFixed64(&slice[opt_mfsz + 12]);
  parsed_footer.val_sz = DecodeFixed64(&slice[opt_mfsz + 20]);

  logf(LOG_DBG2, "Optimistic Read: %llu, Actual Size: %llu\n", opt_rdsz,
       parsed_footer.manifest_sz);

  ParsedFooter& pf = parsed_footer;
  logf(LOG_DBUG, "Footer: %u %llu %llu %llu\n", pf.num_epochs, pf.manifest_sz,
       pf.key_sz, pf.val_sz);

  if (opt_mfsz < pf.manifest_sz) {
    logf(LOG_DBG2, "Optimistic reading insufficient. Reading again.");
  } else {
    parsed_footer.manifest_data = slice;
    parsed_footer.manifest_data.remove_prefix(opt_mfsz -
                                              parsed_footer.manifest_sz);
    return s;
  }

  parsed_footer.scratch.resize(parsed_footer.manifest_sz);

  s = OpenSeqFileHandle(rank, &sfh, &fsz);
  if (!s.ok()) return s;

  //  s = fh->Read(fsz - pf.manifest_sz, pf.manifest_sz, &pf.manifest_data,
  //               &parsed_footer.scratch[0]);
  sfh->Skip(fsz - parsed_footer.manifest_sz);
  sfh->Read(parsed_footer.manifest_sz, &parsed_footer.manifest_data,
            &parsed_footer.scratch[0]);

  return s;
}

Status CachingDirReader::OpenFileHandle(int rank, SequentialFile** sfh,
                                        RandomAccessFile** rfh, uint64_t* fsz) {
  if (!MAP_HAS(cache_, rank)) return Status::NotFound("Key not found");

  FileCacheEntry& e = cache_[rank];
  if (e.is_open && e.rh) {
    delete e.rh;
    e.rh = nullptr;
    e.is_open = false;
  } else if (e.is_open && e.sh) {
    delete e.sh;
    e.sh = nullptr;
    e.is_open = false;
  } else if (e.is_open) {
    return Status::InvalidArgument("Invalid cache state");
  }

  std::string fname = RdbName(dir_, rank);
  uint64_t file_sz;
  env_->GetFileSize(fname.c_str(), &file_sz);

  Status s = Status::OK();
  if (sfh) {
    SequentialFile* seq_fh;
    s = env_->NewSequentialFile(fname.c_str(), &seq_fh);
    cache_[rank].sh = seq_fh;
    /* Even though these are set, they are valid only if return code is OK */
    *sfh = seq_fh;
  } else if (rfh) {
    RandomAccessFile* rand_fh;
    s = env_->NewRandomAccessFile(fname.c_str(), &rand_fh);
    cache_[rank].rh = rand_fh;
    /* Even though these are set, they are valid only if return code is OK */
    *rfh = rand_fh;
  } else {
    s = Status::InvalidArgument("Both sfh and rfh can not be set.");
  }

  if (!s.ok()) return s;

  cache_[rank].is_open = true;
  *fsz = cache_[rank].fsz;

  return s;
}

}  // namespace plfsio
}  // namespace pdlfs