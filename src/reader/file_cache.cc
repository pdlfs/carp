//
// Created by Ankush J on 2/1/21.
//

#include "file_cache.h"

#include "common.h"
#include "range_reader.h"
#include "reader_base.h"

#include <string>

#define MAP_HAS(m, k) (m.find(k) != m.end())

namespace pdlfs {
namespace plfsio {
template <typename T>
Status CachingDirReader<T>::ReadDirectory(std::string dir, int& num_ranks) {
  dir_ = dir;
  uint64_t fsz;

  for (int rank = 0;; rank++) {
    std::string fname = RdbName(dir_, rank);
    bool file_exists = env_->FileExists(fname.c_str());
    if (!file_exists) break;

    Status s = env_->GetFileSize(fname.c_str(), &fsz);
    if (!s.ok()) continue;

    logf(LOG_DBUG, "File: %s, size: %u\n", fname.c_str(), fsz);

    if (!s.ok()) continue;

    FileCacheEntry<T> fe = {rank, false, nullptr, fsz};
    cache_[rank] = fe;
  }

  logf(LOG_INFO, "%u files found.\n", cache_.size());
  num_ranks = cache_.size();
  num_ranks_ = num_ranks;

  return Status::OK();
}

template <typename T>
Status CachingDirReader<T>::ReadFooter(int rank, ParsedFooter& parsed_footer,
                                       uint64_t opt_rdsz) {
  Status s = Status::OK();

  uint64_t fsz;
  s = GetFileSize(rank, &fsz);
  if (!s.ok()) return s;

  ReadRequest req;
  req.offset = fsz - opt_rdsz;
  req.bytes = opt_rdsz;
  parsed_footer.scratch.resize(opt_rdsz);
  req.scratch = &parsed_footer.scratch[0];
  s = Read(rank, req, true);
  if (!s.ok()) return s;

  const uint64_t kFooterSuffixSz = 28;
  uint64_t opt_mfsz = opt_rdsz - kFooterSuffixSz;
  parsed_footer.num_epochs = DecodeFixed32(&req.slice[opt_mfsz]);
  parsed_footer.manifest_sz = DecodeFixed64(&req.slice[opt_mfsz + 4]);
  parsed_footer.key_sz = DecodeFixed64(&req.slice[opt_mfsz + 12]);
  parsed_footer.val_sz = DecodeFixed64(&req.slice[opt_mfsz + 20]);

  logf(LOG_DBG2, "Optimistic Read: %llu, Actual Size: %llu\n", opt_rdsz,
       parsed_footer.manifest_sz);

  ParsedFooter& pf = parsed_footer;
  logf(LOG_DBUG, "Footer: %u %llu %llu %llu\n", pf.num_epochs, pf.manifest_sz,
       pf.key_sz, pf.val_sz);

  if (opt_mfsz < pf.manifest_sz) {
    logf(LOG_DBG2, "Optimistic reading insufficient. Reading again.");
  } else {
    parsed_footer.manifest_data = req.slice;
    parsed_footer.manifest_data.remove_prefix(opt_mfsz -
                                              parsed_footer.manifest_sz);
    return s;
  }

  parsed_footer.scratch.resize(parsed_footer.manifest_sz);
  req.offset = fsz - parsed_footer.manifest_sz - kFooterSuffixSz;
  req.bytes = parsed_footer.manifest_sz;
  req.scratch = &parsed_footer.scratch[0];

  s = Read(rank, req, true);
  if (s.ok()) {
    parsed_footer.manifest_data = req.slice;
  }

  return s;
}

template <typename T>
Status CachingDirReader<T>::GetFileSize(int rank, uint64_t* fsz) {
  if (!MAP_HAS(cache_, rank)) {
    return Status::InvalidArgument("rank not found");
  }

  *fsz = cache_[rank].fsz;
  return Status::OK();
}

template <>
Status CachingDirReader<RandomAccessFile>::OpenFileHandle(int rank,
                                                          RandomAccessFile** fh,
                                                          uint64_t* fsz) {
  if (!MAP_HAS(cache_, rank)) return Status::NotFound("Key not found");
  FileCacheEntry<RandomAccessFile>& e = cache_[rank];

  if (e.is_open) {
    delete e.fh;
    e.is_open = false;
  }

  std::string fname = RdbName(dir_, rank);
  uint64_t file_sz;
  env_->GetFileSize(fname.c_str(), &file_sz);

  Status s = Status::OK();
  RandomAccessFile* rand_fh;
  s = env_->NewRandomAccessFile(fname.c_str(), &rand_fh);
  if (!s.ok()) return s;

  cache_[rank].is_open = true;
  *fh = rand_fh;
  *fsz = cache_[rank].fsz;

  return s;
}

template <>
Status CachingDirReader<SequentialFile>::OpenFileHandle(int rank,
                                                        SequentialFile** fh,
                                                        uint64_t* fsz) {
  if (!MAP_HAS(cache_, rank)) return Status::NotFound("Key not found");
  FileCacheEntry<SequentialFile>& e = cache_[rank];

  if (e.is_open) {
    delete e.fh;
    e.is_open = false;
  }

  std::string fname = RdbName(dir_, rank);
  uint64_t file_sz;
  env_->GetFileSize(fname.c_str(), &file_sz);

  Status s = Status::OK();
  SequentialFile* seq_fh;
  s = env_->NewSequentialFile(fname.c_str(), &seq_fh);
  if (!s.ok()) return s;

  cache_[rank].is_open = true;
  *fh = seq_fh;
  *fsz = cache_[rank].fsz;

  return s;
}

template <typename T>
Status CachingDirReader<T>::GetFileHandle(int rank, T** fh, uint64_t* fsz,
                                          bool force_reopen) {
  if (!MAP_HAS(cache_, rank)) return Status::InvalidArgument("Rank not found");

  Status s = Status::OK();

  bool is_open = cache_[rank].is_open;
  bool to_open = (!is_open || force_reopen);

  if (to_open) {
    s = OpenFileHandle(rank, fh, fsz);
    cache_[rank].fh = *fh;
    cache_[rank].fsz = *fsz;
  } else {
    *fh = cache_[rank].fh;
    *fsz = cache_[rank].fsz;
  }

  return s;
}

template <>
Status CachingDirReader<RandomAccessFile>::Read(int rank, ReadRequest& request,
                                                bool force_reopen) {
  Status s = Status::OK();

  RandomAccessFile* fh;
  uint64_t fsz;

  s = GetFileHandle(rank, &fh, &fsz, force_reopen);
  if (!s.ok()) return s;

  s = fh->Read(request.offset, request.bytes, &request.slice, request.scratch);

  return s;
}

template <>
Status CachingDirReader<SequentialFile>::Read(int rank, ReadRequest& request,
                                              bool force_reopen) {
  Status s = Status::OK();

  SequentialFile* fh;
  uint64_t fsz;

  s = GetFileHandle(rank, &fh, &fsz, force_reopen);
  if (!s.ok()) return s;

  if (request.offset) {
    s = fh->Skip(request.offset);
    if (!s.ok()) return s;
  }

  s = fh->Read(request.bytes, &request.slice, request.scratch);

  return s;
}

template <>
Status CachingDirReader<RandomAccessFile>::ReadBatch(
    int rank, std::vector<ReadRequest>& requests) {
  Status s = Status::OK();

  for (size_t i = 0; i < requests.size(); i++) {
    s = Read(rank, requests[i], false);
    if (!s.ok()) return s;
  }

  return s;
}

template <>
Status CachingDirReader<SequentialFile>::ReadBatch(
    int rank, std::vector<ReadRequest>& requests) {
  Status s = Status::OK();

  SequentialFile* fh;
  uint64_t fsz;
  s = GetFileHandle(rank, &fh, &fsz, true);
  if (!s.ok()) return s;

  std::sort(requests.begin(), requests.end());

  uint64_t file_cursor = 0;
  for (size_t i = 0; i < requests.size(); i++) {
    ReadRequest r = requests[i];
    uint64_t abs_offset = r.offset;
    uint64_t rel_offset = abs_offset - file_cursor;
    if (rel_offset < 0) {
      s = Status::InvalidArgument("Overlapping batch reads not supported");
      break;
    }

    r.offset = rel_offset;

    s = Read(rank, r, false);
    if (!s.ok()) break;

    requests[i].slice = r.slice;
    file_cursor += rel_offset + r.bytes;
  }

  return s;
}

template class CachingDirReader<RandomAccessFile>;
template class CachingDirReader<SequentialFile>;

}  // namespace plfsio
}  // namespace pdlfs