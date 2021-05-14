//
// Created by Ankush on 5/14/2021.
//

#include "reader_base.h"

namespace pdlfs {
namespace plfsio {
void ReaderBase::ReadManifests() {
  fdcache_.ReadDirectory(options_.data_path, num_ranks_);
  // XXX: tmp
  num_ranks_ = 8;

  rank_cursors_.resize(fdcache_.NumRanks(), 0);

  for (int rank = 0; rank < num_ranks_; rank++) {
    ParsedFooter pf;
    fdcache_.ReadFooter(rank, pf);
    manifest_reader_.UpdateKVSizes(pf.key_sz, pf.val_sz);
    manifest_reader_.ReadManifest(rank, pf.manifest_data, pf.manifest_sz);

    logf(LOG_DBUG, "[MFREAD] Rank %d, items: %" PRIu64 " (epochs: %u)\n", rank,
         pf.manifest_sz, pf.num_epochs);
  }

  manifest_.GetKVSizes(key_sz_, val_sz_);
}

Status ReaderBase::ReadSST(const PartitionManifestItem& item, Slice& sst,
                           char* scratch) {
  Status s = Status::OK();
  // key is assumed to be float
  size_t item_sz = val_sz_ + sizeof(float);
  assert(item_sz >= sizeof(float));
  ReadRequest req;
  req.bytes = item.part_item_count * item_sz;

  req.scratch = scratch;

  bool reopen = false;
  size_t& cursor = rank_cursors_[item.rank];

  /* reopen every file on first read, just to be safe
   * we assume fdcache doesn't serve any other reader
   * as sliding_sorter is reading
   */
  if (cursor == 0) reopen = true;

  if (item.offset < cursor) {
    reopen = true;  // random read in seq file; move cursor to offset 0
    cursor = 0;
  }

  // relative offset
  req.offset = item.offset - cursor;
  s = fdcache_.Read(item.rank, req, reopen);
  if (!s.ok()) return s;

  sst = req.slice;
  cursor += req.bytes;

  return s;
}
}  // namespace plfsio
}  // namespace pdlfs