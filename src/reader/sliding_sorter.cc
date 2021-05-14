//
// Created by Ankush on 5/13/2021.
//

#include "sliding_sorter.h"

#include "file_cache.h"

namespace pdlfs {
namespace plfsio {
size_t SlidingSorter::val_sz_;

Status SlidingSorter::AddItem(const PartitionManifestItem& item) {
  if (num_ranks_ == 0) {
    rank_cursors_.resize(fdcache_.NumRanks(), 0);
  } else {
    assert(num_ranks_ == fdcache_.NumRanks());
  }

  Status s = Status::OK();
  // key is assumed to be float
  size_t item_sz = val_sz_ + sizeof(float);
  assert(item_sz >= sizeof(float));
  ReadRequest req;
  req.bytes = item.part_item_count * item_sz;

//  char buf[req.bytes];
  char *buf = new char[req.bytes];
  req.scratch = &buf[0];

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

  cursor += req.bytes;

  AddSST(req.slice, item_sz, item.part_item_count);
  delete buf;
  return s;
}
}  // namespace plfsio
}  // namespace pdlfs