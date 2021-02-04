//
// Created by Ankush J on 1/22/21.
//

#include "range_reader.h"

namespace pdlfs {
namespace plfsio {

Status RangeReader::ReadManifest(std::string dir_path) {
  logger_.RegisterBegin("MFREAD");

  dir_path_ = dir_path;
  fdcache_.ReadDirectory(dir_path_, num_ranks_);

  RandomAccessFile* src;
  std::vector<ManifestReadWorkItem> work_items;
  work_items.resize(num_ranks_);

  task_tracker_.Reset();

  for (int rank = 0; rank < num_ranks_; rank++) {
    work_items[rank].rank = rank;
    work_items[rank].fdcache = &fdcache_;
    work_items[rank].task_tracker = &task_tracker_;
    work_items[rank].manifest_reader = &manifest_reader_;
    thpool_->Schedule(ManifestReadWorker, (void*)(&work_items[rank]));
  }

  task_tracker_.WaitUntilCompleted(num_ranks_);

  uint64_t key_sz, val_sz;
  manifest_.GetKVSizes(key_sz, val_sz);
  logf(LOG_INFO, "Key/Value Sizes: %lu/%lu\n", key_sz, val_sz);

  logger_.RegisterEnd("MFREAD");

  return Status::OK();
}

Status RangeReader::QueryParallel(int epoch, float rbegin, float rend) {
  logger_.RegisterBegin("SSTREAD");

  PartitionManifestMatch match_obj;
  manifest_.GetOverLappingEntries(epoch, rbegin, rend, match_obj);

  logf(LOG_INFO, "Query Match: %llu SSTs found (%llu items)",
       match_obj.items.size(), match_obj.mass_total);

  std::vector<KeyPair> query_results;
  ReadSSTs(match_obj, query_results);

  logger_.RegisterEnd("SSTREAD");
  logger_.RegisterBegin("SORT");

#if defined(PDLFS_TBB)
  std::sort(std::execution::par, query_results.begin(), query_results.end(),
            KeyPairComparator());
#else
  std::sort(query_results.begin(), query_results.end(), KeyPairComparator());
#endif
  logger_.RegisterEnd("SORT");

  logf(LOG_INFO, "Query Results: %zu elements found\n", query_results.size());

#define ITEM(idx) query_results[(idx)].key
  logf(LOG_INFO, "Query Results: preview: %.3f %.3f %.3f...\n", ITEM(5000),
       ITEM(6500), ITEM(8000));
  logger_.PrintStats();

  return Status::OK();
}
Status RangeReader::Query(int epoch, float rbegin, float rend) {
  logger_.RegisterBegin("SSTREAD");

  PartitionManifestMatch match_obj;
  manifest_.GetOverLappingEntries(epoch, rbegin, rend, match_obj);
  logf(LOG_INFO, "Query Match: %llu SSTs found (%llu items)",
       match_obj.items.size(), match_obj.mass_total);

  Slice slice;
  std::string scratch;
  for (uint32_t i = 0; i < match_obj.items.size(); i++) {
    PartitionManifestItem& item = match_obj.items[i];
    // logf(LOG_DBUG, "Item Rank: %d, Offset: %llu\n", item.rank,
    // item.offset);
    ReadBlock(item.rank, item.offset, item.part_item_count * 60, slice,
              scratch);
  }

  logger_.RegisterEnd("SSTREAD");

  logger_.RegisterBegin("SORT");
#if defined(PDLFS_TBB)
  std::sort(std::execution::par, query_results_.begin(), query_results_.end(),
            KeyPairComparator());
#else
  std::sort(query_results_.begin(), query_results_.end(), KeyPairComparator());
#endif
  logger_.RegisterEnd("SORT");

  logf(LOG_INFO, "Query Results: %zu elements found\n", query_results_.size());

  logger_.PrintStats();

  return Status::OK();
}

void RangeReader::ManifestReadWorker(void* arg) {
  ManifestReadWorkItem* item = static_cast<ManifestReadWorkItem*>(arg);

  RandomAccessFile* src;
  uint64_t src_sz;

  ParsedFooter pf;

  item->fdcache->GetFileHandle(item->rank, &src, &src_sz);
  RangeReader::ReadFooter(src, src_sz, pf);
  item->manifest_reader->UpdateKVSizes(pf.key_sz, pf.val_sz);
  item->manifest_reader->ReadManifest(item->rank, pf.manifest_data,
                                      pf.manifest_sz);
  item->task_tracker->MarkCompleted();
}

Status RangeReader::ReadFooter(RandomAccessFile* fh, uint64_t fsz,
                               ParsedFooter& pf) {
  static const uint64_t footer_sz = 28;
  Slice s;
  std::string scratch;
  scratch.resize(footer_sz);
  Status status = fh->Read(fsz - footer_sz, footer_sz, &s, &scratch[0]);

  pf.num_epochs = DecodeFixed32(&s[0]);
  pf.manifest_sz = DecodeFixed64(&s[4]);
  pf.key_sz = DecodeFixed64(&s[12]);
  pf.val_sz = DecodeFixed64(&s[20]);

  // logf(LOG_DBUG, "Footer: %u %llu %llu %llu\n", pf.num_epochs,
  // pf.manifest_sz, pf.key_sz, pf.val_sz);

  scratch.resize(pf.manifest_sz);
  status = fh->Read(fsz - pf.manifest_sz - footer_sz, pf.manifest_sz,
                    &pf.manifest_data, &scratch[0]);

  return status;
}

Status RangeReader::ReadSSTs(PartitionManifestMatch& match,
                             std::vector<KeyPair>& query_results) {
  Slice slice;
  std::string scratch;

  std::vector<SSTReadWorkItem> work_items;
  work_items.resize(match.items.size());
  query_results.resize(match.mass_total);
  task_tracker_.Reset();

  uint64_t mass_sum = 0;

  for (uint32_t i = 0; i < match.items.size(); i++) {
    PartitionManifestItem& item = match.items[i];
    work_items[i].item = &item;
    work_items[i].key_sz = match.key_sz;
    work_items[i].val_sz = match.val_sz;

    work_items[i].query_results = &query_results;
    work_items[i].qrvec_offset = mass_sum;
    mass_sum += item.part_item_count;

    work_items[i].fdcache = &fdcache_;
    work_items[i].task_tracker = &task_tracker_;

    thpool_->Schedule(SSTReadWorker, (void*)&work_items[i]);
  }

  assert(mass_sum == match.mass_total);

  task_tracker_.WaitUntilCompleted(work_items.size());

  return Status::OK();
}

void RangeReader::SSTReadWorker(void* arg) {
  SSTReadWorkItem* wi = static_cast<SSTReadWorkItem*>(arg);

  int rank;
  RandomAccessFile* src;
  uint64_t src_sz;

  Status s = wi->fdcache->GetFileHandle(rank, &src, &src_sz);
  assert(s.ok());

  Slice slice;
  std::string scratch;

  const size_t key_sz = wi->key_sz;
  const size_t val_sz = wi->val_sz;
  const size_t item_sz = wi->key_sz + wi->val_sz;
  const size_t sst_sz = wi->item->part_item_count * item_sz;

  std::vector<KeyPair>& qvec = *wi->query_results;
  int qidx = wi->qrvec_offset;

  src->Read(wi->item->offset, sst_sz, &slice, &scratch[0]);
  std::vector<KeyPair> query_results;

  uint64_t block_offset = 0;
  while (block_offset < sst_sz) {
    qvec[qidx].key = DecodeFloat32(&slice[block_offset]);
    qvec[qidx].value = std::string(&slice[block_offset + key_sz], val_sz);
    //    kp.value = "";

    block_offset += item_sz;
    qidx++;
  }

  wi->task_tracker->MarkCompleted();
}
}  // namespace plfsio
}  // namespace pdlfs
