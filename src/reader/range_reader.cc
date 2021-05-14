//
// Created by Ankush J on 1/22/21.
//

#include "range_reader.h"

#include "reader_base.h"

namespace pdlfs {
namespace plfsio {

Status RangeReader::ReadManifest(const std::string& dir_path) {
  logger_.RegisterBegin("MFREAD");

  Status s = Status::OK();

  dir_path_ = dir_path;
  fdcache_.ReadDirectory(dir_path_, num_ranks_);

  manifest_reader_.EnableManifestOutput(dir_path);

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

  if (options_.analytics_on) {
    logf(LOG_INFO, "Running analytics...\n");
    /* write manifest to plfs/particle/../../plots */
    std::string exp_path = dir_path + "/../../plots";

    Env* env = options_.env;
    if (!env->FileExists(exp_path.c_str())) {
      s = env->CreateDir(exp_path.c_str());
    }

    if (s.ok()) manifest_.GenOverlapStats(exp_path.c_str(), env);
  }

  return s;
}

Status RangeReader::QueryParallel(int epoch, float rbegin, float rend) {
  logger_.RegisterBegin("SSTREAD");

  PartitionManifestMatch match_obj;
  manifest_.GetOverLappingEntries(epoch, rbegin, rend, match_obj);

  logf(LOG_INFO, "Query Match: %llu SSTs found (%llu items)", match_obj.Size(),
       match_obj.TotalMass());

  match_obj.Print();

  std::vector<KeyPair> query_results;
  RankwiseReadSSTs(match_obj, query_results);

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

#define ITEM(ptile) \
  query_results[((ptile) * (query_results.size() - 1) / 100)].key

  if (!query_results.empty()) {
    logf(LOG_INFO, "Query Results: preview: %.3f %.3f %.3f ... %.3f\n", ITEM(0),
         ITEM(10), ITEM(50), ITEM(100));
  }

  logger_.PrintStats();

  return Status::OK();
}
Status RangeReader::Query(int epoch, float rbegin, float rend) {
  logger_.RegisterBegin("SSTREAD");

  PartitionManifestMatch match_obj;
  manifest_.GetOverLappingEntries(epoch, rbegin, rend, match_obj);
  logf(LOG_INFO, "Query Match: %llu SSTs found (%llu items)", match_obj.Size(),
       match_obj.TotalMass());

  Slice slice;
  std::string scratch;
  for (uint32_t i = 0; i < match_obj.Size(); i++) {
    PartitionManifestItem& item = match_obj[i];
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

  //  item->fdcache->GetFileHandle(item->rank, &src, &src_sz);
  //  RangeReader::ReadFooter(src, src_sz, pf);
  item->fdcache->ReadFooter(item->rank, pf);
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

  logf(LOG_DBUG, "Footer: %u %llu %llu %llu\n", pf.num_epochs, pf.manifest_sz,
       pf.key_sz, pf.val_sz);

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
  work_items.resize(match.Size());
  query_results.resize(match.TotalMass());
  task_tracker_.Reset();

  uint64_t mass_sum = 0;
  uint64_t key_sz, val_sz;
  match.GetKVSizes(key_sz, val_sz);

  for (uint32_t i = 0; i < match.Size(); i++) {
    PartitionManifestItem& item = match[i];
    work_items[i].item = &item;
    work_items[i].key_sz = key_sz;
    work_items[i].val_sz = val_sz;

    work_items[i].query_results = &query_results;
    work_items[i].qrvec_offset = mass_sum;
    mass_sum += item.part_item_count;

    work_items[i].fdcache = &fdcache_;
    work_items[i].task_tracker = &task_tracker_;

    thpool_->Schedule(SSTReadWorker, (void*)&work_items[i]);
  }

  assert(mass_sum == match.TotalMass());

  task_tracker_.WaitUntilCompleted(work_items.size());

  return Status::OK();
}

Status RangeReader::RankwiseReadSSTs(PartitionManifestMatch& match,
                                     std::vector<KeyPair>& query_results) {
  Slice slice;
  std::string scratch;

  task_tracker_.Reset();

  uint64_t mass_sum = 0;
  uint64_t key_sz, val_sz;
  match.GetKVSizes(key_sz, val_sz);

  std::vector<int> ranks;
  match.GetUniqueRanks(ranks);

  std::vector<RankwiseSSTReadWorkItem> work_items;
  work_items.resize(ranks.size());
  query_results.resize(match.TotalMass());

  logf(LOG_INFO, "Matching ranks: %zu\n", ranks.size());

  for (uint32_t i = 0; i < ranks.size(); i++) {
    int rank = ranks[i];
    work_items[i].rank = rank;
    uint64_t mass_rank = match.GetMatchesByRank(rank, work_items[i].wi_vec);
    work_items[i].key_sz = key_sz;
    work_items[i].val_sz = val_sz;

    work_items[i].query_results = &query_results;
    work_items[i].qrvec_offset = mass_sum;
    mass_sum += mass_rank;

    work_items[i].fdcache = &fdcache_;
    work_items[i].task_tracker = &task_tracker_;

    thpool_->Schedule(RankwiseSSTReadWorker, (void*)&work_items[i]);
  }

  assert(mass_sum == match.TotalMass());

  task_tracker_.WaitUntilCompleted(work_items.size());

  return Status::OK();
}

void RangeReader::SSTReadWorker(void* arg) {
  SSTReadWorkItem* wi = static_cast<SSTReadWorkItem*>(arg);
  Status s = Status::OK();

  int rank = wi->item->rank;

  Slice slice;
  std::string scratch;

  const size_t key_sz = wi->key_sz;
  const size_t val_sz = wi->val_sz;
  const size_t item_sz = wi->key_sz + wi->val_sz;
  const size_t sst_sz = wi->item->part_item_count * item_sz;

  std::vector<KeyPair>& qvec = *wi->query_results;
  int qidx = wi->qrvec_offset;

  ReadRequest req;
  req.offset = wi->item->offset;
  req.bytes = sst_sz;
  req.scratch = &scratch[0];

  s = wi->fdcache->Read(rank, req, true);
  if (!s.ok()) {
    logf(LOG_ERRO, "Read Failure");
    return;
  }

  slice = req.slice;
  std::vector<KeyPair> query_results;

  uint64_t block_offset = 0;
  while (block_offset < sst_sz) {
    qvec[qidx].key = DecodeFloat32(&slice[block_offset]);
    // XXX: val?
    block_offset += item_sz;
    qidx++;
  }

  wi->task_tracker->MarkCompleted();
}

void RangeReader::RankwiseSSTReadWorker(void* arg) {
  RankwiseSSTReadWorkItem* wi = static_cast<RankwiseSSTReadWorkItem*>(arg);
  Status s = Status::OK();

  int rank = wi->rank;

  const size_t key_sz = wi->key_sz;
  const size_t val_sz = wi->val_sz;
  const size_t kvp_sz = wi->key_sz + wi->val_sz;

  std::vector<KeyPair>& qvec = *wi->query_results;
  uint64_t qidx = wi->qrvec_offset;

  std::vector<ReadRequest> req_vec;
  req_vec.resize(wi->wi_vec.size());

  std::vector<std::string> scratch_vec;
  scratch_vec.resize(wi->wi_vec.size());

  for (size_t i = 0; i < req_vec.size(); i++) {
    ReadRequest& req = req_vec[i];
    PartitionManifestItem& item = wi->wi_vec[i];
    req.offset = item.offset;
    req.bytes = kvp_sz * item.part_item_count;
    scratch_vec[i].resize(req.bytes);
    req.scratch = &(scratch_vec[i][0]);
    /* we copy this because req-vec gets reordered */
    req.item_count = item.part_item_count;
  }

  s = wi->fdcache->ReadBatch(rank, req_vec);
  if (!s.ok()) {
    logf(LOG_ERRO, "Read Failure");
    return;
  }

  // XXX: don't reuse req_vec, or create copy above
  for (size_t i = 0; i < req_vec.size(); i++) {
    Slice slice = req_vec[i].slice;

    uint64_t keyblk_sz = req_vec[i].item_count * key_sz;
    /* valblk_off is absolute, keyblk_cur is relative */
    uint64_t keyblk_cur = 0;
    uint64_t valblk_cur = req_vec[i].offset + keyblk_sz;

    while (keyblk_cur < keyblk_sz) {
      qvec[qidx].key = DecodeFloat32(&slice[keyblk_cur]);
      qvec[qidx].offset = valblk_cur;

      keyblk_cur += key_sz;
      valblk_cur += val_sz;
      qidx++;
    }
  }

  wi->task_tracker->MarkCompleted();
}

void RangeReader::ReadBlock(int rank, uint64_t offset, uint64_t size,
                            Slice& slice, std::string& scratch, bool preview) {
  Status s = Status::OK();

  scratch.resize(size);
  ReadRequest req;
  req.offset = offset;
  req.bytes = size;
  req.scratch = &scratch[0];
  s = fdcache_.Read(rank, req);
  if (!s.ok()) {
    logf(LOG_ERRO, "Bad Status");
    return;
  }

  slice = req.slice;

  uint64_t num_items = size / 60;
  uint64_t vec_off = query_results_.size();

  uint64_t block_offset = 0;
  while (block_offset < size) {
    KeyPair kp;
    kp.key = DecodeFloat32(&slice[block_offset]);
    // XXX: val?
    query_results_.push_back(kp);

    block_offset += 60;
  }
}
}  // namespace plfsio
}  // namespace pdlfs
