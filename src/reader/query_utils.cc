//
// Created by Ankush on 5/26/2021.
//

#include "query_utils.h"

#include <unistd.h>
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)

namespace pdlfs {
namespace plfsio {
Status QueryUtils::SummarizeManifest(PartitionManifest& manifest) {
  Status s = Status::OK();

  manifest.SortByKey();

  int epcnt;
  s = manifest.GetEpochCount(epcnt);
  if (!s.ok()) return s;

  logf(LOG_INFO, "Total Epochs: %d\n", epcnt);

  for (int ep = 0; ep < epcnt; ep++) {
    Range ep_range;
    s = manifest.GetEpochRange(ep, ep_range);
    if (!s.ok()) return s;

    uint64_t ep_itemcnt;
    s = manifest.GetEpochMass(ep, ep_itemcnt);
    if (!s.ok()) return s;

    if (ep_itemcnt == 0u) continue;

    logf(LOG_INFO, "Epoch %d: %.1f to %.1f (%" PRIu64 " items)", ep,
         ep_range.range_min, ep_range.range_max, ep_itemcnt);

    std::string print_buf_concat;
    for (float qpnt = 0.01; qpnt < 2; qpnt += 0.25) {
      PartitionManifestMatch match;
      manifest.GetOverlappingEntries(ep, qpnt, match);

      char print_buf[64];
      snprintf(print_buf, 64, "%.3f (%.3f%%), ", qpnt,
               match.GetSelectivity() * 100);

      print_buf_concat += print_buf;
      if (print_buf_concat.size() > 60u) {
        logf(LOG_INFO, "%s", print_buf_concat.c_str());
        print_buf_concat = "";
      }
    }

    if (print_buf_concat.size()) {
      logf(LOG_INFO, "%s", print_buf_concat.c_str());
      print_buf_concat = "";
    }
  }

  return s;
}

Status QueryUtils::GenQueryPlan(PartitionManifest& manifest,
                                std::vector<Query>& queries) {
  Status s = Status::OK();

  int epcnt;
  s = manifest.GetEpochCount(epcnt);
  if (!s.ok()) return s;

  std::vector<float> overlaps;

  for (int e = 0; e < epcnt; e++) {
    uint64_t epmass;
    s = manifest.GetEpochMass(e, epmass);
    if (epmass < 1000ull) continue;

    s = GenQueries(manifest, e, queries, overlaps, /* olapmax */ PCT(2),
                   /* dist */ PCT(0.2),
                   /* count */ 3);
    if (!s.ok()) return s;
  }

  return s;
}

template <>
void QueryUtils::ThreadSafetyWarning<SequentialFile>() {
  logf(LOG_WARN,
       "FileCache/SequentialFile is not thread-safe when intra-rank "
       "parallelism is used!");
}

template <>
void QueryUtils::ThreadSafetyWarning<RandomAccessFile>() {}

template <typename T>
void QueryUtils::SSTReadWorker(void* arg) {
  SSTReadWorkItem<T>* wi = static_cast<SSTReadWorkItem<T>*>(arg);
  Status s = Status::OK();

  pid_t tid = gettid();

  int rank = wi->item->rank;

  Slice slice;
  const size_t key_sz = wi->key_sz;
  const size_t val_sz = wi->val_sz;
  const size_t kvp_sz = wi->key_sz + wi->val_sz;
  const size_t sst_sz = kvp_sz * wi->item->part_item_count;
  const size_t keyblk_sz = key_sz * wi->item->part_item_count;

  std::string scratch;
  scratch.resize(sst_sz);

  std::vector<KeyPair>& qvec = *wi->query_results;
  int qidx = wi->qrvec_offset;

  ReadRequest req;
  req.offset = wi->item->offset;
  req.bytes = sst_sz;
  // XXX: tmp
  req.bytes = sst_sz / kvp_sz * key_sz;
  req.scratch = &scratch[0];

  int req_id = wi->task_tracker->MarkBegin(tid);

  s = wi->fdcache->Read(rank, req, /* force-reopen */ false);
  if (!s.ok()) {
    logf(LOG_ERRO, "Read Failure");
    return;
  }

  wi->task_tracker->MarkIOCompleted(req_id);

  slice = req.slice;

  uint64_t keyblk_cur = 0;
  uint64_t valblk_cur = req.offset + keyblk_sz;

  while (keyblk_cur < keyblk_sz) {
    qvec[qidx].key = DecodeFloat32(&slice[keyblk_cur]);
    qvec[qidx].offset = valblk_cur;

    keyblk_cur += key_sz;
    valblk_cur += val_sz;
    qidx++;
  }

  wi->task_tracker->MarkCompleted(req_id);
}

template <typename T>
void QueryUtils::RankwiseSSTReadWorker(void* arg) {
  RankwiseSSTReadWorkItem<T>* wi =
      static_cast<RankwiseSSTReadWorkItem<T>*>(arg);
  Status s = Status::OK();

  pid_t tid = gettid();

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

  int req_id = wi->task_tracker->MarkBegin(tid);

  s = wi->fdcache->ReadBatch(rank, req_vec);
  if (!s.ok()) {
    logf(LOG_ERRO, "Read Failure");
    return;
  }

  wi->task_tracker->MarkIOCompleted(req_id);

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

  wi->task_tracker->MarkCompleted(req_id);
}

Status QueryUtils::GenQueries(PartitionManifest& manifest, int epoch,
                              std::vector<Query>& queries,
                              std::vector<float>& overlaps, float max_overlap,
                              float min_dist, int count_within_dist) {
  Status s = Status::OK();
  Range r;
  s = manifest.GetEpochRange(epoch, r);
  if (!s.ok()) return s;

  float rbeg = r.range_min;
  float rend = rbeg + 0.001f;

  while (rend < r.range_max) {
    Query q(epoch, rbeg, rend);
    PartitionManifestMatch match;

    manifest.GetOverlappingEntries(q, match);

    float query_sel = match.GetSelectivity();

    int num_in_range = ItemsWithinRange(overlaps, query_sel, min_dist);
    bool query_useful = query_sel < max_overlap;
    bool query_needed = num_in_range < count_within_dist;

    if (query_needed and query_useful) {
      queries.push_back(q);
      overlaps.push_back(query_sel);
    }

    rend += 0.1;

    if (query_needed or (not query_useful)) {
      /* if current query range is not useful, it's pointless to keep
       * extending it so we move on */
      rbeg = rend + 0.1;
      rend = rbeg + 0.001;
    }
  }

  return s;
}

template void QueryUtils::SSTReadWorker<RandomAccessFile>(void* arg);
template void QueryUtils::SSTReadWorker<SequentialFile>(void* arg);

template void QueryUtils::RankwiseSSTReadWorker<RandomAccessFile>(void* arg);
template void QueryUtils::RankwiseSSTReadWorker<SequentialFile>(void* arg);
}  // namespace plfsio
}  // namespace pdlfs
