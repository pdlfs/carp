//
// Created by Ankush on 6/18/2021.
//

#pragma once

#include "carp/manifest.h"
#include "common.h"

namespace {
class PMISort {
 public:
  bool operator()(const pdlfs::plfsio::PartitionManifestItem& a,
                  const pdlfs::plfsio::PartitionManifestItem& b) const {
    return (a.epoch < b.epoch) || ((a.epoch == b.epoch && a.offset < b.offset));
  }
};
}  // namespace
namespace pdlfs {
namespace plfsio {
class QueryMatchOptimizer {
 public:
  static Status OptimizeSchedule(PartitionManifestMatch& match) {
    Status s = Status::OK();

    std::map< int, std::vector< PartitionManifestItem > > item_map;
    std::vector< int > all_ranks;
    match.GetUniqueRanks(all_ranks);

    const size_t match_origcnt = match.items_.size();

    for (size_t ri = 0; ri < all_ranks.size(); ri++) {
      int rank = all_ranks[ri];
      std::vector< PartitionManifestItem >& vec = item_map[rank];
      assert(vec.empty());
      match.GetMatchesByRank(rank, vec);
      std::sort(vec.begin(), vec.end(), PMISort());
    }

    match.items_.resize(0);

    for (size_t ri = 0; ri < all_ranks.size(); ri++) {
      int rank = all_ranks[ri];
      std::vector< PartitionManifestItem >& vec = item_map[rank];
      if (!vec.empty()) match.items_.push_back(vec[0]);
    }

    for (size_t ri = 0; ri < all_ranks.size(); ri++) {
      int rank = all_ranks[ri];
      std::vector< PartitionManifestItem >& vec = item_map[rank];
      for (size_t vi = 1; vi < vec.size(); vi++) {
        match.items_.push_back(vec[vi]);
      }
    }

    assert(match.items_.size() == match_origcnt);

    return s;
  }

  static Status Optimize(PartitionManifestMatch& in,
                         PartitionManifestMatch& out) {
    Status s = Status::OK();

    std::vector< int > match_ranks;
    in.GetUniqueRanks(match_ranks);
    std::sort(match_ranks.begin(), match_ranks.end());

    if (!match_ranks.empty()) {
      logv(__LOG_ARGS__, LOG_INFO, "Matching Ranks, Count: %zu (Min: %d, Max: %d)\n",
           match_ranks.size(), match_ranks[0],
           match_ranks[match_ranks.size() - 1]);
    }

    uint64_t key_sz, val_sz;
    in.GetKVSizes(key_sz, val_sz);
    out.SetKVSizes(key_sz, val_sz);

    out.SetDataSize(in.DataSize());

    for (size_t i = 0; i < match_ranks.size(); i++) {
      int rank = match_ranks[i];
      std::vector< PartitionManifestItem > items_orig, items_opt;
      in.GetMatchesByRank(rank, items_orig);
      OptimizeRank(items_orig, items_opt, key_sz + val_sz);

      for (size_t j = 0; j < items_opt.size(); j++) {
        out.AddItem(items_opt[j]);
      }
    }

    logv(__LOG_ARGS__, LOG_INFO,
         "Match Optimization complete, in: %" PRIu64 ", out: %" PRIu64
         " items\n"
         "Size Inflation: %.2f%%",
         in.Size(), out.Size(), out.TotalMass() * 100.0 / in.TotalMass());

    return s;
  }

 private:
  static void OptimizeRank(std::vector< PartitionManifestItem >& items_in,
                           std::vector< PartitionManifestItem >& items_out,
                           uint64_t kvp_sz) {
    PartitionManifestItem cur;
    bool cur_set = false;
    for (size_t i = 0; i < items_in.size(); i++) {
      if (!cur_set) {
        cur = items_in[i];
        cur_set = true;
        continue;
      }

      uint64_t prev_end = cur.offset + (kvp_sz * cur.part_item_count);
      uint64_t cur_start = items_in[i].offset;
      uint64_t intermediate_sz = (cur_start - prev_end);
      if (intermediate_sz < MB(1)) {
        // merge
        assert(intermediate_sz % kvp_sz == 0);
        uint64_t num_extra_items = intermediate_sz / kvp_sz;
        cur.part_item_count += num_extra_items;
        cur.part_item_count += items_in[i].part_item_count;
        cur.part_item_oob += items_in[i].part_item_oob;
      } else {
        // flush
        items_out.push_back(cur);
        cur = items_in[i];
      }
    }

    if (cur_set) {
      items_out.push_back(cur);
    }

    assert(items_out.size() <= items_in.size());
  }
};
}  // namespace plfsio
}  // namespace pdlfs
