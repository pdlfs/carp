#pragma once

#include "common.h"

#include <pdlfs-common/port.h>

namespace pdlfs {
namespace carp {
class PivotUtils;
class CarpBase {
 public:
  static constexpr uint32_t kMaxPivots = 256;

 private:
  /* Return true if this is the first time you're blocking
   * Implication: you use OOBs, and the first time you're
   * blocking to renegotiate, all your data is in oob_left.
   * This assumption is used for pivot calculations
   *
   * For CARP, this is simply overridden as:
   * return mts_mgr_.FirstBlock();
   */
  virtual bool FirstBlock() = 0;

  /* Override to return all keys in OOB, such that
   * oob_left has all keys < range_min_
   * oob_right has all keys > range_max_
   * if FirstBlock() returns true, neither range_min_ or range_max are defined
   * and all OOB'ed keys go to oob_left
   *
   * Override as nop if not using OOBs.
   *
   * For CARP, this is overridden as:
   * return oob_buffer_.GetPartitionedProps(oob_left, oob_right)
   */
  virtual bool GetPartitionedOOBProps(std::vector<float> oob_left,
                                      std::vector<float> oob_right) = 0;

  virtual void UpdatePivots(double* pivots, int num_pivots) {
    mutex_.AssertHeld();

    float pivot_min = pivots[0];
    float pivot_max = pivots[num_pivots - 1];

    if (!FirstBlock()) {
      assert(FloatUtils::float_lte(pivot_min, range_min_));
      assert(FloatUtils::float_gte(pivot_max, range_max_));
    }

    range_min_ = pivot_min;
    range_max_ = pivot_max;

    std::copy(pivots, pivots + num_pivots, rank_bins_.begin());
    std::fill(rank_counts_.begin(), rank_counts_.end(), 0);

    AfterPivotUpdate();
  }

  /* Hook to define post-update operations;
   * for CARP, oob->UpdateBounds() will be invoked here
   * TODO: need to figure out how to connect this to CarpDB
   */
  virtual void AfterPivotUpdate() = 0;

  friend class PivotUtils;

  int my_rank_;

  port::Mutex mutex_;
  port::CondVar cv_;

  double range_min_;
  double range_max_;

  std::vector<float> rank_bins_;
  std::vector<float> rank_counts_;
  std::vector<uint64_t> rank_counts_aggr_;

  double my_pivots_[kMaxPivots];
  size_t my_pivot_count_;
  double my_pivot_width_;
};
}  // namespace carp
}  // namespace pdlfs
