#pragma once

#include "carp_base.h"

#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <vector>

namespace pdlfs {
namespace carp {

/* forward declaration */
class CarpBase;

class PivotUtils {
 public:
  template <typename T>
  static std::string SerializeVector(std::vector<T>& v);

  template <typename T>
  static std::string SerializeVector(T* v, size_t vsz);

  /**
   * @brief Calculate pivots from the current pivot_ctx state.
   * This also modifies OOB buffers (sorts them), but their order shouldn't
   * be relied upon anyway.
   *
   * SAFE version computes "token pivots" in case no mass is there to
   * actually compute pivots. This ensures that merging calculations
   * do not fail.
   *
   * XXX: a more semantically appropriate fix would be to define addition
   * and resampling for zero-pivots
   *
   * @param carp pivot context
   *
   * @return
   */
  static int CalculatePivots(CarpBase* carp, size_t num_pivots);

 private:
  static int CalculatePivotsFromOob(CarpBase* carp, int num_pivots);

  static int CalculatePivotsFromAll(CarpBase* carp, int num_pivots);

  static int GetRangeBounds(CarpBase* carp, std::vector<float>& oobl,
                            std::vector<float>& oobr, float& range_start,
                            float& range_end);
};
}  // namespace carp
}  // namespace pdlfs
