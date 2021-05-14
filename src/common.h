//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/env.h"

#include <math.h>

#define LOG_ERRO 5
#define LOG_WARN 4
#define LOG_INFO 3
#define LOG_DBUG 2
#define LOG_DBG2 1

#define LOG_LVL 2

int logf(int lvl, const char* fmt, ...);

namespace pdlfs {
namespace carp {
// clang-format off
class FloatUtils {
public:
  static constexpr float kFloatCompThreshold = 1e-3;

  static bool float_eq(float a, float b) {
    return fabs(a - b) < kFloatCompThreshold;
  }

  static bool float_gt(float a, float b) {
    return a > b + kFloatCompThreshold;
  }

  static bool float_gte(float a, float b) {
    return a > b - kFloatCompThreshold;
  }

  static bool float_lt(float a, float b) {
    return a < b - kFloatCompThreshold;
  }

  static bool float_lte(float a, float b) {
    return a < b + kFloatCompThreshold;
  }
};
// clang-format on
}  // namespace carp

namespace plfsio {
typedef struct RdbOptions {
  Env* env;
  std::string data_path;
  uint32_t parallelism;
  bool analytics_on;

  bool query_on;
  int query_epoch;

  float query_begin;
  float query_end;
  bool query_batch;
  std::string query_batch_in;

  RdbOptions()
      : env(NULL),
        parallelism(1),
        analytics_on(false),
        query_on(false),
        query_epoch(-1),
        query_begin(0),
        query_end(0),
        query_batch(false) {}
} RdbOptions;
}  // namespace plfsio
}  // namespace pdlfs