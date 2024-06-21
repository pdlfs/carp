//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/env.h"

#include <math.h>

#define LOG_LVL 3

int logv2(int lvl, const char* fmt, ...);

#define LOG_ERRO 1
#define LOG_WARN 2
#define LOG_INFO 3
#define LOG_DBUG 4
#define LOG_DBG2 5
#define LOG_DBG3 6

#define DEF_LOGGER ::pdlfs::Logger::Default()
#define __LOG_ARGS__ DEF_LOGGER, __FILE__, __LINE__

int logv(pdlfs::Logger* info_log, const char* file, int line, int level,
         const char* fmt, ...);
int loge(const char* op, const char* path);

#define EXPAND_ARGS(...) __VA_ARGS__

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
#define KB(n) (1024 * (n))
#define MB(n) (1024 * KB(n))

typedef struct RdbOptions {
  Env* env;
  std::string data_path;
  std::string output_path;
  uint32_t parallelism;
  bool analytics_on;

  bool query_on;
  int query_rank;
  int query_epoch;

  float query_begin;
  float query_end;

  bool query_batch;
  std::string query_batch_in;

  bool full_scan;

  RdbOptions()
      : env(NULL),
        parallelism(1),
        analytics_on(false),
        query_on(false),
        query_rank(-1),
        query_epoch(-1),
        query_begin(0),
        query_end(0),
        query_batch(false),
        full_scan(false) {}
} RdbOptions;
}  // namespace plfsio
}  // namespace pdlfs
