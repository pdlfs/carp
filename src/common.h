//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/env.h"

#define LOG_ERRO 5
#define LOG_WARN 4
#define LOG_INFO 3
#define LOG_DBUG 2
#define LOG_DBG2 1

#define LOG_LVL 1

int logf(int lvl, const char* fmt, ...);

namespace pdlfs {
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
