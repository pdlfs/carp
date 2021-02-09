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
  uint32_t parallelism;
} RdbOptions;
}  // namespace plfsio
}  // namespace pdlfs
