//
// Created by Ankush J on 2/1/21.
//

#pragma once

#include "pdlfs-common/env.h"

#define LOG_ERRO 4
#define LOG_WARN 3
#define LOG_INFO 2
#define LOG_DBUG 1

int logf(int lvl, const char* fmt, ...);

namespace pdlfs {
namespace plfsio {
typedef struct RdbOptions {
  Env* env;
} RdbOptions;
}  // namespace plfsio
}  // namespace pdlfs
