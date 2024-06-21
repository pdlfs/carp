//
// Created by Ankush J on 2/1/21.
//

#include "common.h"

#include <stdarg.h>
#include <stdio.h>

int logv2(int lvl, const char* fmt, ...) {
  if (lvl < LOG_LVL) return 0;

  const char* prefix;
  va_list ap;
  switch (lvl) {
    case 5:
      prefix = "!!! ERROR !!! ";
      break;
    case 4:
      prefix = "-WARNING- ";
      break;
    case 3:
      prefix = "-INFO- ";
      break;
    case 2:
      prefix = "-DEBUG- ";
      break;
    case 1:
      prefix = "-DEBUG- ";
      break;
    default:
      prefix = "";
      break;
  }
  fprintf(stderr, "%s", prefix);

  va_start(ap, fmt);
  vfprintf(stderr, fmt, ap);
  va_end(ap);

  fprintf(stderr, "\n");
  return 0;
}

int logv(pdlfs::Logger* info_log, const char* file, int line, int level,
         const char* fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  if (info_log != NULL) {
    int glvl = 0, gvlvl = 0;  // glog level and glog verbose lvl
    if (level == LOG_ERRO) {
      glvl = 2;
    } else if (level == LOG_WARN) {
      glvl = 1;
    } else if (level >= LOG_INFO) {
      glvl = 0;
      gvlvl = level - LOG_INFO;
    }

    info_log->Logv(file, line, glvl, gvlvl, fmt, ap);
  }
  va_end(ap);

  return 0;
}

int loge(const char* op, const char* path) {
  return logv(__LOG_ARGS__, LOG_ERRO, "!%s(%s): %s", strerror(errno), op, path);
}
