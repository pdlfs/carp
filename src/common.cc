//
// Created by Ankush J on 2/1/21.
//

#include <stdarg.h>
#include <stdio.h>

int logf(int lvl, const char* fmt, ...) {
  const char* prefix;
  va_list ap;
  switch (lvl) {
    case 4:
      prefix = "!!! ERROR !!! ";
      break;
    case 3:
      prefix = "-WARNING- ";
      break;
    case 2:
      prefix = "-INFO- ";
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
