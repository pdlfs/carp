/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#include "range_reader.h"

#include "pdlfs-common/env.h"

#include <sys/stat.h>

#if __cplusplus >= 201103
#define OVERRIDE override
#else
#define OVERRIDE
#endif

namespace pdlfs {
namespace plfsio {
void gen() {}
void test(float qbeg, float qend) {
  RdbOptions options;
  RandomAccessFile* src;
  Env* env = port::PosixGetDefaultEnv();
  const char* dpath = "/Users/schwifty/Repos/workloads/rdb/tables";
  options.env = env;
  RangeReader rr(options);
  rr.Read(dpath);
  rr.Query(0, qbeg, qend);
}
}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  printf("Hello World!\n");
  float qbeg = std::stof(argv[1]);
  float qend = std::stof(argv[2]);
  printf("Query: %.3f %.3f\n", qbeg, qend);
  pdlfs::plfsio::test(qbeg, qend);
}
