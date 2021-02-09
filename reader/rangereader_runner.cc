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

#ifdef PDLFS_TBB
#define TBB_PROMPT "TBB enabled. Additional optimizations may be used."
#else
#define TBB_PROMPT "TBB disabled. Additional optimizations unavailable."
#endif

namespace pdlfs {
namespace plfsio {
void feature_prompt() {
  logf(LOG_INFO, TBB_PROMPT);
}

void test(float qbeg, float qend, int par = 8) {
  RdbOptions options;
  RandomAccessFile* src;
  Env* env = port::PosixGetDefaultEnv();
//  Env* env = port::PosixGetUnBufferedIOEnv();
 const char* dpath = "/panfs/probescratch/TableFS/test-aj-512/out/particle";
  // const char* dpath = "/Users/schwifty/Repos/workloads/rdb/tables";
  options.env = env;
  options.parallelism = par;
  RangeReader rr(options);
  rr.ReadManifest(dpath);
  rr.QueryParallel(0, qbeg, qend);
}
}  // namespace plfsio
}  // namespace pdlfs

int main(int argc, char* argv[]) {
  float qbeg = std::stof(argv[1]);
  float qend = std::stof(argv[2]);
  int par = std::stoi(argv[3]);
  printf("Query: %.3f %.3f (par: %d)\n", qbeg, qend, par);
  pdlfs::plfsio::feature_prompt();
  pdlfs::plfsio::test(qbeg, qend, par);
}
