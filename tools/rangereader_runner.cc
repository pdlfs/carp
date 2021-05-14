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

#include "reader/range_reader.h"

#include "pdlfs-common/env.h"

#include <getopt.h>
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
void feature_prompt() { logf(LOG_INFO, TBB_PROMPT); }
}  // namespace plfsio
}  // namespace pdlfs

void PrintHelp() {
  printf(
      "./prog [-p parallelism] [-a analytics] [-q query -s query_start -e "
      "query_end] [-b batch_query_path ]\n");
}

void ParseOptionsLong(int argc, char* argv[],
                      pdlfs::plfsio::RdbOptions& options) {
  int c;
  int digit_optind = 0;

  while (1) {
    int this_option_optind = optind ? optind : 1;
    int option_index = 0;
    static struct option long_options[] = {
        {"query", optional_argument, 0, 0},
    };

    c = getopt_long(argc, argv, "q", long_options, &option_index);
    if (c == -1) break;
  }
}

void ParseOptions(int argc, char* argv[], pdlfs::plfsio::RdbOptions& options) {
  extern char* optarg;
  extern int optind;
  int c;

  while ((c = getopt(argc, argv, "i:p:aqb:e:x:y:h")) != -1) {
    switch (c) {
      case 'i':
        options.data_path = optarg;
        break;
      case 'p':
        options.parallelism = std::stoi(optarg);
        break;
      case 'a':
        options.analytics_on = true;
        break;
      case 'q':
        options.query_on = true;
        break;
      case 'b':
        options.query_batch = true;
        options.query_batch_in = optarg;
        break;
      case 'e':
        options.query_epoch = std::stoi(optarg);
        break;
      case 'x':
        options.query_begin = std::stof(optarg);
        break;
      case 'y':
        options.query_end = std::stof(optarg);
        break;
      case 'h':
        PrintHelp();
        exit(0);
        break;
    }
  }

#define BOOLS(p) ((p) ? "ON" : "OFF")

  printf("[Threads] %d\n", options.parallelism);
  printf("[Analytics] %s\n", BOOLS(options.analytics_on));
  if (options.query_on) {
    printf("[Query] Mode: Single\n");
    printf("[Query] %.3f to %.3f\n", options.query_begin, options.query_end);
  } else if (options.query_batch) {
    printf("[Query] Mode: Batch\n");
    printf("[Query] Batchfile: %s\n", options.query_batch_in.c_str());
  } else {
    printf("[Query] Mode: Off\n");
  }
}

int main(int argc, char* argv[]) {
  pdlfs::plfsio::feature_prompt();
  pdlfs::plfsio::RdbOptions options;
  ParseOptions(argc, argv, options);

  if (options.query_batch) {
    printf("Batch Queries not implemented\n");
    exit(EXIT_FAILURE);
  }

  if (options.query_on && options.query_epoch < 0) {
    printf("[ERROR] Epoch < 0\n");
    exit(EXIT_FAILURE);
  }

  options.env = pdlfs::port::PosixGetDefaultEnv();

  if (!options.env->FileExists(options.data_path.c_str())) {
    printf("Input directory does not exist\n");
    exit(EXIT_FAILURE);
  }

  pdlfs::plfsio::RangeReader reader(options);
  reader.ReadManifest(options.data_path);
  if (options.query_on) {
    reader.QueryParallel(options.query_epoch, options.query_begin,
                         options.query_end);
  } else if (options.query_batch) {
    printf("Batch Queries not implemented\n");
    exit(EXIT_FAILURE);
  }
  return 0;
}
