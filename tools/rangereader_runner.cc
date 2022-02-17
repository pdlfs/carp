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

#include <carp/carp_config.h>
#include "reader/range_reader.h"

#include "pdlfs-common/env.h"

#include <getopt.h>
#include <sys/stat.h>

#if __cplusplus >= 201103
#define OVERRIDE override
#else
#define OVERRIDE
#endif

#ifdef CARP_PARALLEL_SORT
#define TBB_PROMPT "TBB enabled. Additional optimizations may be used."
#else
#define TBB_PROMPT "TBB disabled. Additional optimizations unavailable."
#endif

namespace pdlfs {
namespace plfsio {
void feature_prompt() { logf(LOG_INFO, TBB_PROMPT); }

void ReadCSV(Env* env, const char* csv_path, std::vector< Query >& qvec) {
  std::string data;

  Status s = ReadFileToString(env, csv_path, &data);
  if (!s.ok()) {
    logf(LOG_ERRO, "error: %s", s.ToString().c_str());
    exit(-1);
  }

  const char* const data_ptr = data.c_str();
  size_t i = 0;

  while (i < data.size()) {
    int epoch;
    float qbeg, qend;

    int bytes_read;
    int items_read =
        sscanf(data_ptr + i, "%d,%f,%f\n%n", &epoch, &qbeg, &qend, &bytes_read);

    if (items_read != 3) {
      logf(LOG_ERRO, "CSV parsing failed! Items read: %d", items_read);
      exit(-1);
    }

    i += bytes_read;

    Query q(epoch, qbeg, qend);
    qvec.push_back(q);

    logf(LOG_INFO, "Query parsed: %s", q.ToString().c_str());
  }
}

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

  while ((c = getopt(argc, argv, "i:p:aqb:e:x:y:sh")) != -1) {
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
      case 's':
        options.full_scan = true;
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

  std::string full_scan = "";
  if (options.full_scan) {
    full_scan = "\n!!! ALERT: FULL SCAN !!!";
  }

  if (options.query_on) {
    printf("[Query] Mode: Single%s\n", full_scan.c_str());
    printf("[Query] %.3f to %.3f\n", options.query_begin, options.query_end);
  } else if (options.query_batch) {
    printf("[Query] Mode: Batch%s\n", full_scan.c_str());
    printf("[Query] Batchfile: %s\n", options.query_batch_in.c_str());
  } else {
    printf("[Query] Mode: Off\n");
  }
}

int main(int argc, char* argv[]) {
  pdlfs::plfsio::feature_prompt();
  pdlfs::plfsio::RdbOptions options;
  ParseOptions(argc, argv, options);

  if (options.query_on && !options.analytics_on && options.query_epoch < 0) {
    printf("[ERROR] Epoch < 0\n");
    exit(EXIT_FAILURE);
  }

  options.env = pdlfs::port::PosixGetDefaultEnv();

  if (!options.env->FileExists(options.data_path.c_str())) {
    printf("Input directory does not exist\n");
    exit(EXIT_FAILURE);
  }

  pdlfs::plfsio::RangeReader< pdlfs::RandomAccessFile > reader(options);
  if (options.query_on and !options.analytics_on) {
    reader.ReadManifest(options.data_path);
    if (options.full_scan) {
      reader.QueryNaive(options.query_epoch, options.query_begin,
                        options.query_end);
    } else {
      reader.QueryParallel(options.query_epoch, options.query_begin,
                           options.query_end);
    }
  } else if (options.query_batch) {
    if (options.full_scan) {
      logf(LOG_ERRO, "Full Scan not implemented on batch queries");
      exit(-1);
    }
    std::vector< pdlfs::plfsio::Query > qvec;
    pdlfs::plfsio::ReadCSV(options.env, options.query_batch_in.c_str(), qvec);
    reader.ReadManifest(options.data_path);
    reader.QueryParallel(qvec);
  } else if (options.analytics_on) {
    reader.AnalyzeManifest(options.data_path, options.query_on);
  }

  return 0;
}
