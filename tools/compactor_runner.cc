//
// Created by Ankush on 5/11/2021.
//

#include "common.h"
#include "reader/compactor.h"

#include <pdlfs-common/port.h>
#include <stdio.h>
#include <unistd.h>

typedef pdlfs::plfsio::RdbOptions RdbOptions;

void PrintHelp(const char* prog) {
  printf("Usage: %s -i <plfs_dir> [-o <out_dir>]", prog);
}

void ParseOptions(int argc, char* argv[], RdbOptions& options) {
  extern char* optarg;
  extern int optind;
  int c;
  while ((c = getopt(argc, argv, "i:e:o:")) != -1) {
    switch (c) {
      case 'i':
        options.data_path = optarg;
        break;
      case 'e':
        options.query_epoch = std::stoi(optarg);
        break;
      case 'o':
        options.output_path = optarg;
        break;
      default:
        PrintHelp(argv[0]);
        exit(0);
        break;
    }
  }

  if (options.output_path.empty()) {
    std::string dest = options.data_path;
    if (dest[dest.size() - 1] == '/') dest.resize(dest.size() - 1);
    dest += ".merged";
    options.output_path = dest;
  }

  if (!options.env->FileExists(options.data_path.c_str())) {
    logv(__LOG_ARGS__, LOG_ERRO, "Dir not set, or set dir does not exist!");
    PrintHelp(argv[0]);
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  RdbOptions options;
  options.env = pdlfs::port::PosixGetDefaultEnv();
  ParseOptions(argc, argv, options);
  pdlfs::plfsio::Compactor compactor(options);
  pdlfs::Status s = compactor.Run();
  logv(__LOG_ARGS__, LOG_INFO, "Return Status: %s\n", s.ToString().c_str());
  return 0;
}
