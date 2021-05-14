//
// Created by Ankush on 5/14/2021.
//

#include "reader/FmtChecker.h"

#include <stdio.h>

typedef pdlfs::plfsio::RdbOptions RdbOptions;

void PrintHelp(const char* prog) { printf("Usage: %s -i <plfs_dir>", prog); }

void ParseOptions(int argc, char* argv[], RdbOptions& options) {
  extern char* optarg;
  extern int optind;
  int c;
  while ((c = getopt(argc, argv, "i:")) != -1) {
    switch (c) {
      case 'i':
        options.data_path = optarg;
        break;
      default:
        PrintHelp(argv[0]);
        exit(0);
        break;
    }
  }

  if (!options.env->FileExists(options.data_path.c_str())) {
    logf(LOG_ERRO, "Dir not set, or set dir does not exist!");
    PrintHelp(argv[0]);
    exit(1);
  }
}

int main(int argc, char* argv[]) {
  RdbOptions options;
  options.env = pdlfs::port::PosixGetDefaultEnv();
  ParseOptions(argc, argv, options);
  pdlfs::plfsio::FmtChecker fmt_checker(options);
  pdlfs::Status s = fmt_checker.Run();
  logf(LOG_INFO, "Return Status: %s\n", s.ToString().c_str());
  return 0;
}
