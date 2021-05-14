//
// Created by Ankush on 5/11/2021.
//

#include "common.h"
#include "reader/compactor.h"

#include <pdlfs-common/port.h>
#include <stdio.h>

int main() {
  printf("hello world - compactor\n");
  pdlfs::plfsio::RdbOptions options;
  options.data_path = "/mnt/c/Users/Ankush/Repos/RDB";
  options.env = pdlfs::port::PosixGetDefaultEnv();
  pdlfs::plfsio::Compactor compactor(options);
  compactor.Run();
}