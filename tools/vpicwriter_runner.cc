#include "vpic_writer.h"

#include <getopt.h>

void PrintHelp() {
  printf("./prog -i vpic_dir -o h5partfile.hdf5 -n ntimesteps\n");
}

void ParseOptions(int argc, char* argv[], VPICWriterOpts& opts) {
  extern char* optarg;
  extern int optind;
  int c;

  while ((c = getopt(argc, argv, "i:o:n:h")) != -1) {
    switch (c) {
      case 'i':
        opts.dir_in = optarg;
        break;
      case 'o':
        opts.file_out = optarg;
        break;
      case 'n':
        opts.ts_out = std::stoi(optarg);
      case 'h':
      default:
        PrintHelp();
        break;
    }
  }
}

int main(int argc, char* argv[]) {
  VPICWriterOpts opts;
  opts.dir_in = "";
  opts.file_out = "";
  opts.ts_out = -1;

  ParseOptions(argc, argv, opts);

  if ((opts.dir_in == "") or (opts.file_out == "") or (opts.ts_out < 0)) {
    PrintHelp();
    return 0;
  }

  MPI_Init(&argc, &argv);

  int nranks;
  MPI_Comm_size(MPI_COMM_WORLD, &nranks);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  if (my_rank == 0) {
    logf(LOG_INFO, "[DIR_IN] %s", opts.dir_in.c_str());
    logf(LOG_INFO, "[FILE_OUT] %s", opts.file_out.c_str());
    logf(LOG_INFO, "[NUM_TS] %d", opts.ts_out);
  }

  opts.my_rank = my_rank;
  opts.num_ranks = nranks;

  VPICWriter writer(opts);
  writer.Run();

  MPI_Finalize();

  return 0;
}
