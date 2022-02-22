#include "vpic_writer.h"

int main(int argc, char* argv[]) {
  MPI_Init(&argc, &argv);

  int nranks;
  MPI_Comm_size(MPI_COMM_WORLD, &nranks);

  int my_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

  logf(LOG_INFO, "Hello World @ Rank %d/%d\n", my_rank, nranks);

  VPICWriterOpts opts;
  opts.dir_in = "/mnt/lustre/carp-big-run/particle.compressed";
  opts.file_out = "/mnt/lustre/carp-big-run/particle.hdf5";
  opts.my_rank = my_rank;
  opts.num_ranks = nranks;

  VPICWriter writer(opts);
  writer.Run();

  MPI_Finalize();

  return 0;
}
