#!/usr/bin/env bash

NNODES=32
hoststr=$(for i in `seq 0 $((NNODES - 1))`; do echo h$i-dib:16; done | paste -sd, -)
mpirun_flags=$(echo --mca btl tcp,self,vader --mca btl_tcp_if_include ibs2 --mca pml ob1)
mpirun_flags=' '

# . ~/spack/share/spack/setup-env.sh
# spack load mpi

bin=/users/ankushj/repos/carp-umbrella/build-ompi/carp-prefix/src/carp-build/tools/vpicwriter
bin=/users/ankushj/repos/carp-umbrella/build-mpich/carp-prefix/src/carp-build/tools/vpicwriter

cmd=$(echo mpirun --host $hoststr $mpirun_flags -np $((NNODES * 16)) $bin)
# cmd=$(echo mpirun -np $((NNODES * 16)) $bin)
cmd_flags=$(echo -i /mnt/lustre/carp-big-run/particle.compressed.uniform -o $1/particle.$2to$3.hdf5 -s $2 -e $3)
echo $cmd $cmd_flags

$(echo $cmd $cmd_flags)
