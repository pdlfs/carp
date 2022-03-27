#!/usr/bin/env bash

HDF5_LIB="/users/ankushj/repos/hdf5/hdf5-2004-apt-ompi/CMake-hdf5-1.10.7/HDF5-1.10.7-Linux/HDF_Group/HDF5/1.10.7/lib"
MPI_LIB="/users/ankushj/repos/fastquery-ompi/fastquery-0.8.4.3/mpi/install/lib"
FB_LIB="/users/ankushj/repos/common-install/fastbit/lib"
FQ_LIB="/users/ankushj/repos/common-install/fastquery/lib"

export LD_LIBRARY_PATH="$MPI_LIB;$HDF5_LIB;$FB_LIB;$FQ_LIB"

MPIEXEC="/users/ankushj/repos/fastquery-ompi/fastquery-0.8.4.3/mpi/install/bin/mpirun"
OMPI_FLAGS="--mca btl tcp,self,vader --mca btl_tcp_if_include ibs2 --mca pml ob1"
FQ_BUILD_BIN=/users/ankushj/repos/fastquery-ompi/fastquery-0.8.4.3/fastquery/examples/buildIndex

DRYRUN=0

hoststr() {
  NNODES=$1
  CORES_PER_NODE=$2
  hstr=$(seq 0 $(( NNODES - 1)) | sed 's/^/h/g' | sed "s/$/-dib:$CORES_PER_NODE/g" | paste -sd, -)
  echo $hstr
}

hoststr_relative() {
  NNODES=$1
  NODE_BEG=$(hostname | cut -d. -f 1 | sed 's/h//g')
  CORES_PER_NODE=$2
  hstr=$(seq $NODE_BEG $(( NODE_BEG + NNODES - 1)) | sed 's/^/h/g' | sed "s/$/-dib:$CORES_PER_NODE/g" | paste -sd, -)
  echo $hstr
}

fsize() {
  echo $(ls -l $1 | awk '{ print $5 }')
}

buildIndex() {
  RUN_ID=$1
  NNODES=$2
  CORES_PER_NODE=$3
  H5FILE=$4
  IDXFILE=$5
  ATTR_PATH=$6
  ATTR=$7
  MPISUBSIZE=$8

  HSTR=$(hoststr_relative $NNODES $CORES_PER_NODE)

  # CMD="$MPIEXEC \
    # --host $HSTR \
    # -n $(( NNODES * CORES_PER_NODE )) \
    # $OMPI_FLAGS \
    # -x LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    # $FQ_BUILD_BIN -m H5PART -b 30 -f $H5FILE -i $H5FILE.idx -r -v 1 -l $MPISUBSIZE \
    # "

  # CMD="$MPIEXEC \
    # --host $HSTR \
    # -n $(( NNODES * CORES_PER_NODE )) \
    # $OMPI_FLAGS \
    # -x LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    # $FQ_BUILD_BIN -h"

  # CMD="$MPIEXEC \
    # --host $HSTR \
    # -n $(( NNODES * CORES_PER_NODE )) \
    # $OMPI_FLAGS \
    # -x LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    # $FQ_BUILD_BIN -m H5PART -b 30 -f $H5FILE -i $H5FILE.idx \ 
    # -r -v 1 -p $ATTR_PATH -n $ATTR -l $MPISUBSIZE \
    # "

  CMD="$MPIEXEC \
    --host $HSTR \
    -n $(( NNODES * CORES_PER_NODE )) \
    $OMPI_FLAGS \
    -x LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    $FQ_BUILD_BIN -m H5PART -b 30 -f $H5FILE -i $IDXFILE \ 
    -r -v 1 -n $ATTR -l $MPISUBSIZE \
    "

  echo $CMD | tee -a cmds.log.$RUN_ID

  [[ $DRYRUN -eq 0 ]] && LD_LIBRARY_PATH=$LD_LIBRARY_PATH $CMD
  [[ $DRYRUN -eq 0 ]] || sleep 1
}

run_worker() {
  RUN_ID=$1
  NNODES=$2
  CORES_PER_NODE=$3
  ATTR=$4
  ATTR_PATH=$5
  H5FILE=$6
  IDXFILE=$7
  MPISUBSIZE=$8


  TIME_BEGIN=$(date +%s)

  buildIndex $RUN_ID $NNODES $CORES_PER_NODE $H5FILE $IDXFILE $ATTR_PATH $ATTR $MPISUBSIZE

  TIME_END=$(date +%s)
  TIME_TAKEN=$(( TIME_END - TIME_BEGIN))

  SIZE_OLD=$(fsize ${H5FILE})
  SIZE_NEW=$(fsize ${IDXFILE})

  echo $NNODES, $CORES_PER_NODE, $H5FILE, $ATTR_PATH, $ATTR, $MPISUBSIZE, $TIME_TAKEN, $SIZE_OLD, $SIZE_NEW  >> runs.log.$RUN_ID
}

run() {
  STRIPE=4
  H5FILE=$1
  IDXFILE=$H5FILE.idx

  RUN_ID=$(echo $RANDOM)
  RUN_ID=mar14.pools.par8
  CORES_PER_NODE=16
  ATTR="Ux"
  # ATTR="__"
  ATTR_PATH="/Step#200"
  # H5FILE=/mnt/lt20ad2/stripe$STRIPE/particle.t200.hdf5
  # H5FILE=/mnt/lt20ad2/stripe$STRIPE/particle.3t.hdf5
  # IDXFILE=/mnt/lt20/stripe$STRIPE/particle.t200.hdf5.idx
  # IDXFILE=/mnt/lt20/stripe$STRIPE/particle.t200.hdf5.idx
  # H5FILE=/mnt/lt20ad2/stripe10/particle.t200.hdf5
  # H5FILE=/mnt/lt20ad2/stripe20/particle.t200.hdf5

  echo "Run ID: $RUN_ID"

  # [[ $DRYRUN -eq 0 ]] && cp $H5FILE ${H5FILE}.backup
  # [[ $DRYRUN -eq 0 ]] && cp ${H5FILE}.backup $H5FILE

  MPISUB_LIST_M=(1 2 5 10 20)
  # MPISUB_LIST_M=(2 5 10 20)
  MPISUB_LIST_M=( 10 )
  NNODES_LIST=( 8 16 32 )
  NNODES_LIST=( 8 )
  for MPISUBSIZE_M in ${MPISUB_LIST_M[@]};
  do
    MPISUBSIZE=$((MPISUBSIZE_M * 1000 * 1000))
    echo "MPISUBSIZE: ", $MPISUBSIZE
    for NNODES in ${NNODES_LIST[@]};
    do
      run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE $MPISUBSIZE
      # rm $IDXFILE
      sleep 20
      # [[ $DRYRUN -eq 0 ]] && cp ${H5FILE}.backup $H5FILE
    done
  done

}

run $1
# run 8
# run 16
# run 20
# echo $(hoststr_cur 4 16)
# run /mnt/lt20/stripe16/particle.0to2.hdf5
# run /mnt/lt20/stripe16/particle.3to5.hdf5
# run /mnt/lt20/stripe16/particle.6to8.hdf5
# run /mnt/lt20/stripe16/particle.9to11.hdf5
