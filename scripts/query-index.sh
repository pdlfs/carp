#!/usr/bin/env bash

HDF5_LIB="/users/ankushj/repos/hdf5/hdf5-2004-apt-ompi/CMake-hdf5-1.10.7/HDF5-1.10.7-Linux/HDF_Group/HDF5/1.10.7/lib"
MPI_LIB="/users/ankushj/repos/fastquery-ompi/fastquery-0.8.4.3/mpi/install/lib"
FB_LIB="/users/ankushj/repos/common-install/fastbit/lib"
FQ_LIB="/users/ankushj/repos/common-install/fastquery/lib"

export LD_LIBRARY_PATH="$MPI_LIB;$HDF5_LIB;$FB_LIB;$FQ_LIB"

MPIEXEC="/users/ankushj/repos/fastquery-ompi/fastquery-0.8.4.3/mpi/install/bin/mpirun"
OMPI_FLAGS="--mca btl tcp,self,vader --mca btl_tcp_if_include ibs2 --mca pml ob1"
FQ_BUILD_BIN=/users/ankushj/repos/fastquery-ompi/fastquery-0.8.4.3/fastquery/examples/buildIndex
FQ_QUERY_BIN=/users/ankushj/repos/fastquery-ompi/fastquery-0.8.4.3/fastquery/examples/queryIndex

DRYRUN=0

hoststr() {
  NNODES=$1
  CORES_PER_NODE=$2
  hstr=$(seq 0 $(( NNODES - 1)) | sed 's/^/h/g' | sed "s/$/-dib:$CORES_PER_NODE/g" | paste -sd, -)
  echo $hstr
}

read_stamp() {
   sudo lctl get_param -R 'osc.*.stats' | grep read_bytes | awk '{ sum += $7 / 1073741824 } END { print sum }'
}

fsize() {
  echo $(ls -l $1 | awk '{ print $5 }')
}

queryIndex() {
  RUN_ID=$1
  NNODES=$2
  CORES_PER_NODE=$3
  H5FILE=$4
  IDXFILE=$5
  ATTR_PATH=$6
  ATTR=$7
  MPISUBSIZE=$8
  QBEG=$9 QEND=${10}

  HSTR=$(hoststr $NNODES $CORES_PER_NODE)

  # Usage:
  # /users/ankushj/repos/fastquery-ompi/fastquery-0.8.4.3/fastquery/examples/queryIndex -f data-file-name -q query-conditions-in-a-single-string [-i index-file-name] [-c conf-file-name] [-g log-file-name] [-n name-of-variable] [-p path-of-variable] [-m file model [HDF5(default), H5PART, NETCDF, PNETCDF] [-b use-boundingbox-data-selection] [-v verboseness] [-l mpi-subarray-length]
   # e.g:   ./queryIndex -f h5uc-data-index.h5 -q 'px < 0.3' -n y -p TimeStep2

  CMD="$MPIEXEC \
    --host $HSTR \
    -n $(( NNODES * CORES_PER_NODE )) \
    $OMPI_FLAGS \
    -x JOB_ID=$RUN_ID \
    -x LD_LIBRARY_PATH=$LD_LIBRARY_PATH \
    $FQ_QUERY_BIN -m H5PART -f $H5FILE -i $IDXFILE \
    -p $ATTR_PATH -n $ATTR -l $MPISUBSIZE -v 1 \
    "

  echo $CMD | tee -a querycmds.$RUN_ID
  # [[ $DRYRUN -eq 0 ]] && LD_LIBRARY_PATH=$LD_LIBRARY_PATH $CMD -q "Ux > 0.51 && Ux < 0.529"
  [[ $DRYRUN -eq 0 ]] && LD_LIBRARY_PATH=$LD_LIBRARY_PATH $CMD -q "Ux > $QBEG && Ux < $QEND" 2>&1 | tee log
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
  QBEG=$9
  QEND=${10}
  QSEL=${11}


  TIME_BEGIN=$(date +%s)
  SIZE_BEGIN=$(read_stamp)

  queryIndex $RUN_ID $NNODES $CORES_PER_NODE $H5FILE $IDXFILE $ATTR_PATH $ATTR $MPISUBSIZE $QBEG $QEND

  TIME_END=$(date +%s)
  SIZE_END=$(read_stamp)

  TIME_TAKEN=$(( TIME_END - TIME_BEGIN))
  SIZE_DELTA=$(bc <<< "$SIZE_END - $SIZE_BEGIN" )

  NUM_HITS=$(cat log | grep 'successfully complete processing query' | egrep -o '[0-9]+ hits' | head -1 | awk '{ print $1 }')

  echo $NNODES, $CORES_PER_NODE, $H5FILE, $ATTR_PATH, $ATTR, $MPISUBSIZE, $QBEG, $QEND, $QSEL, $TIME_TAKEN, $SIZE_DELTA, $NUM_HITS >> queries.log.$RUN_ID
  echo 1 | sudo tee /proc/sys/vm/drop_caches
}

run() {
  RUN_ID=$(echo $RANDOM).yescopy.ibs2
  RUN_ID=mar7.bigquery.stripe10.dropcaches
  RUN_ID=mar13.spancheck.redo
  CORES_PER_NODE=16
  ATTR="Ux"
  # ATTR="__"
  ATTR_PATH="/Step#200"
  H5FILE=/mnt/lt20ad2/stripe4/particle.t200.hdf5
  # H5FILE=/mnt/lt20ad2/stripe10/particle.3t.hdf5
  IDXFILE=/mnt/lt20ad2/stripe4/particle.t200.hdf5.idx
  IDXFILE_ALT=/mnt/lt20/stripe4/particle.t200.hdf5.idx
  # H5FILE=/mnt/lt20ad2/stripe10/particle.t200.hdf5
  # H5FILE=/mnt/lt20ad2/stripe20/particle.t200.hdf5

  MPISUBSIZE=10000000
  NNODES_LIST=(1 2 4)
  for NNODES in ${NNODES_LIST[@]};
  do
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE $MPISUBSIZE 0.51 0.515
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE $MPISUBSIZE 0.51 0.519
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE $MPISUBSIZE 0.51 0.520
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE $MPISUBSIZE 0.51 0.525
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE $MPISUBSIZE 0.51 0.529
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE $MPISUBSIZE 0.51 0.530

    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE_ALT $MPISUBSIZE 0.51 0.515
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE_ALT $MPISUBSIZE 0.51 0.519
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE_ALT $MPISUBSIZE 0.51 0.520
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE_ALT $MPISUBSIZE 0.51 0.525
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE_ALT $MPISUBSIZE 0.51 0.529
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $IDXFILE_ALT $MPISUBSIZE 0.51 0.530
  done

  return

  echo "Run ID: $RUN_ID"
  NNODES_LIST=( 1 2 4 8 16 32 )
  MPISUBSIZE=2000000
  for NNODES in ${NNODES_LIST[@]};
  do
    run_worker $RUN_ID $NNODES 16 $ATTR $ATTR_PATH $H5FILE $MPISUBSIZE
    sleep 20
  done

}

parse_epoch() {
  epochidx=$1

  pool_id=$(( epochidx / 3 ))
  pools=( a b c d )

  pool_name=${pools[$pool_id]}

  part_dir=/mnt/lustre/carp-big-run/particle.compressed.uniform
  ts=($(ls -d $part_dir/T.* | egrep -o 'T.[0-9]+$' | sort -t. -k2n | sed 's/T\./\n\/Step#/g'))
  tsname=${ts[$epochidx]}

  echo $pool_name, $tsname
}

run_worker_std_query_par() {
  RUN_ID=$1
  EPOCH=$2
  QBEG=$3
  QEND=$4

  epoch_params=$(parse_epoch $2)
  epoch_pool=$(echo $epoch_params | cut -d, -f 1)
  echo $epoch_pool
  H5FILE=$(fd 'hdf5$' /mnt/lt20ad2/pool$epoch_pool)
  IDXFILE=$(fd 'idx$' /mnt/lt20ad2/pool$epoch_pool)
  echo $H5FILE, $IDXFILE
}

run_std_query_par() {
  run_worker_std_query_par abc 4 0.5 0.6
}

run_std_query_seq() {
  RUN_ID=$1
  EPOCH=$2
  QBEG=$3
  QEND=$4
  QSEL=$5

  epoch_params=$(parse_epoch $2)
  epoch_ts=$(echo $epoch_params | cut -d, -f 2)

  NNODES=1

  echo $epoch_ts, $QBEG, $QEND

  EPOCH_RBEG=$(( (EPOCH / 3) * 3 ))
  EPOCH_REND=$(( EPOCH_RBEG + 2 ))
  echo $EPOCH_RBEG to $EPOCH_REND
  H5FILE=/mnt/lt20/stripe4/particle.${EPOCH_RBEG}to${EPOCH_REND}.hdf5
  IDXFILE=$H5FILE.idx
  ls $H5FILE
  MPISUBSIZE=10000000

  run_worker $RUN_ID $NNODES 16 "Ux" $epoch_ts $H5FILE $IDXFILE $MPISUBSIZE $QBEG $QEND $QSEL
}

run_std_query_batch() {
  QUERY_CSV=$1
  RUN_ID=batchq4.uniform.$(date +%b%d.%H%M)

  for line in $(cat $QUERY_CSV | grep -v epoch); do
    epoch=$(echo $line | cut -d, -f 1)
    qbeg=$(echo $line | cut -d, -f 2)
    qend=$(echo $line | cut -d, -f 3)
    qsel=$(echo $line | cut -d, -f 4)

    run_std_query_seq $RUN_ID $epoch $qbeg $qend $qsel

  done
}

for i in `seq 0 2`; do
  run_std_query_batch queries.in.csv
done
