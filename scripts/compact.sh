#!/bin/bash

COMPBIN=/users/ankushj/range/carp/build-rel/tools/compactor
READBIN=/users/ankushj/range/carp/build-rel/tools/rangereader
INDIR=/mnt/ltio/jobs-qlat/vpic-carp8m/carp_P3584M_intvl250000/plfs/plfstmp
OUTDIR=$INDIR.out
EPCNT=5

get_rdb() {
    echo RDB-$(printf "%08x" $1).tbl
}

rename() {
  BASEDIR=$1
  echo "Basedir: "$BASEDIR

  EPCNT=$(ls $BASEDIR | egrep -o '^[0-9]{0,2}$' | wc -l)
  echo "Epochs found: "$EPCNT

  if [[ $EPCNT -eq 0 ]];
  then
    return
  fi

  ridx=0
  for ep in `seq 0 $EPCNT`; do
    epdir=$BASEDIR/$ep
    for f in `ls $epdir/RDB*`; do
      echo $f
      mv $f $BASEDIR/$(get_rdb $ridx)
      ridx=$(( ridx + 1 ))
    done
    rm $epdir/vpic*
    rmdir $epdir
  done
}

for EPOCH in `seq 0 $EPCNT`; do
  $COMPBIN -i $INDIR -o $OUTDIR -e $EPOCH &
done

rename $OUTDIR
