#!/usr/bin/env bash

DS_NAME="driver_offers_IL_2016_08"
DS_PATH="gett_algo/datasets/driver_offers/$DS_NAME"

SRC_DS_DIR="/mnt1/$DS_PATH"
DST_DS_DIR="/mnt1/gett_algo/datasets/driver_offers/${DS_NAME}_clean"
mkdir -p $DST_DS_DIR


presto-cli --catalog hive --schema gett_algo \
--execute "select concat('aws s3 cp --recursive \
s3://presto-db/$DS_PATH/driver_id=',driver_id,'/ /mnt1/$DS_PATH/',driver_id) \
from ( \
    select count(*), cast(driver_id as varchar) driver_id \
    from $DS_NAME \
    group by 2 \
    having count(*) >= 1000 \
)" > $SRC_DS_PATH/download_files.sh; sed -i 's/"//g' $SRC_DS_PATH/download_files.sh; chmod +x $SRC_DS_PATH/download_files.sh

#$SRC_DS_PATH/download_files.sh

#for d in `ls $SRC_DS_DIR`; do
#    for f in `ls $SRC_DS_DIR/$d/`; do
#        echo "$SRC_DS_DIR/$d/$f --> $DST_DS_DIR/$d.csv"
#        cp -f $SRC_DS_DIR/$d/$f $DST_DS_DIR/$d.gz
#        gunzip $DST_DS_DIR/$d.gz
#        mv $DST_DS_DIR/$d $DST_DS_DIR/$d.csv
#    done
#done
