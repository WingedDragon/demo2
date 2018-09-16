#!/bin/bash

#params
partitions=300
day_begin=`date -d "-7 day" +"%Y%m%d"`
day_end=`date -d "-1 day" +"%Y%m%d"`

day_end=2018-07-16
status=02

#spark sql job
/Users/finup/framework/spark-2.2.0-bin-hadoop2.7/bin/spark-sql \
--master yarn  \
--name r_customer \
--S \
--hiveconf partitions=$partitions \
--hiveconf day_begin=$day_begin \
--hiveconf day_end=$day_end \
--hiveconf status=$status \
--num-executors 1 \
--executor-memory 1G \
--executor-cores 1 \
-f etl_bidding