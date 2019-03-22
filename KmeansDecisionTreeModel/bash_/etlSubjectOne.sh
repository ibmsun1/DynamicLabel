#!/bin/sh
sharding=$1
/home/hadoop/spark/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--name DelDataBehavior1"$sharding" \
--num-executors 3 \
--executor-memory 2g \
--executor-cores 2 \
--class com.ezr.etl.DelDataBehaviorOne \
/data/lib/KmeansDecisionTreeModel.jar  7 300 10  5 $sharding
