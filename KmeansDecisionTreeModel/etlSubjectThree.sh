#!/bin/sh
sharding=$1
/home/hadoop/spark/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--name DelDataBehavior3"$sharding" \
--num-executors 3 \
--executor-memory 2g \
--executor-cores 2 \
--jars /data/lib/mysql-connector-java-5.1.27.jar \
--class com.ezr.etl.DelDataBehaviorTwoAndThree \
/data/lib/KmeansDecisionTreeModel.jar 7 300 10 4 3 2 9 10 11 12 13 14 15 16 17 $sharding
