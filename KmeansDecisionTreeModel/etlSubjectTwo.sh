#!/bin/sh
sharding=$1
/home/hadoop/spark/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--name DelDataBehavior2"$sharding" \
--num-executors 3 \
--executor-memory 2g \
--executor-cores 2 \
--jars /data/lib/mysql-connector-java-5.1.27.jar \
--class com.ezr.etl.DelDataBehaviorTwoAndThree \
/data/lib/KmeansDecisionTreeModel.jar 5 300 10 6 7 1 8 $sharding
