#!/bin/sh
sharding=$1
/home/hadoop/spark/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--name ClusterModelBehavior2"$sharding" \
--num-executors 3 \
--executor-memory 2g \
--executor-cores 2 \
--class com.ezr.model.ClusterModel \
/data/lib/KmeansDecisionTreeModel.jar 5 300 10 6 7 1 8 $sharding
