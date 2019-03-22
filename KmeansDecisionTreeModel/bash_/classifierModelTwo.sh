#!/bin/sh
sharding=$1
/home/hadoop/spark/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--name DecisionTreeClassifierOne2"$sharding" \
--num-executors 3 \
--executor-memory 2g \
--executor-cores 2 \
--class com.ezr.classifier.DecisionTreeClassifierTwoAndTree \
/data/lib/KmeansDecisionTreeModel.jar 5 300 10 6 7 1 8 $sharding
