特征工程

1.券敏感
spark2-submit  --master yarn --deploy-mode cluster --name CouponSensitive1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.coupon.CouponSensitive /home/messi/src/FeatureEngine.jar 1

2.互动意愿
spark2-submit  --master yarn --deploy-mode cluster --name ActDeliver1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.actwilling.ActDeliver /home/messi/src/FeatureEngine.jar 1

分享有礼
spark2-submit  --master yarn --deploy-mode cluster --name ActMediaShare1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.actwilling.ActMediaShare /home/messi/src/FeatureEngine.jar 1

spark2-submit  --master yarn --deploy-mode cluster --name ActReserve1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.actwilling.ActReserve /home/messi/src/FeatureEngine.jar 1

spark2-submit  --master yarn --deploy-mode cluster --name ActScanGet1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.actwilling.ActScanGet /home/messi/src/FeatureEngine.jar 1

完善资料
spark2-submit  --master yarn --deploy-mode cluster --name InfoPerfect1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.actwilling.InfoPerfect /home/messi/src/FeatureEngine.jar 1

现金红包
spark2-submit  --master yarn --deploy-mode cluster --name MpReceiverPacket1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.actwilling.MpReceiverPacket /home/messi/src/FeatureEngine.jar 1

订单评论
spark2-submit  --master yarn --deploy-mode cluster --name SerComment1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.actwilling.SerComment /home/messi/src/FeatureEngine.jar 1

联系导购
spark2-submit  --master yarn --deploy-mode cluster --name ShopGuide1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.actwilling.ShopGuide /home/messi/src/FeatureEngine.jar 1

3.积分敏感
spark2-submit  --master yarn --deploy-mode cluster --name BonusSensitive1 --jars /home/messi/jars/joda-convert-1.8.1.jar,/home/messi/jars/joda-time-2.9.9.jar 
--class com.ezr.bonus.BonusSensitive /home/messi/src/FeatureEngine.jar 1


人群分类

# 数据处理
spark2-submit  --master yarn --deploy-mode cluster --name DelDataBehavior1
--class com.ezr.etl.DelDataBehaviorOne /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10  5  1

spark2-submit  --master yarn  --deploy-mode cluster --name DelDataBehavior2  --jars /home/messi/jars/mysql-connector-java-5.1.27.jar 
--class com.ezr.etl.DelDataBehaviorTwoAndThree /home/messi/src/KmeansDecisionTreeModel.jar 5 300 10 6 7 1 8  1

spark2-submit  --master yarn  --deploy-mode cluster --name DelDataBehavior3  --jars /home/messi/jars/mysql-connector-java-5.1.27.jar 
--class com.ezr.etl.DelDataBehaviorTwoAndThree /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10 4 3 2 9 10 11 12 13 14 15 16 17  1

# 聚类
spark2-submit  --master yarn  --deploy-mode cluster --name ClusterModelBehavior1
--class com.ezr.model.ClusterModel /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10  5  1

spark2-submit  --master yarn  --deploy-mode cluster --name ClusterModelBehavior2
--class com.ezr.model.ClusterModel /home/messi/src/KmeansDecisionTreeModel.jar 5 300 10 6 7 1 8  1

spark2-submit  --master yarn  --deploy-mode cluster --name ClusterModelBehavior3
--class com.ezr.model.ClusterModel /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10 4 3 2 9 10 11 12 13 14 15 16 17  1


# 通过决策树分类后整理数据  ==增量数据打标签
spark2-submit  --master yarn  --deploy-mode cluster --name DecisionTreeClassifierOne1  
--class com.ezr.classifier.DecisionTreeClassifierOne /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10  5  1

spark2-submit  --master yarn  --deploy-mode cluster --name DecisionTreeClassifierTwoAndTree2  
--class com.ezr.classifier.DecisionTreeClassifierTwoAndTree /home/messi/src/KmeansDecisionTreeModel.jar 5 300 10 6 7 1 8  1

spark2-submit  --master yarn  --deploy-mode cluster --name DecisionTreeClassifierTwoAndTree3  
--class com.ezr.classifier.DecisionTreeClassifierTwoAndTree /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10 4 3 2 9 10 11 12 13 14 15 16 17  1


#写入Es
spark-submit --master yarn  --deploy-mode cluster  --name ToEsShopGuideSpark1 \ 
--jars /home/messi/jars/elasticsearch-5.1.1.jar,/home/messi/jars/elasticsearch-spark_2.10-2.4.4.jar,/home/messi/jars/mysql-connector-java-5.1.27.jar \
 --class com.ezr.toes.ToEsShopGuide --num-executors 1 --executor-memory 1g --executor-cores 1   /home/messi/src/ToEsSpark1.jar 1

spark2-submit --master yarn  --deploy-mode cluster  --name ToEsShopGuideSpark2 
--jars /home/messi/jars/elasticsearch-5.1.1.jar,/home/messi/jars/elasticsearch-hadoop-5.1.1.jar,/home/messi/jars/mysql-connector-java-5.1.27.jar  
 --class com.ezr.toes.ToEsShopGuide --num-executors 1 --executor-memory 1g --executor-cores 1   /home/messi/src/ToEsSpark2.jar 1

=====================================================================
# 分类
spark2-submit  --master yarn  --deploy-mode cluster --name ClassifierModel1  
--class com.ezr.model.ClassifierModel /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10  5  5  5  32  1

spark2-submit  --master yarn  --deploy-mode cluster --name ClassifierModel2  
--class com.ezr.model.ClassifierModel /home/messi/src/KmeansDecisionTreeModel.jar 5 300 10 6 7 1 8  5  5  32  1

spark2-submit  --master yarn  --deploy-mode cluster --name ClassifierModel3  
--class com.ezr.model.ClassifierModel /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10 4 3 2 9 10 11 12 13 14 15 16 17   5  5  32  1


# 打标签
spark2-submit  --master yarn  --deploy-mode cluster --name KmeansDecisionTreeModel1  --jars /home/messi/jars/mysql-connector-java-5.1.27.jar
--class com.ezr.bi.label.KmeansDecisionTreeModelOne /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10  5

spark2-submit  --master yarn  --deploy-mode cluster --name KmeansDecisionTreeModel2  --jars /home/messi/jars/mysql-connector-java-5.1.27.jar
--class com.ezr.bi.label.KmeansDecisionTreeModelsTwoAThree /home/messi/src/KmeansDecisionTreeModel.jar 5 300 10 6 7 1 8 

spark2-submit  --master yarn  --deploy-mode cluster --name KmeansDecisionTreeModel3  --jars /home/messi/jars/mysql-connector-java-5.1.27.jar
--class com.ezr.bi.label.KmeansDecisionTreeModelsTwoAThree /home/messi/src/KmeansDecisionTreeModel.jar 7 300 10 4 3 2 9 10 11 12 13 14 15 16 17  

