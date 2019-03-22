package com.ezr.model

import com.ezr.config.ConfigHelper
import com.ezr.util.Utils
import org.apache.log4j.Logger
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Messi on 2018/11/22.
  */
object ClassifierModel {
  implicit val logger = Logger.getLogger(ClassifierModel.getClass)
  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(args.length - 1)
    val subject = Utils.getSubject(args)
    val spark = SparkSession.builder().appName("ClassifierModel" + shardingGrpId + subject).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    val tableResult = ConfigHelper.env + shardingGrpId + ConfigHelper.vipResults
    val sample = spark.sql(s"select score, center from  $tableResult  where subject = $subject")
    trainModel(spark, sample, args, shardingGrpId)
    logger.info(s"主题: $subject " + "分类模型已保存在\t" + s"/messi/$subject" + "\t下, 缓存数据已释放")
    spark.stop()
  }

  /**
    * 依据聚类结果做训决策树模型训练, 并保存模型
    * model.save("/messi/DecisionTreeModel" + Utils.getSubject(args) + "/")
    * @param spark
    * @param df
    * @param args
    * @param shardingGrpId
    * @return
    */
  def trainModel(spark: SparkSession, df: DataFrame, args: Array[String], shardingGrpId: String): DecisionTreeClassificationModel ={
    val dataSet: Dataset[LabeledPoint] = toDataSet(spark, df)
    dataSet.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info("dataSet: " + dataSet.count())
    dataSet.show(20, false)

    val split: Array[Dataset[LabeledPoint]] = dataSet.randomSplit(Array(0.7, 0.3))
    val (trainSet, testSet) = (split(0), split(1))
    logger.info("trainSet: " + trainSet.count())
    trainSet.show(25, false)

    logger.info("testSet: " + testSet.count())
    testSet.show(20, false)

    val model: DecisionTreeClassificationModel = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features").setImpurity("gini").fit(dataSet)
    val predDF : DataFrame = model.transform(dataSet)
    logger.info("predDF : " + predDF .count())
    predDF .show(10000)

    logger.info("label: " + model.getLabelCol + "\t" + "predictionCol: " + model.getPredictionCol)
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol(model.getLabelCol).setPredictionCol(model.getPredictionCol).setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predDF)
    logger.info(s"精确度: $accuracy")

    println("决策树模型: " + model.toDebugString)
    dataSet.unpersist()
    model
  }

  /**
    * 获取聚类数据 -> LabeledPoint
    * @param spark
    * @param df
    * @return
    */
  def toDataSet(spark: SparkSession, df: DataFrame): Dataset[LabeledPoint] ={
    import spark.implicits._
    df.rdd.map(v=>{
      val score = v.getAs[Double]("score")
      val center = v.getAs[Int]("center")
      LabeledPoint(center, Vectors.dense(score))
    }).toDS()
  }
}
