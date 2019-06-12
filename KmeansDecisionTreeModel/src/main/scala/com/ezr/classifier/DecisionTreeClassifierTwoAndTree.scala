package com.ezr.classifier

import com.ezr.config.ConfigHelper
import com.ezr.model.ClassifierModel
import com.ezr.util.Utils
import org.apache.log4j.Logger
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
  * Created by Messi on 2018/11/22.
  */
object DecisionTreeClassifierTwoAndTree {

  implicit val logger = Logger.getLogger(DecisionTreeClassifierTwoAndTree.getClass)
  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(args.length - 1)
    val subject = Utils.getSubject(args)
    val spark = SparkSession.builder().appName("DecisionTreeClassifierTwoAndTree" + shardingGrpId + subject).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    val tableResult = ConfigHelper.env + shardingGrpId + ConfigHelper.vipResults
    val resultDF = spark.sql(s"select score, center from  $tableResult  where subject = $subject")
    val tableScore = ConfigHelper.env + shardingGrpId + ConfigHelper.vipScores
    val scoreDF = spark.sql(s"select score  from  $tableScore  where subject = $subject")
    val tableBehavior = ConfigHelper.env + shardingGrpId + ConfigHelper.vipBehavior
    val sampleDF = spark.sql(s"select *  from  $tableBehavior  where subject = $subject")
    label(spark, resultDF, scoreDF, sampleDF, args, shardingGrpId)
    spark.stop()
  }

  /**
    * 分类后整理数据
    * @param spark
    * @param resultDF
    * @param scoreDF
    * @param sampleDF
    * @param args
    * @param shardingGrpId
    */
  def label(spark: SparkSession, resultDF: DataFrame, scoreDF: DataFrame, sampleDF: DataFrame, args: Array[String], shardingGrpId: String) = {
    import spark.implicits._

    val subject = Utils.getSubject(args)
    val model: DecisionTreeClassificationModel = DecisionTreeClassificationModel.load("hdfs://uhadoop-0f1pin-master1:8020/classifier_" + shardingGrpId + "_" + subject)
//    val model: DecisionTreeClassificationModel = ClassifierModel.trainModel(spark, resultDF, args, shardingGrpId)
    val transDS = scoreDF.rdd.zipWithIndex().map(_.swap).map(v=>{
      val id = v._1
      val features = Vectors.dense(v._2.getDouble(0))
      (id, features)
    }).toDF("id", "features")

    val transDF: DataFrame = model.transform(transDS)
    transDF.show(100, false)

    val clusterRDD: RDD[(Long, Double)] = transDF.rdd.map(v=>{
      v.getAs[Double]("prediction")
    }).zipWithIndex().map(_.swap)

    val behaviorRDD: RDD[(Long, (Long, Int, Int, Double, Int, String))] = sampleDF.rdd.map(v=>{
      val vipId = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val score = v.getAs[Double]("score")
      val subject = v.getAs[Int]("subject")
      val behavior = v.getAs[String]("behavior")
      (vipId, brandid, copid, score, subject, behavior)
    }).zipWithIndex().map(_.swap)

    val labelRDD = behaviorRDD.join(clusterRDD).map(v=>{
      val vipId = v._2._1._1
      val brandid = v._2._1._2
      val copid = v._2._1._3
      val score = v._2._1._4
      val center = v._2._2.toInt
      val subject = v._2._1._5
      val behavior = v._2._1._6
      Row(vipId, brandid, copid, score, center, subject, behavior)
    })

    val schemaLabels: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType),
      StructField("score", DoubleType), StructField("center", IntegerType), StructField("subject", IntegerType), StructField("behavior", StringType)))

    val LabelDF: DataFrame = spark.createDataFrame(labelRDD, schemaLabels)
    logger.info("LabelDF -> count: " + LabelDF.count())
    LabelDF.show(10, false)

    val tableLabels = ConfigHelper.env + shardingGrpId + ConfigHelper.vipLabels
    Utils.hiveSets(spark, ConfigHelper.env + shardingGrpId, "DecisionTreeClassifierTwoAndTreeBehaviorLabels" + shardingGrpId + Utils.getSubject(args))
    LabelDF.write.mode(SaveMode.Append).insertInto(tableLabels)
  }
}
