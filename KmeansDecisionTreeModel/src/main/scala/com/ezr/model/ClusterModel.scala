package com.ezr.model

import com.ezr.config.ConfigHelper
import com.ezr.util.Utils
import org.apache.log4j.Logger
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by Messi on 2018/11/22.
  */
object ClusterModel {
  implicit val logger = Logger.getLogger(ClusterModel.getClass)
  def main(args: Array[String]): Unit = {

    val shardGrpId = args(args.length - 1)
    var numClusters = 5
    if (null != args(0)) numClusters = args(0).toInt else 5
    var numIterations = 300
    if (null != args(1)) numIterations = args(1).toInt else 300
    var runs = 10
    if (null != args(2)) runs = args(2).toInt else 10

    val subject = Utils.getSubject(args)
    implicit val spark = SparkSession.builder().appName("ClusterModel" + shardGrpId + subject).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    val tableScore = ConfigHelper.env + shardGrpId + ConfigHelper.vipScores
    trainModel(spark, args: Array[String], spark.sql(s"select vipid, brandid, copid, score from  $tableScore  where subject = $subject "), numClusters, numIterations, runs, shardGrpId)
    logger.info(s"主题: $subject " + "聚类完成, 并释放缓存数据.")
    spark.stop()
  }

  /**
    * Kmeans++聚类
    * @param spark
    * @param args
    * @param sampleDF
    * @param numClusters
    * @param numIterations
    * @param runs
    * @param shardGrpId
    * @return
    */
  def trainModel(spark: SparkSession, args: Array[String], sampleDF: DataFrame, numClusters: Int, numIterations: Int, runs: Int, shardGrpId:String) ={
    sampleDF.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info("sampleDF: " + sampleDF.count())
    sampleDF.show(7, false)

    val trainDF = getTrainDF(spark, sampleDF)
    val model: KMeansModel = new KMeans().setK(numClusters).setMaxIter(numIterations).setInitSteps(runs)
      .setFeaturesCol("score").setPredictionCol("center").fit(trainDF)
    val sse: Double = model.computeCost(trainDF)
    logger.info(s"簇内误差平方和: \n$sse")

    val centers: Array[linalg.Vector] = model.clusterCenters
    centers.foreach(centers => {logger.info(s"聚类中心: \n$centers")})

    val transDF: DataFrame = model.transform(trainDF)
    logger.info("transDF ->count :" + transDF.count().toString)
    transDF.show(7, false)

    val trans = transDF.rdd.map(v => {
      val score = v.getAs[DenseVector]("score").values(0)
      val center = v.getAs[Int]("center")
      (score, center)
    }).zipWithIndex().map(_.swap)

    val subject = Utils.getSubject(args)
    val transRow = sampleDF.select("vipid", "brandid", "copid").rdd.zipWithIndex().map(v=>{
      val vipid = v._1.getAs[Long]("vipid")
      val brandid = v._1.getAs[Int]("brandid")
      val copid = v._1.getAs[Int]("copid")
      val key = "%s_%s_%s".format(vipid, brandid, copid)
      (v._2, key)
    }).join(trans).map(v=>{
      val k = v._2._1.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      val copid = k(2).toInt
      val score = v._2._2._1
      val center = v._2._2._2
      Row(vipid, brandid, copid, score, center, subject)
    })

    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType), StructField("score", DoubleType), StructField("center", IntegerType), StructField("subject", IntegerType)))
    val tableResult = ConfigHelper.env + shardGrpId + ConfigHelper.vipResults
    Utils.hiveSets(spark, ConfigHelper.env + shardGrpId, "ClusterModelBehaviorResult" + shardGrpId + Utils.getSubject(args))
    spark.createDataFrame(transRow, schema).write.mode(SaveMode.Append).insertInto(tableResult)

    /*保存聚类模型*/
    model.save("hdfs://uhadoop-0f1pin-master1:8020/cluster_" + shardGrpId + "_" + subject)
    trainDF.unpersist()
  }

  /**
    * 将传入的DataFrame转化为矩阵
    * @param spark
    * @param df
    * @return
    */
  def getTrainDF(spark: SparkSession, df: DataFrame):DataFrame = {
    import spark.implicits._
    df.select("score").rdd.zipWithIndex().map(v=>{
      val score = v._1.getAs[Double]("score")
      (v._2, Vectors.dense(score))
    }).toDF("id", "score")
  }
}
