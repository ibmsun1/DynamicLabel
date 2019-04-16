package com.ezr.etl

import com.ezr.config.ConfigHelper
import com.ezr.util.Utils
import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * Created by Messi on 2018/11/22,
  * 主题1, 数据处理, 归一化后打分, 写入打分表, 用于聚类.
  */
object DelDataBehaviorOne {
  implicit val logger = Logger.getLogger(DelDataBehaviorOne.getClass)
  def main(args: Array[String]): Unit = {
    val shardGrpId = args(args.length - 1)
    val subject = Utils.getSubject(args)
    implicit val spark = SparkSession.builder().appName("DelDataBehaviorOne" + shardGrpId + subject).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    val table = ConfigHelper.env + shardGrpId + ConfigHelper.behavior1
    val sampleDF = spark.sql(s"select vipid, brandid, copid, couponrate, couponcount, avgtimelength, lastdistancecurrentdays from  $table")
    process(spark, args, sampleDF, shardGrpId)
    logger.info(s"主题: $subject " + "打分完毕.")
    spark.stop()
  }

  /**
    * 数据整理
    * 券核销率 * 100
    * 将最后一次核销时间为-1 调整为0.0
    * 用于归一[1, 100]
    * @param spark
    * @param sampleDF
    * @return
    */
  def getSample(spark: SparkSession, sampleDF: DataFrame) = {
    import spark.implicits._
    val sample = sampleDF.rdd.map(f = v => {
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val offRate = v.getAs[Double]("couponrate") * 100.00
      val offs = v.getAs[Int]("couponcount").toDouble
      var offAvgTime: Double = v.getAs[Double]("avgtimelength")
      offAvgTime = if(offAvgTime < 1 &&  offAvgTime >= 0) 1.00 else offAvgTime
      var offLast = v.getAs[Int]("lastdistancecurrentdays").toDouble
      offLast = if (offLast < 0 ) 0.00 else if(offLast >= 0 && offLast <= 1) 1.00 else offLast
      (vipid, brandid, copid, offRate, offs, offAvgTime, offLast)
    })

    val scaleSample = sample.map(v=>{
      val offRate = v._4
      val offs = v._5
      val offAvgTime: Double = v._6
      val offLast = v._7
      (offRate, offs, offAvgTime, offLast)
    }).zipWithIndex()
      .map(v=>{
        val index = v._2
        val offRate = v._1._1
        val offs = v._1._2
        val offAvgTime: Double = v._1._3
        val offLast = v._1._4
        (index, Vectors.dense(offRate, offs, offAvgTime, offLast))
      }).toDF("id", "features")
    logger.info("scaleSample -> count: " + scaleSample.count())
    scaleSample.show(7, false)

    /*对[核销券数, 券核销平均时长, 最后一次核销时间] 归一到 [1, 100]*/
    val mnx = (1.00, 100.00)
    val scaleData = Utils.scaler("features", "scaledFeatures", mnx._1, mnx._2, scaleSample)
    logger.info("scaleData -> count: " + scaleData.count())
    scaleData.show(7, false)

    (sample, scaleData)
  }

  /**
    * 打分公式
    * @param s
    * @return
    */
  def getScore(s: (Double, Double, Double, Double)): Double = {
    var score: Double = if (s._4 == 0) 0 else
      (s._1 + s._2) * (1 / s._3 + 1 / s._4)
    score = score.formatted("%.2f").toDouble
    score
  }

  /**
    * 打分并整合数据
    * 打分公式：计算构成雷达图面积的2倍, 券核销率 A, 核销券数 B, 由于平均时长和最后一次核销时间负相关故取倒数 1/C 和 1/D
    * Score = (A + B) * (1/C + 1/D)
    * @param spark
    * @param dataFrame
    * @param shardGrpId
    */
  def process(spark: SparkSession, actType:Array[String], dataFrame: DataFrame, shardGrpId: String) ={
    import spark.implicits._
    val scales = getSample(spark, dataFrame)

    /*对归一完成[核销券数, 券核销平均时长, 最后一次核销时间]进行打分*/
    val scoreScaler = scales._2.select("scaledFeatures").rdd.map(v=>{
      val feature = v.getAs[DenseVector]("scaledFeatures")
      val offRate = feature(0)
      val offs = feature(1)
      val offAvgTime: Double = feature(2)
      val offLast = feature(3)
      getScore(offRate, offs, offAvgTime, offLast)
    }).zipWithIndex().map(v=>{
      (v._2, Vectors.dense(v._1))
    }).toDF("id", "score")

    /*打分完成后做数据处理, 都保证在[1.00, 100.00]之间*/
    val score = Utils.scaler("score", "scaleDScore", 1.00, 100.00, scoreScaler)
      .select("scaleDScore").rdd.zipWithIndex()
      .map(v=>{
        val id = v._2
        val score = v._1.getAs[DenseVector]("scaleDScore")(0)
        (id, score)
      })

    val sample = scales._1.zipWithIndex().map(v=>{
      val id = v._2
      val vipid = v._1._1
      val brandid = v._1._2
      val copid = v._1._3
      (id, (vipid, brandid, copid))
    })

    val write = sample.join(score).map(v=>{
      val vipid = v._2._1._1
      val brandid = v._2._1._2
      val copid = v._2._1._3
      val score = v._2._2
      Row(vipid, brandid, copid, score, 1)
    })

    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType),
      StructField("copid", IntegerType), StructField("score", DoubleType), StructField("subject", IntegerType)))
    val writeDF: DataFrame = spark.createDataFrame(write, schema)
    writeDF.show(3, false)

    val tableScore = ConfigHelper.env + shardGrpId + ConfigHelper.vipScores
    Utils.hiveSets(spark, ConfigHelper.env + shardGrpId, "DelDataBehaviorOne" + shardGrpId + Utils.getSubject(actType))
    writeDF.write.mode(SaveMode.Append).insertInto(tableScore)
  }
}
