package com.ezr.etl

import com.ezr.config.ConfigHelper
import com.ezr.util.Utils
import org.apache.log4j.Logger
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
  def sample(spark: SparkSession, sampleDF: DataFrame) = {
    import spark.implicits._
    val  sample: DataFrame = sampleDF.rdd.map(f = v => {
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val offRate = v.getAs[Double]("couponrate") * 100.00
      val offs = v.getAs[Int]("couponcount").toDouble
      var offAvgTime: Double = v.getAs[Double]("avgtimelength")
      offAvgTime = if(offAvgTime <1 &&  offAvgTime >= 0) 1.00 else offAvgTime
      var offLast = v.getAs[Int]("lastdistancecurrentdays").toDouble
      offLast = if (offLast <0 ) 0.00 else if(offLast >= 0) 1.00 else offLast
      (vipid, brandid, copid, offRate, offs, offAvgTime, offLast)
    }).toDF("vipid", "brandid", "copid", "offRate", "offs", "offAvgTime", "offLast")
    logger.info("sample -> count: " + sample.count())
    sample.show(7, false)

    val offS: DataFrame = sample.select("offs").rdd.zipWithIndex().map(v=>{
      val Id = v._2
      val offs = v._1.getAs[Double]("offs")
      (Id, Vectors.dense(offs))
    }).toDF("Id", "offs")
    logger.info("offS -> count: " + offS.count())
    offS.show(7, false)

    val offAvgTime: DataFrame = sample.select("offAvgTime").rdd.zipWithIndex().map(v=>{
      val Id = v._2
      val offAvgTime = v._1.getAs[Double]("offAvgTime")
      (Id, Vectors.dense(offAvgTime))
    }).toDF("Id", "offAvgTime")
    logger.info("offAvgTime -> count: " + offAvgTime.count())
    offAvgTime.show(7, false)

    val offLast = sample.select("offLast").rdd.zipWithIndex().map(v=>{
      val Id = v._2
      val offLast = v._1.getAs[Double]("offLast")
      (Id, Vectors.dense(offLast))
    }).toDF("Id", "offLast")
    logger.info("offLast -> count: " + offLast.count())
    offLast.show(7, false)

    val offT = ("offs", "offT")
    val avgT = ("offAvgTime", "avgT")
    val lastT = ("offLast", "lastT")
    (sample, (offT, offS), (avgT, offAvgTime), (lastT, offLast))
  }

  /**
    * 对[核销券数, 券核销平均时长, 最后一次核销时间] 归一到 [1, 100]
    * @param spark
    * @param scaleDF
    * @return
    */
  def scale(spark: SparkSession, scaleDF: DataFrame) = {
    val mnx = (1.00, 100.00)
    val splAndTCls = sample(spark, scaleDF)

    val offs = splAndTCls._2
    val offAvgTime = splAndTCls._3
    val offLst = splAndTCls._4

    val modelOffs = Utils.scaler(offs._1._1, offs._1._2, mnx._1, mnx._2, offs._2)
    logger.info("modelOffs -> count: " + modelOffs.count())
    modelOffs.show(7, false)

    val modelAvgTime = Utils.scaler(offAvgTime._1._1, offAvgTime._1._2, mnx._1, mnx._2, offAvgTime._2)
    logger.info("modelAvgTime -> count: " + modelAvgTime.count())
    modelAvgTime.show(7, false)

    val modelLst = Utils.scaler(offLst._1._1, offLst._1._2, mnx._1, mnx._2, offLst._2)
    logger.info("modelLst -> count: " + modelLst.count())
    modelLst.show(7, false)

    (splAndTCls._1, (modelOffs, modelAvgTime, modelLst))
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
    * 打分公式：计算构成雷达图面积的2倍, 券核销率 A, 券数 B, 由于平均时长和最后一次核销时间负相关故取倒数 1/C 和 1/D
    * Score = (A + B) * (1/C + 1/D)
    * @param spark
    * @param dataFrame
    * @param shardGrpId
    */
  def process(spark: SparkSession, actType:Array[String], dataFrame: DataFrame, shardGrpId: String) ={
    import spark.implicits._
    val scales = scale(spark, dataFrame)
    val sample: RDD[(Long, (String, Double))] = scales._1.select("vipid", "brandid", "copid", "offRate").rdd.zipWithIndex().map(_.swap).map(v=>{
      val id = v._1
      val vipid = v._2.getAs[Long]("vipid")
      val brandid = v._2.getAs[Int]("brandid")
      val copid = v._2.getAs[Int]("copid")
      val key = "%s_%s_%s".format(vipid, brandid, copid)
      val offRate = v._2.getAs[Double]("offRate")
      (id, (key, offRate))
    })

    val offs: RDD[(Long, Double)] = scales._2._1.rdd.map(v=>{
      val id = v.getAs[Long]("Id")
      val offT  = v.getAs[DenseVector]("offT").values(0)
      (id, offT)
    })

    val avgTime: RDD[(Long, Double)] = scales._2._2.rdd.map(v=>{
      val id = v.getAs[Long]("Id")
      val avgT  = v.getAs[DenseVector]("avgT").values(0)
      (id, avgT)
    })

    val last: RDD[(Long, Double)] = scales._2._3.rdd.map(v=>{
      val id = v.getAs[Long]("Id")
      val lastT = v.getAs[DenseVector]("lastT").values(0)
      (id, lastT)
    })

    val s: RDD[(Long, (String, Double, Double))] = sample.join(offs).map(v=>{
      val id = v._1
      val key = v._2._1._1
      val offRate = v._2._1._2
      val offT = v._2._2
      (id, (key, offRate, offT))
    })

    val avg: RDD[(Long, (String, Double, Double, Double))] = s.join(avgTime).map(v=>{
      val id = v._1
      val key = v._2._1._1
      val offRate = v._2._1._2
      val offT = v._2._1._3
      val avgT = v._2._2
      (id, (key, offRate, offT, avgT))
    })

    val lst: RDD[(String, Double, Double, Double, Double)] = avg.join(last).map(v=>{
      val key = v._2._1._1
      val offRate = v._2._1._2
      val offT = v._2._1._3
      val avgT = v._2._1._4
      val lastT =  v._2._2
      (key, offRate, offT, avgT, lastT)
    })

    val w = lst.map(v=>{
      val key = v._1
      val offRate = v._2
      val offs = v._3
      val offAvgTime = v._4
      val lastT = v._5
      val score = getScore(offRate, offs, offAvgTime, lastT)
      (key, score)
    }).toDF("keyVip", "score")
    w.show(9, false)

    val wScaler = w.select("score").rdd.zipWithIndex().map(_.swap).map(v=>{
      val id = v._1
      val score = v._2.getAs[Double]("score")
      (id, Vectors.dense(score))
    }).toDF("id", "score")
    logger.info("wScaler -> count: " + "\t" + wScaler.count())
    wScaler.show(7, false)

    val mnx = (1.00, 100.00)
    val scaledData = Utils.scaler("score", "features", mnx._1, mnx._2, wScaler)
    logger.info("scaledData -> count: " + scaledData.count())
    scaledData.show(10, false)

    val scaleD = scaledData.rdd.zipWithIndex().map(_.swap).map(v=>{
      val id = v._1
      val score = v._2.getAs[DenseVector]("features").values(0)
      (id, score)
    })

    val write: DataFrame = w.select("keyVip").rdd.zipWithIndex().map(_.swap).map(v=>{
      val id = v._1
      val key = v._2.getAs[String]("keyVip")
      (id, key)
    }).join(scaleD).map(v=>{
      val k = v._2._1.toString.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      val copid = k(2).toInt
      val score = v._2._2.formatted("%.2f").toDouble
      val subject = 1
      (vipid, brandid, copid, score, subject)
    }).toDF("vipid", "brandid", "copid", "score", "subject")
    write.show(9, false)
    logger.info("writeRDD -> count: " + "\t" + write.count())

    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType),
      StructField("copid", IntegerType), StructField("score", DoubleType), StructField("subject", IntegerType)))
    val writeDF: DataFrame = spark.createDataFrame(write.rdd, schema)
    writeDF.show(3, false)

    val tableScore = ConfigHelper.env + shardGrpId + ConfigHelper.vipScores
    Utils.hiveSets(spark, ConfigHelper.env + shardGrpId, "DelDataBehaviorOne" + shardGrpId + Utils.getSubject(actType))
    writeDF.write.mode(SaveMode.Append).insertInto(tableScore)
  }
}
