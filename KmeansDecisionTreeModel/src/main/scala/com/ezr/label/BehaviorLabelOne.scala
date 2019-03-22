package com.ezr.label

import com.ezr.config.ConfigHelper
import com.ezr.util.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql._

/**
  * Created by Messi on 2018/11/22.
  */
object BehaviorLabelOne {
  implicit val logger = Logger.getLogger(BehaviorLabelOne.getClass)
  def main(args: Array[String]): Unit = {
    val shardGrpId = args(args.length - 1)
    val subject = Utils.getSubject(args)
    val spark = SparkSession.builder().appName("BehaviorLabelOne" + shardGrpId + subject).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    val tableScore = ConfigHelper.env + shardGrpId + ConfigHelper.vipScores
    val scoreDF: Dataset[Row] = spark.sql(s"select vipid, brandid, copid, score from  $tableScore  where subject = $subject")
    classify(spark, args, scoreDF, shardGrpId)
    logger.info("主题: " + subject + "完成人群标签.")
    spark.stop()
  }

  /**
    * 分类数据整理
    * @param spark
    * @param args
    * @param scoreDF
    * @param shardGrpId
    */
  def classify(spark: SparkSession, args:Array[String], scoreDF: DataFrame, shardGrpId: String)= {
    val writeRDD = scoreDF.rdd.map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val score = v.getAs[Double]("score")
      val subject = 1
      val interval = Utils.getIntervalBySubject(subject, args)
      val label = Utils.label(score, interval)
      val behavior = score + ": " + "{" + score + ":" + 5 + "}"
      Row(vipid, brandid, copid, score, label, subject, behavior)
    })

    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType),
      StructField("score", DoubleType), StructField("label", StringType), StructField("subject", IntegerType), StructField("behavior", StringType)))

    val writeDF: DataFrame = spark.createDataFrame(writeRDD, schema)
    logger.info("writeDF -> length: " + writeDF.count)
    writeDF.show(3, false)

    val tableLabels = ConfigHelper.env + shardGrpId + ConfigHelper.vipLabels
    Utils.hiveSets(spark, ConfigHelper.env + shardGrpId, "BehaviorLabelOne" + shardGrpId + Utils.getSubject(args))
    writeDF.write.mode(SaveMode.Append).insertInto(tableLabels)
  }
}
