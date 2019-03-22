package com.ezr.label

import com.ezr.config.ConfigHelper
import com.ezr.util.Utils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._

/**
  * Created by Messi on 2018/11/22.
  */
object BehaviorLabelsTwoAThree {
  implicit val logger = Logger.getLogger(BehaviorLabelsTwoAThree.getClass)
  def main(args: Array[String]): Unit = {
    val shardGrpId = args(args.length - 1)
    val subject = Utils.getSubject(args)
    val spark = SparkSession.builder().appName("BehaviorLabelsTwoAThree" + shardGrpId + subject).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    val tableBehavior = ConfigHelper.env + shardGrpId + ConfigHelper.vipBehavior
    val delDF: Dataset[Row] = spark.sql(s"select vipid, brandid, copid, score, behavior from  $tableBehavior  where  subject = $subject")
    val tableScore = ConfigHelper.env + shardGrpId + ConfigHelper.vipScores
    val scoreDF: Dataset[Row] = spark.sql(s"select vipid, brandid, copid, score from  $tableScore where subject = $subject ")
    classify(spark, args, delDF, scoreDF, shardGrpId)
    logger.info("主题: " + subject + "完成人群标签.")
    spark.stop()
  }

  /**
    * 数据分类 写入Hive
    * @param spark
    * @param args
    * @param delDF
    * @param scoreDF
    * @param shardGrpId
    */
  def classify(spark: SparkSession, args:Array[String], delDF: DataFrame, scoreDF:DataFrame, shardGrpId: String)={

    val label = scoreDF.rdd.map(v => {
      val vipid = v.getAs[Long]("vipid")
      val score = v.getAs[Double]("score")
      val subject = Utils.getSubject(args)
      val interval = Utils.getIntervalBySubject(subject, args)
      val label = Utils.label(score, interval)
      (vipid, label)})
    logger.info("label: " + label.count() + "\t" + label.first()._1 + "\t:vipId" + label.first()._2 + "\t:label")

    val sample: RDD[(Long, (Int, Int, Double, String))] = Utils.getRDD(delDF).map(v =>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val score = v.getAs[Double]("score")
      val behavior = v.getAs[String]("behavior")
      (vipid,(brandid, copid, score, behavior))})
    logger.info("sample: " + sample.first()._1 + "\t:vipid" + sample.first()._2._1 + "\t:brandid" + sample.first()._2._2 + "\t:copid" + sample.first()._2._3
      + "\t:score" + sample.first()._2._4 + "\t:behavior")

    val behaviorLabel = sample.join(label).map(v =>{
      val vipId = v._1
      val brandId = v._2._1._1
      val copId = v._2._1._2
      val score = v._2._1._3
      val behavior = v._2._1._4
      val label = v._2._2
      val subject = Utils.getSubject(args)
      Row(vipId, brandId, copId, score, label, subject, behavior)})
    logger.info("behaviorLabel: " + behaviorLabel.first().apply(0) + " :vipid\t" + behaviorLabel.first().apply(1) + " :brandid\t" + behaviorLabel.first().apply(2) + " :copid\t" + behaviorLabel.first().apply(3) + " :score\t" + behaviorLabel.first().apply(4) + " :label\t"
      + behaviorLabel.first().apply(5) + " :subject\t" + behaviorLabel.first().apply(6) + "\t:behavior")

    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType),
      StructField("score", DoubleType), StructField("label", StringType), StructField("subject", IntegerType), StructField("behavior", StringType)))

    val behaviorLabelDF: DataFrame = spark.createDataFrame(behaviorLabel, schema)
    logger.info("behaviorLabelDF -> behaviorLabelDF.count: " + behaviorLabelDF.count())
    behaviorLabelDF.show(7, false)

    val tableLabels = ConfigHelper.env + shardGrpId + ConfigHelper.vipLabels
    Utils.hiveSets(spark, ConfigHelper.env + shardGrpId, "BehaviorLabelsTwoAThreeBehaviorLabel" + shardGrpId + Utils.getSubject(args))
    behaviorLabelDF.write.mode(SaveMode.Append).insertInto(tableLabels)
  }
}
