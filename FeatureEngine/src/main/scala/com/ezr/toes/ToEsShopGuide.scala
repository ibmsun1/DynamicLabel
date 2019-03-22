package com.ezr.toes

import com.ezr.config.ConfigHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._

object ToEsShopGuide {

  case class ToEs(vipid: Long, brandid: Int, copid: Int, score: Double, center: Int, subject: Int, behavior: String)
  case class ToEsShopGuideShopSaler(vipid: Long, brandid: Int, copid: Int, shopid: Int, salerid: Int, score: Double, center: Int, subject: Int, behavior: String)
  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val esIp = "192.168.12.181"
    val esPort = "9200"
    val spark = SparkSession.builder().appName("ToEsShopGuide" + shardingGrpId).config("es.nodes", esIp).config("es.port", esPort).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    val guideMergeTable = ConfigHelper.env + shardingGrpId + ConfigHelper.shopGuideMerge
    val shopMergeDF = spark.sql(s"select * from  $guideMergeTable")
    val labelTable = ConfigHelper.env + shardingGrpId + ConfigHelper.vipLabels
    val labelDF = spark.sql(s"select * from  $labelTable")
    toEs(spark, labelDF, shopMergeDF, shardingGrpId)
    spark.stop()
  }

  /**
    * 写入Es
    * @param spark
    * @param labelDF
    * @param shopMergeDF
    * @param shardingGrpId
    * @return
    */
  def toEs(spark: SparkSession, labelDF: DataFrame, shopMergeDF: DataFrame, shardingGrpId: String) = {
    val shop = shopMergeDF.rdd.map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = -7
      val shopid = v.getAs[Int]("shopid")
      val salerid = v.getAs[Int]("salerid")
      val key = "%s_%s_%s".format(vipid, brandid, copid)
      (key, (shopid, salerid))
    })

    labelDF.rdd.filter(v=>{
      v.getAs[Int]("copid") == -7
    }).map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val score = v.getAs[Double]("score")
      val center = v.getAs[Int]("center")
      val subject = v.getAs[Int]("subject")
      val behavior = v.getAs[String]("behavior")
      val key = "%s_%s_%s".format(vipid, brandid, copid)
      (key, (score, center, subject, behavior))
    }).join(shop).map(v=>{
      val k = v._1.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      val copid = k(2).toInt
      val shopid = v._2._2._1
      val salerid = v._2._2._2
      val score = v._2._1._1
      val center = v._2._1._2
      val subject = v._2._1._3
      val behavior = v._2._1._4
      ToEsShopGuideShopSaler(vipid, brandid, copid, shopid, salerid, score, center, subject, behavior)
    }).saveToEs("dynamiclabel" + shardingGrpId + "/label",Map("es.mapping.id" -> "id"))

    labelDF.rdd.filter(v=>{v.getAs[Int]("copid") != -7}).map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val score = v.getAs[Double]("score")
      val center = v.getAs[Int]("center")
      val subject = v.getAs[Int]("subject")
      val behavior = v.getAs[String]("behavior")
      ToEs(vipid, brandid, copid, score, center, subject, behavior)
    }).saveToEs("dynamiclabel" + shardingGrpId + "/label",Map("es.mapping.id" -> "id"))
  }
}
