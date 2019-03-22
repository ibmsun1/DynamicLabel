package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by Messi on 2018/11/22.
  */
object ShopGuide {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val spark = SparkSession.builder().appName("ShopGuide" + shardingGrpId).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    getGuide(spark, shardingGrpId)
    spark.stop()
  }

  /**
    * 联系导购
    * @param spark
    * @param shardingGrpId
    */
  def getGuide(spark: SparkSession, shardingGrpId: String)={
    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.shopGuide
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.shopGuideTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("vipid")
    columns.add("brandid")
    columns.add("shopid")
    columns.add("salerid")
    val guide = EtlUtil.getDataFromHive(spark, columns, tbl, null)
    val guideTemp = EtlUtil.getDataFromHive(spark, columns, tblTmp, null)

    val unionRDD = guide.union(guideTemp).rdd
    val rowRDD = unionRDD.map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val shopid = v.getAs[Int]("shopid")
      val salerid = v.getAs[Int]("salerid")
      val key = "%s_%s_%s_%s".format(vipid, brandid, shopid, salerid)
      (key, 1)
    }).reduceByKey((_ + _)).map(v=>{
      val k = v._1.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      Row(vipid, brandid, -7, 16, v._2)
    })
    Utils.hiveSets(spark, ConfigHelper.env + shardingGrpId, "ShopGuide" + shardingGrpId)
    Utils.toBehavior(spark, rowRDD, table)

    /**
      * 联系导购中间表, 方便在写入Es时取shopid和salerid字段
      */
    val unionRow = unionRDD.distinct().map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val shopid = v.getAs[Int]("shopid")
      val salerid = v.getAs[Int]("salerid")
      Row(vipid, brandid, shopid, salerid)
    })
    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("shopid", IntegerType), StructField("salerid", IntegerType)))
    val tableMerge = ConfigHelper.env + shardingGrpId + ConfigHelper.shopGuideMerge
    val writeDF =  spark.createDataFrame(unionRow, schema)
    writeDF.show(3, false)
    writeDF.write.mode(SaveMode.Overwrite).insertInto(tableMerge)
  }
}
