package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row

/**
  * Created by Messi on 2018/11/22.
  */
object ShopGuide {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val conf = new SparkConf().setAppName("ShopGuide" + shardingGrpId).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    getGuide(sqlContext, shardingGrpId)
    sc.stop()
  }

  /**
    * 联系导购
    * @param sqlContext
    * @param shardingGrpId
    */
  def getGuide(sqlContext: HiveContext, shardingGrpId: String)={
    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.shopGuide
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.shopGuideTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("vipid")
    columns.add("brandid")
    columns.add("shopid")
    columns.add("salerid")
    val guide = EtlUtil.getDataFromHive(sqlContext, columns, tbl, null).rdd
    val guideTemp = EtlUtil.getDataFromHive(sqlContext, columns, tblTmp, null).rdd

    val rowRDD = guide.union(guideTemp).map(v=>{
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
    Utils.hiveSets(sqlContext, ConfigHelper.env + shardingGrpId, "ShopGuide" + shardingGrpId)
    Utils.toBehavior(sqlContext, rowRDD, table)
  }

}
