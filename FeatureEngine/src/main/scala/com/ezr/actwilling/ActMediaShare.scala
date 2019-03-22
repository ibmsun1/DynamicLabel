package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Messi on 2018/11/22.
  */
object ActMediaShare {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val spark = SparkSession.builder().appName("ActMediaShare" + shardingGrpId).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    getMediaShare(spark, shardingGrpId)
    spark.stop()
  }

  /**
    * 分享有礼
    * @param spark
    * @param shardingGrpId
    */
  def getMediaShare(spark: SparkSession, shardingGrpId: String) = {

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.actShare
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.actShareTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("id")
    columns.add("vipid")
    columns.add("brandid")
    columns.add("copid")
    columns.add("sharenum")
    columns.add("viewnum")
    columns.add("unix_timestamp(lastmodifieddate) lastmodifieddate")
    val row = EtlUtil.getDistinctDataFromTwoTable(spark, columns, tbl, null, tblTmp, null).map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val sharenum = v.getAs[Int]("sharenum")
      val viewnum = v.getAs[Int]("viewnum")
      (vipid, brandid, copid, sharenum, viewnum)
    })

    val rowRDD1 = row.map(v=>{
      val vipid = v._1
      val brandid = v._2
      val copid = v._3
      val sharenum = v._4
      Row(vipid, brandid, copid, 14, sharenum)
    })
    Utils.toBehavior(spark, rowRDD1, table)

    val rowRDD2 = row.map(v=>{
      val vipid = v._1
      val brandid = v._2
      val copid = v._3
      val viewnum = v._5
      Row(vipid, brandid, copid, 15, viewnum)
    })
    Utils.hiveSets(spark, ConfigHelper.env + shardingGrpId, "ActMediaShare" + shardingGrpId)
    Utils.toBehavior(spark, rowRDD2, table)
  }
}
