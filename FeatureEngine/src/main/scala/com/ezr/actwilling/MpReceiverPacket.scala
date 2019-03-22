package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Messi on 2018/11/22.
  */
object MpReceiverPacket {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val spark = SparkSession.builder().appName("MpReceiverPacket" + shardingGrpId).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    getReceivePacket(spark, shardingGrpId)
    spark.stop()
  }

  /**
    * 现金红包
    * @param spark
    * @param shardingGrpId
    */
  def getReceivePacket(spark: SparkSession, shardingGrpId: String) = {

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.receivePacket
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.receivePacketTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("id")
    columns.add("brandid")
    columns.add("copid")
    val receivePacket = EtlUtil.getDataFromHive(spark, columns, tbl, null)
    val receivePacketTemp = EtlUtil.getDataFromHive(spark, columns, tblTmp, null)

    val rowRDD = receivePacket.union(receivePacketTemp).rdd.map(v=>{
      val vipid = v.getAs[Long]("id")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val key = "%s_%s_%s".format(vipid, brandid, copid)
      (key, 1)
    }).reduceByKey(_ + _).map(v=>{
      val k = v._1.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      val copid = k(2).toInt
      val count = v._2
      Row(vipid, brandid, copid, 13, count)
    })
    Utils.hiveSets(spark, ConfigHelper.env + shardingGrpId, "MpReceiverPacket" + shardingGrpId)
    Utils.toBehavior(spark, rowRDD, table)
  }
}
