package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Messi on 2018/11/22.
  */
object MpReceiverPacket {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val conf = new SparkConf().setAppName("MpReceiverPacket" + shardingGrpId).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    getReceivePacket(sqlContext, shardingGrpId)
    sc.stop()
  }

  /**
    * 现金红包
    * @param hiveContext
    * @param shardingGrpId
    */
  def getReceivePacket(hiveContext: HiveContext, shardingGrpId: String) = {

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.receivePacket
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.receivePacketTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("id")
    columns.add("brandid")
    columns.add("copid")
    val receivePacket = EtlUtil.getDataFromHive(hiveContext, columns, tbl, null).rdd
    val receivePacketTemp = EtlUtil.getDataFromHive(hiveContext, columns, tblTmp, null).rdd

    val rowRDD = receivePacket.union(receivePacketTemp).map(v=>{
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
    Utils.hiveSets(hiveContext, ConfigHelper.env + shardingGrpId, "MpReceiverPacket" + shardingGrpId)
    Utils.toBehavior(hiveContext, rowRDD, table)
  }
}
