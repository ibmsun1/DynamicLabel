package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Messi on 2018/11/22.
  */
object ActReserve {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val conf = new SparkConf().setAppName("ActReserve" + shardingGrpId).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    getActReserve(sqlContext, shardingGrpId)
    sc.stop()
  }

  /**
    * 预约
    * @param hiveContext
    * @param shardingGrpId
    */
  def getActReserve(hiveContext: HiveContext, shardingGrpId: String) = {

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.actReserve
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.actReserveTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("id")
    columns.add("vipid")
    columns.add("brandid")
    columns.add("copid")
    columns.add("status")
    columns.add("unix_timestamp(lastmodifieddate) lastmodifieddate")

    val rowRDD = EtlUtil.getDistinctDataFromTwoTable(hiveContext, columns, tbl, null, tblTmp, null).filter(v=>{
      val status = v.getAs[Int]("status")
      status == 1 || status == 4
    }).map(v=>{
      var actType = 0
      var count = 0
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val status = v.getAs[Int]("status")
      if(status == 1){
        actType = 10
        count = 1
      }else if (status == 4){
        actType = 11
        count = 1
      }
     Row(vipid, brandid, copid, actType, count)
    })
    Utils.hiveSets(hiveContext, ConfigHelper.env + shardingGrpId, "ActReserve" + shardingGrpId)
    Utils.toBehavior(hiveContext, rowRDD, table)
  }
}
