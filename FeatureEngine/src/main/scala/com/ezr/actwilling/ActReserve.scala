package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Messi on 2018/11/22.
  */
object ActReserve {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val spark = SparkSession.builder().appName("ActReserve" + shardingGrpId).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    getActReserve(spark, shardingGrpId)
    spark.stop()
  }

  /**
    * 预约
    * @param spark
    * @param shardingGrpId
    */
  def getActReserve(spark: SparkSession, shardingGrpId: String) = {

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

    val rowRDD = EtlUtil.getDistinctDataFromTwoTable(spark, columns, tbl, null, tblTmp, null).filter(v=>{
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
    Utils.hiveSets(spark, ConfigHelper.env + shardingGrpId, "ActReserve" + shardingGrpId)
    Utils.toBehavior(spark, rowRDD, table)
  }
}
