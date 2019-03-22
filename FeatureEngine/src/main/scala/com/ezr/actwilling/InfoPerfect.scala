package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Messi on 2018/11/22.
  */
object InfoPerfect {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val spark = SparkSession.builder().appName("InfoPerfect" + shardingGrpId).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    getInfoPerfect(spark, shardingGrpId)
    spark.stop()
  }

  /**
    * 完善资料
    * @param spark
    * @param shardingGrpId
    */
  def getInfoPerfect(spark: SparkSession, shardingGrpId: String) ={

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.infoPerfect
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.infoPerfectTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("id")
    columns.add("copid")
    columns.add("brandid")
    columns.add("verid")
    columns.add("unix_timestamp(lastmodifieddate) lastmodifieddate")
    val rowRDD = EtlUtil.getDistinctDataFromTwoTable(spark, columns, tbl, null, tblTmp, null).map(v=>{
      val vipid = v.getAs[Int]("id").toLong
      val copid = v.getAs[Int]("copid")
      val brandid = v.getAs[Int]("brandid")
      val verid = v.getAs[Int]("verid")
      Row(vipid, brandid, copid, 9, verid)
    })
    Utils.hiveSets(spark, ConfigHelper.env + shardingGrpId, "InfoPerfect" + shardingGrpId)
    Utils.toBehavior(spark, rowRDD, table)
  }
}
