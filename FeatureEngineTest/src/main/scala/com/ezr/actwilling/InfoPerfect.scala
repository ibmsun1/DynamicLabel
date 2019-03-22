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
object InfoPerfect {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val conf = new SparkConf().setAppName("InfoPerfect" + shardingGrpId).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    getInfoPerfect(sqlContext, shardingGrpId)
    sc.stop()
  }

  /**
    * 完善资料
    * @param hiveContext
    * @param shardingGrpId
    */
  def getInfoPerfect(hiveContext: HiveContext, shardingGrpId: String) ={

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.infoPerfect
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.infoPerfectTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("id")
    columns.add("copid")
    columns.add("brandid")
    columns.add("verid")
    columns.add("unix_timestamp(lastmodifieddate) lastmodifieddate")
    val rowRDD = EtlUtil.getDistinctDataFromTwoTable(hiveContext, columns, tbl, null, tblTmp, null).map(v=>{
      val vipid = v.getAs[Int]("id").toLong
      val copid = v.getAs[Int]("copid")
      val brandid = v.getAs[Int]("brandid")
      val verid = v.getAs[Int]("verid")
      Row(vipid, brandid, copid, 9, verid)
    })
    Utils.hiveSets(hiveContext, ConfigHelper.env + shardingGrpId, "InfoPerfect" + shardingGrpId)
    Utils.toBehavior(hiveContext, rowRDD, table)

  }

}
