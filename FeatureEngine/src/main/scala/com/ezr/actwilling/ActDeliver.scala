package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Messi on 2018/11/22.
  */
object ActDeliver {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val spark = SparkSession.builder().appName("ActDeliver" + shardingGrpId).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    getActDeliver(spark, shardingGrpId)
    spark.stop()
  }

  /**
    * 游戏参与度
    * @param spark
    * @param shardingGrpId
    */
  def getActDeliver(spark: SparkSession, shardingGrpId: String) ={

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.actDeliver
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.actDeliverTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("vipid")
    columns.add("brandid")
    columns.add("copid")
    columns.add("acttype")
    val actDeliver = EtlUtil.getDataFromHive(spark, columns, tbl, null)
    val actDeliverTemp = EtlUtil.getDataFromHive(spark, columns, tblTmp, null)

    val rowRDD = actDeliver.union(actDeliverTemp).rdd.filter(v=>{
      val actType = v.getAs[Int]("acttype")
      actType == 4 || actType == 3 || actType == 2
    }).map(v=>{
      var actType = 0
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val acttype = v.getAs[Int]("acttype")
      if(acttype == 4){
        actType = 4
      }else if (acttype == 3){
        actType = 3
      }else if (acttype == 2){
        actType = 2
      }
      val key = "%s_%s_%s".format(vipid, brandid, copid)
      (key, (actType, 1))
    }).reduceByKey{
      case (a, b) =>{
        if(a._1 == b._1 && a._1 == 4){
          (4, (a._2 + b._2))
        }else if (a._1 == b._1 && a._1 == 3){
          (3, (a._2 + b._2))
        }else {
          (2, (a._2 + b._2))
        }
      }
    }.map(v=>{
      val k = v._1.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      val copid = k(2).toInt
      val acttype = v._2._1
      val count = v._2._2
      Row(vipid, brandid, copid, acttype, count)
    })
    Utils.hiveSets(spark, ConfigHelper.env + shardingGrpId, "ActDeliver" + shardingGrpId)
    Utils.toBehavior(spark, rowRDD, table)
  }
}
