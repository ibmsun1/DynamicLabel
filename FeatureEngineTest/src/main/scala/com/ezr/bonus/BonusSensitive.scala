package com.ezr.bonus

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Messi on 2018/11/22.
  */
object BonusSensitive {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val conf = new SparkConf().setAppName("BonusSensitive" + shardingGrpId).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    getBonus(sqlContext, shardingGrpId)
    sc.stop()
  }

  /**
    * 积分
    * @param hiveContext
    * @param shardingGrpId
    */
  def getBonus(hiveContext: HiveContext, shardingGrpId: String) = {

    /**
    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.bonus
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.bonusTemp
    val behavior3 = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior2
    */

    val tbl = "pro.crm_vip_info_bonus1"
    val tblTmp = "pro.crm_vip_info_bonus224"
    val behavior3 = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior2

    val columns = new util.ArrayList[String]()
    columns.add("id")
    columns.add("vipid")
    columns.add("brandid")
    columns.add("copid")
    columns.add("transorigin")
    columns.add("unix_timestamp(lastmodifieddate) lastmodifieddate")

    val rowRDD = EtlUtil.getDistinctDataFromTwoTable(hiveContext, columns, tbl, null, tblTmp, null).filter(v=>{
      val transorigin = v.getAs[Int]("transorigin")
      transorigin == 10 || transorigin == 5 || transorigin == 12
    }).map(v=>{
      var actType = 0
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val transorigin = v.getAs[Int]("transorigin")
      if(transorigin == 10){
        actType = 7
      }else if(transorigin == 5){
        actType = 1
      }else {
        actType = 8
      }
      val key = "%s_%s_%s".format(vipid, brandid, copid)
      (key, (actType, 1))
    }).reduceByKey{
      case (a, b) =>{
        if(a._1 == b._1 && a._1 == 7){
          (7, (a._2 + b._2))
        }else if (a._1 == b._1 && a._1 == 1){
          (1, (a._2 + b._2))
        }else {
          (8, (a._2 + b._2))
        }
      }
    }.map(v=>{
      val k = v._1.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      val copid = k(2).toInt
      val actType = v._2._1
      val count = v._2._2
      Row(vipid, brandid, copid, actType, count)
    })

    /**
      * Utils.hiveSets(hiveContext, ConfigHelper.env + shardingGrpId, "BonusSensitive" + shardingGrpId)
      */
    Utils.hiveSets(hiveContext, "pro", "BonusSensitive")
    Utils.toBehavior(hiveContext, rowRDD, table)

    val perfRDD = hiveContext.sql(s" select vipid, brandid, copid, count from $behavior3  where acttype = 9").rdd.map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val count = v.getAs[Int]("count")
      Row(vipid, brandid, copid, 6, count)
    })

    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType),
      StructField("acttype", IntegerType), StructField("count", IntegerType)))

    val writeDF = hiveContext.createDataFrame(perfRDD, schema)
    writeDF.show(3, false)
    writeDF.write.mode(SaveMode.Append).insertInto(table)
  }
}
