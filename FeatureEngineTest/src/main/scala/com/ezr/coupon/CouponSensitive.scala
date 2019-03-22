package com.ezr.coupon

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{DateUtil, EtlUtil, Utils}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Try

/**
  * Created by Messi on 2018/11/22.
  */
object CouponSensitive {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val conf = new SparkConf().setAppName("CouponSensitive" + shardingGrpId).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    getCoupon(sqlContext, shardingGrpId)
    sc.stop()
  }

  /**
    * 券
    * @param hiveContext
    * @param shardingGrpId
    */
  def getCoupon(hiveContext: HiveContext, shardingGrpId: String) = {

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.coupon
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.couponTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior1

    val columns = new util.ArrayList[String]()
    columns.add("id")
    columns.add("vipid")
    columns.add("brandid")
    columns.add("copid")
    columns.add("status")
    columns.add("selldate")
    columns.add("vipbinddate")
    columns.add("unix_timestamp(lastmodifieddate) lastmodifieddate")

    val rowRDD = EtlUtil.getDistinctDataFromTwoTable(hiveContext, columns, tbl, null, tblTmp, null).map(v =>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val status = v.getAs[java.lang.Short]("status").toInt
      val vipBindDate = DateTime.parse(Try(v.getAs[String]("vipbinddate").substring(0, 10)).getOrElse("1970-01-02"), DateTimeFormat.forPattern("yyyy-MM-dd"))
      val sellDate = DateTime.parse(Try(v.getAs[String]("selldate").substring(0, 10)).getOrElse("1970-01-02"), DateTimeFormat.forPattern("yyyy-MM-dd"))
      val key = "%s_%s_%s_%s_%s".format(vipid, brandid, copid, vipBindDate, sellDate)
      (key, (status, 1))
    }).reduceByKey{
      case (a, b) =>{
        if (a._1 == b._1 && a._1 == 8){
          (8, (a._2 + b._2))
        }else {
          (a._1, (a._2 + b._2))
        }
      }
    }.map(v=>{
      val k = v._1
      val statusCount = v._2
      var sell = 0
      var noSell = 0
      var rate = ""

      if (statusCount._1 == 8){
        sell = statusCount._2
      }

      if(!(statusCount._1 == 8)){
        noSell = statusCount._2
      }

      val t = k.split("_")
      val key = "%s_%s_%s".format(t(0), t(1), t(2))
      val vipBindDate = DateTime.parse(t(3))
      val sellDate = DateTime.parse(t(4))
      rate = (sell / (noSell + sell.toDouble)).formatted("%.2f")
      val keyRateCountAvgTime = "%s_%s_%s".format(key, rate, sell)
      (keyRateCountAvgTime, (statusCount._1, sell, vipBindDate, sellDate))
    }).filter(_._2._1 == 8).mapValues(v=>{
      val dT = DateUtil.getDaysTuple(v._3, v._4, DateTime.now())
      val avg = dT._1 / v._2
      (avg, dT._2)
    }).groupByKey().mapValues(v=>{
      val m: Map[Int, Int] = v.toMap
      val avgTime = m.keys.sum.toDouble
      val last = m.values.max
      (avgTime, last)
    }).map(v=>{
      val key = v._1
      val k: Array[String] = key.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      val copid = k(2).toInt
      val rate = k(3).toDouble
      val count = k(4).toInt
      val avgTime = v._2._1
      val last = v._2._2
      Row(vipid, brandid, copid, 5, rate, count, avgTime, last)
    })

    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType), StructField("acttype", IntegerType), StructField("couponrate", DoubleType),
      StructField("couponcount", IntegerType), StructField("avgtimelength", DoubleType), StructField("lastdistancecurrentdays", IntegerType)))

    Utils.hiveSets(hiveContext, ConfigHelper.env + shardingGrpId, "CouponSensitive" + shardingGrpId)
    val writeDF = hiveContext.createDataFrame(rowRDD, schema)
    writeDF.show(3, false)
    writeDF.write.mode(SaveMode.Append).insertInto(table)
  }
}
