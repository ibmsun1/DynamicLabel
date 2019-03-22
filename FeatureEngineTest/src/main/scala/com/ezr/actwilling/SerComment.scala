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
object SerComment {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val conf = new SparkConf().setAppName("SerComment" + shardingGrpId).set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    getSerComment(sqlContext, shardingGrpId)
    sc.stop()
  }

  /**
    * 订单评价
    * @param hiveContext
    * @param shardingGrpId
    */
  def getSerComment(hiveContext: HiveContext, shardingGrpId: String) = {

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.serComment
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.serCommentTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("vipid")
    columns.add("brandid")
    columns.add("copid")
    columns.add("totalscore")

    val serComment = EtlUtil.getDataFromHive(hiveContext, columns, tbl, null).rdd
    val serCommentTemp = EtlUtil.getDataFromHive(hiveContext, columns, tblTmp, null).rdd

    val rowRDD = serComment.union(serCommentTemp).map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val totalscore = v.getAs[java.math.BigDecimal]("totalscore").doubleValue().toInt
      Row(vipid, brandid, copid, 17, totalscore)
    })

    Utils.hiveSets(hiveContext, ConfigHelper.env + shardingGrpId, "SerComment" + shardingGrpId)
    Utils.toBehavior(hiveContext, rowRDD, table)
  }
}
