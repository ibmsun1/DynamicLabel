package com.ezr.actwilling

import java.util

import com.ezr.config.ConfigHelper
import com.ezr.util.{EtlUtil, Utils}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Messi on 2018/11/22.
  */
object SerComment {

  def main(args: Array[String]): Unit = {
    val shardingGrpId = args(0)
    val spark = SparkSession.builder().appName("DecisionTreeClassifierTwoAndTree" + shardingGrpId).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    getSerComment(spark, shardingGrpId)
    spark.stop()
  }

  /**
    * 订单评价
    * @param spark
    * @param shardingGrpId
    */
  def getSerComment(spark: SparkSession, shardingGrpId: String) = {

    val tbl = ConfigHelper.env + shardingGrpId + ConfigHelper.serComment
    val tblTmp = ConfigHelper.env + shardingGrpId + ConfigHelper.serCommentTemp
    val table = ConfigHelper.env + shardingGrpId + ConfigHelper.behavior3

    val columns = new util.ArrayList[String]()
    columns.add("vipid")
    columns.add("brandid")
    columns.add("copid")
    columns.add("totalscore")

    val serComment = EtlUtil.getDataFromHive(spark, columns, tbl, null)
    val serCommentTemp = EtlUtil.getDataFromHive(spark, columns, tblTmp, null)

    val rowRDD = serComment.union(serCommentTemp).rdd.map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val totalscore = v.getAs[java.math.BigDecimal]("totalscore").doubleValue().toInt
      Row(vipid, brandid, copid, 17, totalscore)
    })

    Utils.hiveSets(spark, ConfigHelper.env + shardingGrpId, "SerComment" + shardingGrpId)
    Utils.toBehavior(spark, rowRDD, table)
  }
}
