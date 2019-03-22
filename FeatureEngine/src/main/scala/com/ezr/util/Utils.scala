package com.ezr.util

import com.ezr.config.ConfigHelper
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

/**
  * Created by Messi on 2018/11/22.
  */
object Utils {

  implicit val logger = Logger.getLogger(Utils.getClass)

  /**
    * 根据主题类型选取ods表
    * @param subject
    * @return
    */
  def getBehaviorTbl(subject: Int): String ={
      val behavior = if(subject == 1){
        ConfigHelper.behavior1
      }else if (subject == 2){
        ConfigHelper.behavior2
      }else if (subject == 3){
        ConfigHelper.behavior3
      }
    behavior.asInstanceOf[String]
  }

  /**
    * Hive优化
    * @param spark
    * @param database
    * @param jobName
    * @return
    */
  def hiveSets(spark: SparkSession, database:String, jobName:String)={
    spark.sql("use " + database)
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.max.dynamic.partitions=1000000")
    spark.sql("SET hive.exec.max.dynamic.partitions.pernode=100000")
    spark.sql("set hive.exec.max.created.files=100000")
    spark.sql("set mapred.job.name=" + jobName)
    spark.sql("set hive.optimize.sort.dynamic.partition=true")
  }
  /**
    * 行为数据汇总, 传入RDD[Row], 写入对应的行为表.
    * @param spark
    * @param rowRDD
    * @param table
    */
  def toBehavior(spark: SparkSession, rowRDD: RDD[Row], table: String)={
    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType),
      StructField("acttype", IntegerType), StructField("count", IntegerType)))

    val writeDF =  spark.createDataFrame(rowRDD, schema)
    writeDF.show(3, false)
    writeDF.write.mode(SaveMode.Append).insertInto(table)
  }

  /**
    * 获取积分主题
    * @param actType
    * @return
    */
  def getSubject(actType:Array[String]):Int ={
    val t = if(actType(3).equals("5")) 1 else if(actType(3).equals("6") && actType.contains("7") && actType.contains("1") && actType.contains("8")) 2
    else if (actType(3).equals("4")  && actType.contains("3") && actType.contains("2") && actType.contains("9") && actType.contains("11") && actType.contains("12")
      && actType.contains("13") && actType.contains("14") && actType.contains("15") && actType.contains("16") && actType.contains("17")) 3
    t.asInstanceOf[Int]
  }
}

