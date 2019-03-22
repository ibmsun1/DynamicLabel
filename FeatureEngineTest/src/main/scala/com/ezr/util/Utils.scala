package com.ezr.util

import com.ezr.config.ConfigHelper
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

/**
  * Created by Messi on 2018/11/22.
  */
object Utils {

  implicit val logger = Logger.getLogger(Utils.getClass)

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
    * 根据行为组合
    * 获取actType 个数
    * @param behavior
    */
  def lenActType(behavior: String): Int = {
    var lenActType = 0
    if(!behavior.contains(",")){
      lenActType = 1
    }else{
      lenActType = behavior.split(",").length
    }
    lenActType
  }

  /**
    * 根据主题
    * 获取actType 个数
    * @param subject
    */
  def lenActTypeBySubject(subject: Int): Int ={
    val lenActType =
    if(subject == 1){
      1
    }else if (subject == 2){
      4
    }else if (subject == 3){
      12
    }
    lenActType.asInstanceOf[Int]
  }

  /**
    * 按照主题获取标签分数区间
    * @param subject
    * @param args
    * @return
    */
  def getIntervalBySubject(subject: Int, args: Array[String]): (Int, Int, Int) ={
    var interval: (Int, Int, Int) = (0, 0, 0)
    if(subject == 1){
      interval = (args(4).toInt, args(5).toInt, args(6).toInt)
    }else if(subject == 2){
      interval = (args(7).toInt, args(8).toInt, args(9).toInt)
    }else if(subject == 3){
      interval = (args(15).toInt, args(16).toInt, args(17).toInt)
    }
    interval
  }

  /**
    * Hive优化
    * @param sqlContext
    * @param database
    * @param jobName
    * @return
    */
  def hiveSets(sqlContext: HiveContext, database:String, jobName:String)={
    sqlContext.sql("use " + database)
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sqlContext.sql("set hive.exec.dynamic.partition=true")
    sqlContext.sql("SET hive.exec.max.dynamic.partitions=1000000")
    sqlContext.sql("SET hive.exec.max.dynamic.partitions.pernode=100000")
    sqlContext.sql("set hive.exec.max.created.files=100000")
    sqlContext.sql("set mapred.job.name=" + jobName)
    sqlContext.sql("set hive.optimize.sort.dynamic.partition=true")
  }
  /**
    * 行为数据汇总, 传入RDD[Row], 写入对应的行为表.
    * @param hiveContext
    * @param rowRDD
    * @param table
    */
  def toBehavior(hiveContext: HiveContext, rowRDD: RDD[Row], table: String)={
    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType),
      StructField("acttype", IntegerType), StructField("count", IntegerType)))

    val writeDF =  hiveContext.createDataFrame(rowRDD, schema)
    writeDF.show(3, false)
    writeDF.write.mode(SaveMode.Append).insertInto(table)
  }
}

