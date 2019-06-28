package com.ezr.util

import com.ezr.config.ConfigHelper
import org.apache.log4j.Logger
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by Messi on 2018/11/22.
  */
object Utils {

  implicit val logger = Logger.getLogger(Utils.getClass)

  /**
    * 获取RDD
    * @param dataFrame
    * @return
    */
  def getRDD(dataFrame: DataFrame):RDD[Row]= {
    dataFrame.rdd
  }

  /**
    * 获取权重配置WeightDF
    * @param spark
    * @return
    */
  def getWeightDF(spark: SparkSession):DataFrame= {
    spark.read.format("jdbc").options(
      Map("url" -> ConfigHelper.myUrl,
        "dbtable" -> "(select Subject, Behavior, Weight from opt_bi_dynamiclable_cfg) as weight",
        "driver" -> ConfigHelper.myDriver,
        "user" -> ConfigHelper.myUser,
        "password" -> ConfigHelper.myPassword)).load()
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
    * @param shardGrpId
    * @param subject
    * @return
    */
  def getBehaviorTbl(shardGrpId: String, subject: Int): String ={
      val behavior = if(subject == 1){
        ConfigHelper.env + shardGrpId + ConfigHelper.behavior1
      }else if (subject == 2){
        ConfigHelper.env + shardGrpId + ConfigHelper.behavior2
      }else if (subject == 3){
        ConfigHelper.env + shardGrpId + ConfigHelper.behavior3
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
    * 归一化到指定区间
    * @param inputCol
    * @param outputCol
    * @param mn
    * @param mx
    * @param scalerDF
    * @return
    */
  def scaler(inputCol: String, outputCol: String, mn: Double, mx: Double, scalerDF: DataFrame): DataFrame ={
    new MinMaxScaler().setInputCol(inputCol).setOutputCol(outputCol).setMin(mn).setMax(mx).fit(scalerDF).transform(scalerDF)
  }

  /**
    * 通过分数区间定位标签
    * @param score
    * @return
    */
  def label(score: Double, interval: (Int, Int, Int, Int)): String ={
    val label = if(score >= interval._1 && score < interval._2) ConfigHelper.labelOne else if(score >= interval._2 && score < interval._3) ConfigHelper.labelTwo else if(score > interval._3 && score <= interval._4) ConfigHelper.labelThree
    label.asInstanceOf[String]
  }

  /**
    * 按照主题获取标签分数区间
    * @param subject
    * @param args
    * @return
    */
  def getIntervalBySubject(subject: Int, args: Array[String]): (Int, Int, Int, Int) ={
    var interval: (Int, Int, Int, Int) = (0, 0, 0, 0)
    interval = (args(0).toInt, args(1).toInt, args(2).toInt, args(3).toInt)
    interval
  }

}

