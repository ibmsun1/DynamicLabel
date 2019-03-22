package com.ezr.util

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by nannan on 2018/8/21.
  */
object EtlUtil {

  

  def getDataFromHive(sqlContext: HiveContext, columns: util.ArrayList[String], tableName: String, filterList: util.ArrayList[String]) ={
    var sqlStr = "select "
    val length = columns.size()
    for(i<- 0 until length){
      if(i==length-1){
        sqlStr += columns.get(i)+" from "
      } else {
        sqlStr += columns.get(i)+","
      }
    }
    var filterStr = ""
    if(filterList != null && !filterList.isEmpty){
      filterStr += " where "
      val size = filterList.size()
      for(i<- 0 until size){
        if(i == size-1){
          filterStr += filterList.get(i)
        }else{
          filterStr += filterList.get(i)+" and "
        }
      }
    }
    sqlStr +=  tableName + filterStr
    println(sqlStr)
    sqlContext.sql(sqlStr)
  }


  def getDataFromOneTable(sqlContext: HiveContext, columns: util.ArrayList[String], tableName: String, filterList: util.ArrayList[String]) ={
    /**读取表数据*/
    val rddDf: DataFrame = getDataFromHive(sqlContext,columns,tableName,filterList)
    /**返回数据集合*/
    rddDf
  }
  /**
    * 获取两张表即全量表和临时表的数据，合并id字段相同的数据，根据lastmodifieddate的时间搓保留相同id的最新的那一行的数据
    * id号唯一的数据原样保留，最后返回RDD[row]数据
    *
    * @param sqlContext 读取数据的HiveContext对象
    * @param columns ArrayList[String] 类型的list 专门用于存放所要查询的字段的集合
    * @param tableName 要查询的全量表的表名
    * @param filterList1 所要查询的全量表的过滤数据的条件
    * @param tableTemp 要查询的临时表的表名
    * @param filterList2 所要查询的临时表的过滤数据的条件，一般情况下可以和filterList1元素一样
    * @return 返回所需要的数据集合rowRdd: RDD[Row] 提供给后面的业务处理使用
    */
  def getDistinctDataFromTwoTable(sqlContext: HiveContext, columns: util.ArrayList[String], tableName: String, filterList1: util.ArrayList[String], tableTemp:String, filterList2: util.ArrayList[String]) ={
    columns.add("CONCAT(id,brandId) flagId")
    val rddDf: DataFrame = getDataFromHive(sqlContext,columns,tableName,filterList1)
    val rddTempDF: DataFrame = getDataFromHive(sqlContext,columns,tableTemp,filterList2)
    val rowRdd: RDD[Row] = rddDf.unionAll(rddTempDF).rdd
      .map(df=>{
        val key = df.getAs[String]("flagId")
        val lastColumn = df.getAs[Long]("lastmodifieddate")
        (key,(df,lastColumn))
      }).reduceByKey{
      case(a,b)=>{
        if(a._2 < b._2){
          (b._1,b._2)
        } else{
          (a._1,a._2)
        }
      }
    }.map(r=>r._2._1)
    rowRdd
  }


  def getOldVipIdOfBroadcast(sc:SparkContext, sqlContext:HiveContext, shardingGrpId: String,vipBindOld:String) ={
    val columns: util.ArrayList[String] = new util.ArrayList[String]()
    columns.add("oldvipid")
    columns.add("brandid")
    val rddDf: DataFrame = getDataFromOneTable(sqlContext,columns,vipBindOld,null)
    val rdd: RDD[(String,Int)] = rddDf.rdd.map(df=>{
      val oldVipId = df.getAs[Long]("oldvipid")
      val brandId = df.getAs[Int]("brandid")
      val key = "%s_%s".format(brandId,oldVipId)
      (key,1)
    })
    if(rdd.isEmpty()){
      println("crm_vip_info_bindold_temp is empty")
    }
    val bdt: Broadcast[collection.Map[String, Int]] = sc.broadcast(rdd.collectAsMap())
    val bdtMap: collection.Map[String, Int] = bdt.value
    bdtMap
  }

}
