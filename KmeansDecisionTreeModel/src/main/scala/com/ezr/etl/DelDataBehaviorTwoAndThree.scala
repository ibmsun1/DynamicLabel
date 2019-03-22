package com.ezr.etl

import com.ezr.config.ConfigHelper
import com.ezr.util.Utils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * Created by Messi on 2018/11/22,
  * 主题2/3, 数据处理, 归一化后打分, 写入打分表, 用于聚类.
  */
object DelDataBehaviorTwoAndThree {
  implicit val logger = Logger.getLogger(DelDataBehaviorTwoAndThree.getClass)
  def main(args: Array[String]): Unit = {
    val shardGrpId = args(args.length - 1)
    val subject = Utils.getSubject(args)
    implicit val spark = SparkSession.builder().appName("DelDataBehaviorTwoAndThree" + shardGrpId + subject).config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").enableHiveSupport().getOrCreate()
    dataProcess(spark, args, shardGrpId)
    logger.info(s"主题: $subject " + "打分完毕.")
    spark.stop()
  }

  /**
    * 处理主题类型
    * actType{
    * [券敏感]: 5  {"券核销率"}; {"核销次数"}; {"核销距离平均时长"}; {"最后一次核销时间"}.
    * [积分敏感]: {"完善资料得分": 6}; {"游戏得分": 7}; {"积分兑券": 1}; {"积分兑礼": 8}.
    * [互动意愿]: {"大转盘": 4}; {"砸金蛋" 3}; {"刮刮卡": 2}; {"完善资料": 9}; {"预约次数": 10};
    * {"到店次数": 11}; {"领券次数": 12}; {"领红包次数": 13}; {"分享次数": 14}; {"打开次数": 15};
    * {"联系导购": 16}; {"订单评价": 17}
    * }
    * @param args
    * @return
    */
  def getActTypes(args: Array[String]): List[Int] ={
    var list = List[Int]()
    val l = args.length -1
    if (args.length <= 3){
      logger.info("未输入主题行为对应的actType类型! ")
      sys.exit(-1)
    }else if(l == 4){
      if(args(3).toInt == 5){
        list = list.+:(args(3).toInt)
        logger.info("券敏感型: actType" + "\t" + args(3) + "\t" + list.length)
      }else {
        logger.info("输入错误, 券敏感型的行为: actType 为 5！")
        sys.exit(-1)
      }
    }

    if(l == 7){
      var perfMaterial = 0
      var game = 0
      var pointCoupons = 0
      var pointLi = 0

      for (i <- 4 until l){
        val e = args(i).toInt
        if(e == 6 || e == 7 || e == 1 || e == 8){
          logger.info("正确的积分敏感型! ")
        }else{
          logger.info("积分感型参数错误, 请确认输入! ")
          sys.exit(-1)
        }
      }

      perfMaterial = args(3).toInt
      game = args(4).toInt
      pointCoupons = args(5).toInt
      pointLi = args(6).toInt
      list = list.+:(perfMaterial).+:(game).+:(pointCoupons).+:(pointLi)
      logger.info("积分敏感主题类型actType: " + "\t" + args(3) + "\t" + args(4) + "\t" + args(5) + "\t" + args(6) + "\t" + args.length)
    }

    if(l == 15){
      var bigWheel = 0
      var goldenEggs = 0
      var scratch = 0
      var complMaterial = 0
      var reservationNum = 0
      var storeNum = 0
      var couponNum = 0
      var redEnvelopesNum = 0
      var shareTimes = 0
      var openTimes = 0
      var conShopGuide = 0
      var orderEvaluate = 0

      for (i <- 4 until l){
        val e = args(i).toInt
        if(e == 4 || e == 3 || e == 2 || e == 9 || e == 10
          || e == 11 || e == 12 || e == 13 || e == 14 || e == 15 || e == 16 || e == 17){
          logger.info("正确的互动意愿型! ")
        }else{
          logger.info("互动意愿型参数错误, 请确认输入! ")
          sys.exit(-1)
        }
      }

      bigWheel = args(3).toInt
      goldenEggs = args(4).toInt
      scratch = args(5).toInt
      complMaterial = args(6).toInt
      reservationNum = args(7).toInt
      storeNum = args(8).toInt
      couponNum = args(9).toInt
      redEnvelopesNum = args(10).toInt
      shareTimes = args(11).toInt
      openTimes = args(12).toInt
      conShopGuide = args(13).toInt
      orderEvaluate = args(14).toInt

      list = list.+:(bigWheel).+:(goldenEggs).+:(scratch).+:(complMaterial).+:(reservationNum).+:(storeNum)
        .+:(couponNum).+:(redEnvelopesNum).+:(shareTimes).+:(openTimes).+:(conShopGuide).+:(orderEvaluate)
      logger.info("积分敏感主题类型actType: " + "\t" + args(3) + "\t" + args(4) + "\t" + args(5) + "\t" + args(6) + "\t" + args(7) + "\t" + args(8) + "\t" + args(9) + "\t" + args(10) + "\t" + args(11) + "\t" + args(12) + "\t" + args(13) + "\t" + args(14) + "\t" + args.length)
    }
    list
  }

  /**
    *  打分公式: sum(count * weight)/Utils.lenActType(behavior)
    * @param actType
    * @param count
    * @param weights
    * @return
    */
  def getScore(actType:Int, count:  Double, weights:(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double,
    Double, Double, Double, Double, Double, Double)): Double = {

    var score = 0.0
    /**
      * 积分敏感型 actType [6, 7, 1, 8]
      */
    if (actType == 6 || actType == 7 || actType == 1 || actType == 8){
      if (actType == 6){
        score = count * weights._1
        logger.info("actType == 6: score: " + score)
      }else if(actType == 7){
        score = count * weights._2
        logger.info("actType == 7: score: " + score)
      }else if (actType == 1){
        score = count * weights._3
        logger.info("actType == 1: score: " + score)
      }else{
        score = count * weights._4
        logger.info("actType == 8: score: " + score)
      }

      /**
        *  互动意愿 [4, 3, 2, 9, 10, 11, 12, 13, 14, 15, 16, 17]
        */
    }else if(actType == 4 || actType == 3 || actType == 2 || actType == 9 || actType == 10 || actType == 11 ||
      actType == 12 || actType == 13 || actType == 14 || actType == 15 || actType == 16 || actType == 17){
      if (actType == 4){
        score = count * weights._5
        logger.info("actType == 4: score: " + score)
      }else if (actType == 3){
        score = count * weights._6
        logger.info("actType == 3: score: " + score)
      }else if (actType == 2 ){
        score = count * weights._7
        logger.info("actType == 2: score: " + score)
      }else if (actType == 9){
        score = count * weights._8
        logger.info("actType == 9: score: " + score)
      }else if (actType == 10){
        score = count * weights._9
        logger.info("actType == 10: score: " + score)
      }else if (actType == 11){
        score = count * weights._10
        logger.info("actType == 11: score: " + score)
      }else if (actType == 12){
        score = count * weights._11
        logger.info("actType == 12: score: " + score)
      }else if (actType == 13){
        score = count * weights._12
        logger.info("actType == 13: score: " + score)
      }else if (actType == 14){
        score = count * weights._13
        logger.info("actType == 14: score: " + score)
      }else if (actType == 15){
        score = count * weights._14
        logger.info("actType == 15: score: " + score)
      }else if (actType == 16){
        score = count * weights._15
        logger.info("actType == 16: score: " + score)
      }else {
        score = count * weights._16
        logger.info("actType == 17: score: " + score)
      }
    }
    logger.info("score: " + score)
    score
  }

  /**
    * 获取sql和weight
    * @param spark
    * @param args
    * @param shardGrpId
    * @return
    */
  def getSQLAndWeightByActTypes(spark:SparkSession, args:Array[String], shardGrpId: String): (String, Broadcast[(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)], String) ={
    import spark.implicits._
    val actTypes: List[Int] = getActTypes(args)
    var actType = ""
    for (v <- 0 until  actTypes.length){
      if(v < actTypes.length - 1 ){
        actType += actTypes(v) + ","
      }else{
        actType += actTypes(v)
      }
    }
    logger.info("actType: " + actType)
    val table = Utils.getBehaviorTbl(shardGrpId, Utils.getSubject(args))
    val sql = s"select vipid, brandid, copid, acttype, count from  $table  " +  " where  acttype in " + "(" + actType + ")"
    logger.info("sql: " + sql)

    /**
      * 读取权重配置.
      */

    var typeTuples:(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double) =
      (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    /*val weightDF = Utils.getWeightDF(spark)
    var typeTuples:(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double) =
      (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    val weightCouponsDF = weightDF.filter($"Subject" === 2).select("Behavior", "Weight")
    val complMater :Double = weightCouponsDF.filter($"Behavior" === "完善资料2").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val game: Double = weightCouponsDF.filter($"Behavior" === "游戏得分").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val vouchers: Double = weightCouponsDF.filter($"Behavior" === "积分兑券").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val eli: Double = weightCouponsDF.filter($"Behavior" === "积分兑礼").select("Weight").collect().apply(0).getAs[Double]("Weight")

    val weightInteractiveDF = weightDF.filter($"Subject" === 3).select("Behavior", "Weight")
    val bigWheel: Double = weightInteractiveDF.filter($"Behavior" === "大转盘").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val goldenEggs: Double = weightInteractiveDF.filter($"Behavior" === "砸金蛋").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val scratch :Double = weightInteractiveDF.filter($"Behavior" === "刮刮卡").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val complmaterial :Double = weightInteractiveDF.filter($"Behavior" === "预约次数").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val reservationnum: Double = weightInteractiveDF.filter($"Behavior" === "到店次数").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val storenum: Double = weightInteractiveDF.filter($"Behavior" === "领券次数").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val couponnum: Double = weightInteractiveDF.filter($"Behavior" === "领红包次数").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val redenvelopesnum: Double = weightInteractiveDF.filter($"Behavior" === "分享次数").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val sharetimes: Double = weightInteractiveDF.filter($"Behavior" === "打开次数").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val opentimes: Double = weightInteractiveDF.filter($"Behavior" === "联系导购").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val conshopguide: Double = weightInteractiveDF.filter($"Behavior" === "订单评价").select("Weight").collect().apply(0).getAs[Double]("Weight")
    val orderevaluate: Double = weightInteractiveDF.filter($"Behavior" === "完善资料3").select("Weight").collect().apply(0).getAs[Double]("Weight")
*/
    if(actType.replaceAll(",","").length == 4){
//      typeTuples = (complMater, game, vouchers, eli, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      typeTuples = (10, 20, 40, 30, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
      logger.info("积分敏感型权重组合: " + "\t" + typeTuples._1 + "\t" + typeTuples._2 + "\t" + typeTuples._3 + "\t" + typeTuples._4)
    }
    if (actType.length == 31){
//      typeTuples = (0.0, 0.0, 0.0, 0.0, bigWheel, goldenEggs, scratch, complmaterial, reservationnum, storenum, couponnum, redenvelopesnum, sharetimes, opentimes, conshopguide, orderevaluate)
      typeTuples = (0.0, 0.0, 0.0, 0.0, 7, 8, 7, 5, 9, 12, 9, 8, 9, 8, 9, 9)
      logger.info("互动意愿型权重组合: " + "\t" + typeTuples._5 + "\t" + typeTuples._6 + "\t" + typeTuples._7 + "\t" + typeTuples._8 + "\t" + typeTuples._9
        + "\t" + typeTuples._10 + "\t" + typeTuples._11 + "\t" + typeTuples._12 + "\t" + typeTuples._13 + "\t" + typeTuples._14 + "\t" + typeTuples._15 + "\t" + typeTuples._16)
    }
    logger.info("sql: " + sql)
    logger.info("权重Tuples: "+ "\t" + typeTuples._1 + "\t" + typeTuples._2 + "\t" + typeTuples._3 + "\t" + typeTuples._4 + "\t" + typeTuples._5 + "\t" + typeTuples._6 + "\t" + typeTuples._7 + "\t" + typeTuples._8 + "\t" + typeTuples._9
      + "\t" + typeTuples._10 + "\t" + typeTuples._11 + "\t" + typeTuples._12 + "\t" + typeTuples._13 + "\t" + typeTuples._14 + "\t" + typeTuples._15 + "\t" + typeTuples._16)
    (sql, spark.sparkContext.broadcast(typeTuples), actType)
  }

  /**
    * 数据处理
    * @param spark
    * @param actType
    * @param shardGrpId
    */
  def dataProcess(spark:SparkSession, actType:Array[String], shardGrpId: String)= {
    import spark.implicits._
    val weightAndSQL = getSQLAndWeightByActTypes(spark, actType, shardGrpId)
    val behaviorSql: String = weightAndSQL._1
    val weight: Broadcast[(Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double)] = weightAndSQL._2

    /**
      * 最大最小值归一处理
      */
    logger.info("behaviorSql: " + behaviorSql)
    val countsDF: DataFrame = spark.sql(behaviorSql).select("count")
    logger.info("countsDF: " + countsDF.count())
    countsDF.show(10, true)
    val min: Double = countsDF.agg("count" -> "min").withColumnRenamed("min(count)", "mn").select("mn").collect().apply(0).getInt(0).toDouble
    val max: Double = countsDF.agg("count" -> "max").withColumnRenamed("max(count)", "mx").select("mx").collect().apply(0).getInt(0).toDouble

    val sample: DataFrame = countsDF.rdd.zipWithIndex().map(_.swap)
      .map(v =>{
        val id = v._1
        val count =  v._2.getAs[Int]("count").toDouble
        (id, Vectors.dense(count))
      }).toDF("id", "score")
    logger.info("sample -> count: " + sample.count())
    sample.show(10, false)

    val scaledData = Utils.scaler("score", "features", min, max, sample)
    logger.info("scaledData -> count: " + scaledData.count())
    scaledData.show(10, false)

    val sampleRDD: RDD[(Long, Double)] = scaledData.rdd.map(v =>{
      val id = v.getAs[Long]("id")
      val count = v.getAs[DenseVector]("features").values(0)
      (id, count)
    })
    logger.info("sampleRDD: " + sampleRDD.count() + "\t" + sampleRDD.first()._1 + "\t" + sampleRDD.first()._2)

    /**
      * 打分
      */
    val train: DataFrame = spark.sql(behaviorSql).rdd.zipWithIndex().map(_.swap).map(v =>{
      val id = v._1
      val vipid = v._2.getAs[Long]("vipid")
      val brandid = v._2.getAs[Int]("brandid")
      val copid = v._2.getAs[Int]("copid")
      val acttype = v._2.getAs[Int]("acttype")
      val key = "%s_%s_%s".format(vipid, brandid, copid)
      (id, (key, acttype))
    }).join(sampleRDD).map(v =>{
      val key = v._2._1._1
      val acttype = v._2._1._2
      val scale = v._2._2
      val score = getScore(acttype, scale, weight.value)
      (key, acttype, scale, score)
    }).toDF("keyVip", "acttype", "scale", "score")
    logger.info("train -> count: " + train.count())
    train.show(10, false)

    /**
      * 对打分结果归一处理
      */
    val scoreScaler = train.select("score").rdd.zipWithIndex().map(_.swap).map(v=>{
      val id = v._1
      val score = v._2.getAs[Double]("score")
      (id, Vectors.dense(score))
    }).toDF("id", "score")
    logger.info("scoreScaler -> count: " + scoreScaler.count())
    scoreScaler.show(5, false)

    val mnx = (1.00, 100.00)
    val scoreMnx = Utils.scaler("score", "scores", mnx._1, mnx._2, scoreScaler)
    logger.info("scoreMnx -> count: " + scoreMnx.count())
    scoreMnx.show(7, false)

    val scoreMnxRDD = scoreMnx.rdd.map(v=>{
      val id = v.getAs[Long]("id")
      val score = v.getAs[DenseVector]("scores").values(0)
      (id, score)
    })
    logger.info("scoreMnxRDD: " + "\t" + scoreMnxRDD.count() + "\t" + scoreMnxRDD.first()._2)

    val joins: RDD[(Long, ((String, Int), Double))] = train.select("keyVip", "acttype").rdd.zipWithIndex().map(_.swap).map(v=>{
      val id = v._1
      val key = v._2.getAs[String]("keyVip")
      val actType = v._2.getAs[Int]("acttype")
      (id, (key, actType))
    }).join(scoreMnxRDD)
    logger.info("joins: " + "\t" + joins.count() + "\t" + joins.first()._2._1._1 + "\t" + joins.first()._2._1._2 + "\t" + joins.first()._2._2)

    /**
      * 进一步处理数据, 写入打分表和行为表
      */
    val behavior = joins.map(v=>{
      val key = v._2._1._1
      val actType = v._2._1._2
      val score = v._2._2.formatted("%.2f").toDouble
      val behavior = (actType + ":" + score)
      (key, behavior)
    }).groupByKey().map(v=>{
      val key = v._1
      val l = v._2.toList
      var behavior = ""
      for (v <- 0 until  l.length){
        if(v < l.length - 1 ){
          behavior += l(v) + ","
        }else{
          behavior += l(v)
        }
      }
      (key, behavior)
    })
    logger.info("behavior: -> count: " + behavior.count() + "\t" + behavior.first()._1 + "\t" + behavior.first()._2)

    val trainS = joins.map(v=>{
      val key = v._2._1._1
      val score = v._2._2
      (key, score)
    }).toDF("keyVip", "score").groupBy("keyVip").agg("score" -> "sum").withColumnRenamed("sum(score)", "total").rdd.map(v=>{
      val key = v.getAs[String]("keyVip")
      val score = v.getAs[Double]("total")
      (key, score)
    })
    logger.info("trainS: -> count: " + trainS.count() + "\t" + trainS.first()._1 + "\t" + trainS.first()._2)

    val trainJoinDF: DataFrame = trainS.join(behavior).map(v=>{
      val k = v._1.split("_")
      val vipid = k(0).toLong
      val brandid = k(1).toInt
      val copid = k(2).toInt
      val subject: Int = Utils.getSubject(actType)
      val behavior = v._2._2
      val lenActType = Utils.lenActType(behavior)
      var total = v._2._1
      total = (total / lenActType).formatted("%.2f").toDouble
      (vipid, brandid, copid, total, behavior, subject)
    }).toDF("vipid", "brandid", "copid", "score", "behavior","subject")
    logger.info("trainJoinDF -> count: " + trainJoinDF.count())
    trainJoinDF.show(10, false)

    /**
      * 写入打分表
      */
    val trainDF = trainJoinDF.select("vipid", "brandid", "copid", "score", "subject")
    logger.info("trainDF -> count: " + trainDF.count())
    trainDF.show(10, false)

    val schema: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType), StructField("score", DoubleType), StructField("subject", IntegerType)))
    val writeDF: DataFrame = spark.createDataFrame(trainDF.rdd, schema)
    writeDF.show(3, false)

    val tableScore = ConfigHelper.env + shardGrpId + ConfigHelper.vipScores
    Utils.hiveSets(spark, ConfigHelper.env + shardGrpId, "DelDataBehaviorTwoAndThreeScore" + shardGrpId + Utils.getSubject(actType))
    writeDF.write.mode(SaveMode.Append).insertInto(tableScore)

    /**
      * 写入行为表
      */
    val vipBehavior = trainJoinDF.rdd.map(v=>{
      val vipid = v.getAs[Long]("vipid")
      val brandid = v.getAs[Int]("brandid")
      val copid = v.getAs[Int]("copid")
      val score = v.getAs[Double]("score")
      val subject = v.getAs[Int]("subject")
      var behavior = v.getAs[String]("behavior")
      behavior = score + ": {" + behavior.replace(",", ";") + "}"
      Row(vipid, brandid, copid, score, subject, behavior)
    })

    val schemaBehavior: StructType = StructType(Seq(StructField("vipid", LongType), StructField("brandid", IntegerType), StructField("copid", IntegerType),
      StructField("score", DoubleType), StructField("subject", IntegerType), StructField("behavior", StringType)))

    val behaviorDF: DataFrame = spark.createDataFrame(vipBehavior, schemaBehavior)
    logger.info("behaviorDF -> count: " + behaviorDF.count())
    behaviorDF.show(10, false)

    val tableBehavior = ConfigHelper.env + shardGrpId + ConfigHelper.vipBehavior
    Utils.hiveSets(spark, ConfigHelper.env + shardGrpId, "DelDataBehaviorTwoAndThreeBehavior" + shardGrpId + Utils.getSubject(actType))
    behaviorDF.write.mode(SaveMode.Append).insertInto(tableBehavior)
  }
}
