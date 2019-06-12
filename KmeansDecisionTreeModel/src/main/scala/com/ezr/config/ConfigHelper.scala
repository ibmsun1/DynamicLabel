package com.ezr.config

import org.apache.log4j.Logger

/**
  * Created by Messi on 2018/11/22.
  */
object ConfigHelper {
  val logger = Logger.getLogger(ConfigHelper.getClass)

  val myUrl = "jdbc:mysql://192.168.12.41:3306/ezp-opt"
  val myDriver = "com.mysql.jdbc.Driver"
  val myUser = "ezwrite"
  val myPassword = "33KlsXareQbsbrfhq2eJ"

  val env = "pro"
  val behavior1 = ".ods_vip_behavior1"
  val behavior2 = ".ods_vip_behavior2"
  val behavior3 = ".ods_vip_behavior3"
  val vipScores = ".dw_vip_scores"
  val vipResults = ".dw_vip_results"
  val vipBehavior = ".dw_vip_behaviors"
  val vipLabels = ".dw_vip_labels"

  val labelOne = "不敏感型"
  val labelTwo = "普通型"
  val labelThree = "非常敏感型"

}
