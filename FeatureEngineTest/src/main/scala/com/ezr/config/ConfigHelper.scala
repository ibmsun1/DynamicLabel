package com.ezr.config

import org.apache.log4j.Logger

/**
  * Created by Messi on 2018/11/22.
  */
object ConfigHelper {
  val logger = Logger.getLogger(ConfigHelper.getClass)

  val env = "test"

  /**
    * 行为、动态标签表配置
    */
  val behavior1 = ".ods_vip_behavior1"
  val behavior2 = ".ods_vip_behavior2"
  val behavior3 = ".ods_vip_behavior3"

  val actDeliver = ".crm_act_deliver_get_log"
  val actShare = ".crm_act_media_vip_sharelog"
  val actReserve = ".crm_act_reserve_info"
  val actScan = ".crm_act_scan_get_log"
  val coupon = ".crm_coupon_list"
  val receivePacket = ".crm_mp_receivepacketlog"
  val serComment = ".crm_ser_comment"
  val bonus = ".crm_vip_info_bonus"
  val infoPerfect = ".crm_vip_info_perfect"
  val shopGuide = ".crm_shop_guide"

  val actDeliverTemp = ".crm_act_deliver_get_log_temp"
  val actShareTemp = ".crm_act_media_vip_sharelog_temp"
  val actReserveTemp = ".crm_act_reserve_info_temp"
  val actScanTemp = ".crm_act_scan_get_log_temp"
  val couponTemp = ".crm_coupon_list_temp"
  val receivePacketTemp = ".crm_mp_receivepacketlog_temp"
  val serCommentTemp = ".crm_ser_comment_temp"
  val bonusTemp = ".crm_vip_info_bonus_temp"
  val infoPerfectTemp = ".crm_vip_info_perfect_temp"
  val shopGuideTemp = ".crm_shop_guide_temp"

}
