package com.ezr.util

import org.joda.time.DateTime

/**
  * Created by Messi on 2018/11/22.
  */
object DateUtil{

  /**
    * 传入DateTime时间, 将其转换为 天数.
    * @param time
    */
  def getDays(time: DateTime): Int = {
    val tY = (time.getYear - 1) * 365
    val tM = time.getDayOfYear
    tY + tM
  }

  /**
    * 传入(vipBindDate, sellDate)两个DateTime, 计算转换为的天数, 最终返回(avg, last)
    * avg = sellDate - vipBindDate
    * last = DateTime.now() - sellDate
    * @param vipBindDate
    * @param sellDate
    * @param now
    */
  def getDaysTuple(vipBindDate: DateTime, sellDate: DateTime, now: DateTime): (Int, Int) = {
    val vipDays = getDays(vipBindDate)
    val sellDays = getDays(sellDate)
    val nowDays = getDays(now)
    val avg = sellDays - vipDays
    val last = nowDays - sellDays
    (avg, last)
  }
}
