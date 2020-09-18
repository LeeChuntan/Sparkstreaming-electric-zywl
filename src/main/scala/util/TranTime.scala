package util

import java.text.SimpleDateFormat

/**
  * 时间转换为时间戳
  */
object TranTime {
  def tranTimeToLong(time:String): String={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val dt = format.parse(time)
    val timestamp: String = dt.getTime().toString
    timestamp
  }

  /**
    * 时间中提取小时分钟秒
    * @param time
    * @return
    */
  def tranTimeToHour(time:String): String={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val dt = format.parse(time)

    val format1 = new SimpleDateFormat("HH:mm:ss")
    val hour = format1.format(dt)
    hour
  }
}
