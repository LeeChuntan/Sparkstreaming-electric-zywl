package method

import java.sql.Statement

import cn.hutool.core.util.IdUtil
import com.alibaba.fastjson.JSONObject
import conf.MyConf
import db.DBredis
import javaUtil.{IpMatch, Utc2Timestamp}
import org.apache.spark.sql.DataFrame
import redis.clients.jedis.Jedis
import util.{JDBCUtil, TranTime}
import org.elasticsearch.spark._

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

/**
  * @program: sparkstreaming
  * @author: LiChuntao
  * @create: 2020-06-23 15:42
  */
object DataInsertIntoES {
  /**
    * 正常流量写入es
    */
  val snowflake = IdUtil.createSnowflake(1, 1)

  def insertData2Es(dataFrame: DataFrame): Unit = {

    var username: String = null
    var time: Long = 0L
    var BJtime: String = null
    var BJtime2Long: String = null
    dataFrame.rdd.mapPartitions(f => {

      val redis: Jedis = DBredis.getConnections()
      val rl = new ListBuffer[mutable.Map[String, String]]
      for (t <- f) {
        val id = snowflake.nextId

        BJtime = new Utc2Timestamp().utcTimeToLocalTime(t.getAs[String]("requestTime"))
        BJtime2Long = TranTime.tranTimeToLong(BJtime)
        val sysid: String = t.getAs[Int]("sysid").toString
        val area: String = t.getAs("c_area")
        val ip: String = t.getAs("ip")
        username = redis.get("ip_" + t.getAs("ip"))
        val rm = Map("pid" -> id.toString,
          "subsystem" -> sysid,
          "update_time" -> System.currentTimeMillis().toString,
          "area" -> area,
          "url" -> t.getAs("url"),
          "requestTime" -> BJtime2Long,
          "type" -> MyConf.nor_type,
          "abnormal" -> MyConf.nor_status,
          "status" -> MyConf.nor_status,
          "user" -> username,
          "level" -> MyConf.nor_level.toString,
          "ip" -> ip)
        rl += rm
      }
      redis.close()
      rl.toIterator
    }).saveToEs(MyConf.es_index_type)
  }


  /**
    * 认证失败以及认证失败多次的插入es
    *
    * @param dataFrame
    */
  def insertAnorDataIntoES(dataFrame: DataFrame, failLevel: Int, failnumLevel: Int, map: Map[String, Int]): Unit = {
    var time: Long = 0L
    var jsonObj = new JSONObject()
    var BJtime2Long: String = null
    var BJtime: String = null

    dataFrame.rdd.mapPartitions(f => {

      val redis: Jedis = DBredis.getConnections()
      val rl = new ListBuffer[mutable.Map[String, String]]
      for (t <- f) {
        val id = snowflake.nextId
        BJtime = new Utc2Timestamp().utcTimeToLocalTime(t.getAs[String]("requestTime"))
        BJtime2Long = TranTime.tranTimeToLong(BJtime)
        val sysid: String = t.getAs[Int]("sysid").toString
        val area: String = t.getAs("c_area")
        val ip: String = t.getAs("ip")
        val username: String = t.getAs("user")

        //选择数据库 存储用户认证次数信息
        redis.incr("landingNum_" + username)
        if (redis.get("landingNum_" + username).equals("1")) {
          redis.expire("landingNum_" + username, map.get("time").get)
        }
        if (redis.exists("landingNum_" + username) && redis.get("landingNum_" + username).toInt >= map.get("error").get) {
          val rm = Map("pid" -> id.toString,
            "subsystem" -> sysid,
            "update_time" -> System.currentTimeMillis().toString,
            "area" -> area,
            "url" -> t.getAs("url"),
            "requestTime" -> BJtime2Long,
            "type" -> MyConf.landing_timeOut,
            "abnormal" -> MyConf.abnor_status,
            "status" -> MyConf.abnor_status,
            "user" -> username,
            "level" -> failnumLevel.toString,
            "ip" -> ip)
          rl += rm
          redis.del("landingNum_" + username)

          //发送订阅消息
          if (true) {
            jsonObj.put("pid", id)
            jsonObj.put("username", username)
            jsonObj.put("level", failnumLevel.toString)
            redis.publish("bmap_alarm_channel", jsonObj.toString)
          }

        } else {
          val rm = Map("pid" -> id.toString,
            "subsystem" -> sysid,
            "update_time" -> System.currentTimeMillis().toString,
            "area" -> area,
            "url" -> t.getAs("url"),
            "requestTime" -> BJtime2Long,
            "type" -> MyConf.landingFail,
            "abnormal" -> MyConf.abnor_status,
            "status" -> MyConf.abnor_status,
            "user" -> username,
            "level" -> failLevel.toString,
            "ip" -> ip)
          rl += rm

          if (true) {
            jsonObj.put("pid", id)
            jsonObj.put("username", username)
            jsonObj.put("level", failLevel)
            redis.publish("bmap_alarm_channel", jsonObj.toString)
          }
        }
      }
      redis.close()
      rl.toIterator
    }).saveToEs(MyConf.es_index_type)
  }

  /**
    * 白名单外IP检测输出到es
    *
    * @param dataFrame
    * @param level
    * @param typeid
    * @param status
    */
  def insertAborIpIntoES(dataFrame: DataFrame, level: Int, typeid: String, status: String): Unit = {
    var time: Long = 0L
    var BJtime: String = null
    var BJtime2Long: String = null
    dataFrame.rdd.mapPartitions(f => {

      val redis: Jedis = DBredis.getConnections()
      val rl = new ListBuffer[mutable.Map[String, String]]
      for (t <- f) {
        val id = snowflake.nextId
        BJtime = new Utc2Timestamp().utcTimeToLocalTime(t.getAs[String]("requestTime"))
        BJtime2Long = TranTime.tranTimeToLong(BJtime)
        val sysid: String = t.getAs[Int]("sysid").toString
        val area: String = t.getAs("c_area")
        val ip: String = t.getAs("ip")
        val rm = Map("pid" -> id.toString,
          "subsystem" -> sysid,
          "update_time" -> System.currentTimeMillis().toString,
          "area" -> area,
          "url" -> t.getAs("url"),
          "requestTime" -> BJtime2Long,
          "type" -> typeid,
          "abnormal" -> status,
          "status" -> status,
          "level" -> level.toString,
          "ip" -> ip)
        rl += rm
      }
      redis.close()
      rl.toIterator
    }).saveToEs(MyConf.es_index_type)
  }

  /**
    * 认证成功以及异常时间段访问输出es
    *
    * @param dataFrame
    * @param level
    * @param typeid
    */
  def insertAborTimeAndIdenSuccessIntoES(dataFrame: DataFrame, abnorTimelevel: Int, abnorArealevel: Int, level: Int, typeid: String, listBuffer: ListBuffer[(String, String)]): Unit = {
    var time: Long = 0L
    var jsonObj = new JSONObject()
    var BJtime: String = null
    var hour: String = null
    var BJtime2Long: String = null
    var IDlist = new ListBuffer[Long]()
    dataFrame.rdd.mapPartitions(f => {

      val redis: Jedis = DBredis.getConnections()
      val rl = new ListBuffer[mutable.Map[String, String]]
      for (t <- f) {
        val id = snowflake.nextId
        BJtime = new Utc2Timestamp().utcTimeToLocalTime(t.getAs[String]("requestTime"))
        BJtime2Long = TranTime.tranTimeToLong(BJtime)
        hour = TranTime.tranTimeToHour(BJtime)

        val sysid: String = t.getAs[Int]("sysid").toString
        val area: String = t.getAs("c_area")
        val ip: String = t.getAs("ip")
        val user: String = t.getAs("user")

        //提取地区精确到市级
        val IpMatchAll: String = new IpMatch().getCityInfo(ip)
        val index1 = IpMatchAll.lastIndexOf("|")
        val index2 = IpMatchAll.lastIndexOf("|", index1 - 1)
        val IpMatchArea = IpMatchAll.substring(index2 + 1, index1)

        for (i <- 0 to listBuffer.size - 1) {
          if (listBuffer(i)._1 < hour && listBuffer(i)._2 > hour) {
            val rm = Map("pid" -> id.toString,
              "subsystem" -> sysid,
              "update_time" -> System.currentTimeMillis().toString,
              "area" -> IpMatchArea,
              "url" -> t.getAs("url"),
              "requestTime" -> BJtime2Long,
              "type" -> MyConf.abnor_Time,
              "abnormal" -> MyConf.abnor_status,
              "status" -> MyConf.abnor_status,
              "user" -> user,
              "level" -> abnorTimelevel.toString,
              "ip" -> ip)
            rl += rm

            if (true) {
              jsonObj.put("pid", id)
              jsonObj.put("username", user)
              jsonObj.put("level", abnorTimelevel)
              redis.publish("bmap_alarm_channel", jsonObj.toString)

              IDlist += (id)
            }
          }
        }
        if (!IpMatchArea.equals(area)) {
          val rm = Map("pid" -> id.toString,
            "subsystem" -> sysid,
            "update_time" -> System.currentTimeMillis().toString,
            "area" -> area,
            "url" -> t.getAs("url"),
            "requestTime" -> BJtime2Long,
            "type" -> MyConf.abnormal_area, //需要修改为异地登录的类型
            "abnormal" -> MyConf.abnor_status,
            "status" -> MyConf.abnor_status,
            "user" -> user,
            "level" -> abnorArealevel.toString,
            "ip" -> ip)
          rl += rm

          if (true) {
            jsonObj.put("pid", id)
            jsonObj.put("username", user)
            jsonObj.put("level", abnorArealevel)
            redis.publish("bmap_alarm_channel", jsonObj.toString)
          }
        } else {
          if (!IDlist.toList.contains(id)) {
            val rm = Map("pid" -> id.toString,
              "subsystem" -> sysid,
              "update_time" -> System.currentTimeMillis().toString,
              "area" -> area,
              "url" -> t.getAs("url"),
              "requestTime" -> BJtime2Long,
              "type" -> typeid,
              "abnormal" -> MyConf.nor_status,
              "status" -> MyConf.nor_status,
              "user" -> user,
              "level" -> level.toString,
              "ip" -> ip)
            rl += rm
          }
        }
      }
      redis.close()
      rl.toIterator
    }).saveToEs(MyConf.es_index_type)
  }

  /**
    * 文件操作频繁操作检测
    *
    * @param dataFrame
    * @param level
    * @param typeid
    */
  def insertFileOperDataIntoES(dataFrame: DataFrame, level: Int, typeid: String, timeConf: Int, amount: Int): Unit = {
    var username: String = null
    var time: Long = 0L
    var jsonObj = new JSONObject()
    var BJtime2Long: String = null
    var BJtime: String = null
    dataFrame.rdd.mapPartitions(f => {
      val redis: Jedis = DBredis.getConnections()
      val rl = new ListBuffer[mutable.Map[String, String]]
      for (t <- f) {
        val id = snowflake.nextId
        BJtime = new Utc2Timestamp().utcTimeToLocalTime(t.getAs[String]("requestTime"))
        BJtime2Long = TranTime.tranTimeToLong(BJtime)
        val sysid: String = t.getAs[Int]("sysid").toString
        val area: String = t.getAs("c_area")
        val ip: String = t.getAs("ip")
        username = redis.get("ip_" + t.getAs("ip"))

        redis.incr("operFile_" + username)
        if (redis.get("operFile_" + username).equals("1")) {
          redis.expire("operFile_" + username, timeConf)
        }

        if (redis.exists("operFile_" + username) && redis.get("operFile_" + username).toInt >= amount) {
          val rm = Map("pid" -> id.toString,
            "subsystem" -> sysid,
            "update_time" -> System.currentTimeMillis().toString,
            "area" -> area,
            "url" -> t.getAs("url"),
            "requestTime" -> BJtime2Long,
            "type" -> typeid, //敏感资源操作异常
            "abnormal" -> MyConf.abnor_status,
            "status" -> MyConf.abnor_status,
            "user" -> username,
            "level" -> level.toString,
            "ip" -> ip)
          rl += rm
          redis.del("operFile_" + username)
          //发送redis订阅消息
          if (true) {
            jsonObj.put("pid", id)
            jsonObj.put("username", username)
            jsonObj.put("level", level)
            redis.publish("bmap_alarm_channel", jsonObj.toString)
          }
        } else {
          val rm = Map("pid" -> id.toString,
            "subsystem" -> sysid,
            "update_time" -> System.currentTimeMillis().toString,
            "area" -> area,
            "url" -> t.getAs("url"),
            "requestTime" -> BJtime2Long,
            "type" -> MyConf.nor_type,
            "abnormal" -> MyConf.nor_status,
            "status" -> MyConf.nor_status,
            "user" -> username,
            "level" -> MyConf.nor_level.toString,
            "ip" -> ip)
          rl += rm
        }
      }
      redis.close()
      rl.toIterator
    }).saveToEs(MyConf.es_index_type)
  }
}
