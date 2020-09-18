package util

import conf.MyConf
import javaUtil.Utc2Timestamp
import util.TranTime
import java.sql.{ResultSet, Statement, Connection => jsc}

import com.alibaba.fastjson.JSONObject
import db.DBredis
import method.AssociatedQueryConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

import scala.collection.mutable.Map

object DBUtil {

  /**
    * 异常数据插入MySQL     认证失败
    * @param dataFrame
    * @param level        异常等级
    * @param typeid       异常类型
    * @param status       异常状态
    * */
  def insertAnorDataIntoMysqlByJdbc(dataFrame: DataFrame, level: Int, typeid: String, status: Int): Unit = {
   var time: Long = 0L
   var jsonObj = new JSONObject()
   var BJtime2Long: String = null
   var BJtime: String = null
//   val timetran = new Utc2Timestamp()
    dataFrame.foreachPartition(partitionsOfRecords => {
      var connection: jsc = null
      var statement: Statement = null
      val redis: Jedis = DBredis.getConnections()
      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        partitionsOfRecords.foreach(fields => {
//          BJtime = new Utc2Timestamp().utcTimeToLocalTime(fields(1).toString)

          //选择数据库 存储用户认证次数信息
          redis.incr("num_" + fields(4))
          if (redis.get("num_" + fields(4)).equals("1")) {
            redis.expire("num_" + fields(4), 300)
          }

          //转时间格式
          BJtime = new Utc2Timestamp().utcTimeToLocalTime(fields(1).toString)
          BJtime2Long = TranTime.tranTimeToLong(BJtime)

          time =System.currentTimeMillis()
          val pid = redis.incr("bmap_result_id")
//          val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
//            "values(" + pid + ",'" + fields(0) + "','" + fields(4) + "','" + fields(3) + "','" + BJtime + "','" + fields(2) + "','" + typeid + "','" + level + "','" + status + "','" +time+ "','" + status + "')"

          if (redis.exists("num_" + fields(4)) && redis.get("num_" + fields(4)).toInt >= 5){
            val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
              "values(" + pid + ",'" + fields(0) + "','" + fields(4) + "','" + fields(3) + "','" + BJtime2Long + "','" + fields(2) + "','" + MyConf.landing_timeOut + "','" + level + "','" + status + "','" +time+ "','" + status + "')"

            statement.execute(sql)
            //statement.executeBatch()
            connection.commit()
          }else{
            val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
              "values(" + pid + ",'" + fields(0) + "','" + fields(4) + "','" + fields(3) + "','" + BJtime2Long + "','" + fields(2) + "','" + typeid + "','" + level + "','" + status + "','" +time+ "','" + status + "')"

            statement.execute(sql)
            //statement.executeBatch()
            connection.commit()
          }
//          statement.execute(sql)
//          connection.commit()

          println("执行到这里了吗")
          //登录失败  向redis发送订阅消息
          if (typeid == MyConf.landingFail){
//          val jsonObj = new JSONObject()
          jsonObj.put("pid", pid)
          jsonObj.put("username", fields(3))
          jsonObj.put("level", level)
          redis.publish("bmap_alarm_channel", jsonObj.toString)
          }

        })
        redis.close()
      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }

  /**
    * 白名单异常IP插入MySQL
    * @param dataFrame
    */

  def insertIntoMysqlByJdbc(dataFrame: DataFrame, level: Int, typeid: String, status: Int): Unit = {

    println("白名单插入方法接受到的流量展示")
    dataFrame.show()
    var BJtime: String = null
    var BJtime2Long: String = null
    dataFrame.foreachPartition(partitionsOfRecords => {
      var connection: jsc = null
      var statement: Statement = null

      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        val redis: Jedis = DBredis.getConnections()

        partitionsOfRecords.foreach(fields => {
          BJtime = new Utc2Timestamp().utcTimeToLocalTime(fields(4).toString)
          BJtime2Long = TranTime.tranTimeToLong(BJtime)
          val pid = redis.incr("bmap_result_id")
          val time =System.currentTimeMillis()
          println("白名单外遍历输出测试")
          val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
                      "values(" + pid + ",'" + fields(0) + "','" + " " + "','" + fields(2) + "','" + BJtime2Long + "','" + fields(8) + "','" + typeid + "','" + level + "','" + status + "','" +time+ "','" + status + "')"

          /*statement.addBatch(sql)
          statement.executeBatch()*/
          statement.execute(sql)
          //statement.executeBatch()
          connection.commit()
        })
        redis.close()
        connection.commit()

      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }

  /**
    * 认证成功插入数据库 检测认证时间点
    * @param dataFrame
    * @param level
    * @param typeid
    * @param status
    */
  def insertNnorDataIntoMysqlByJdbc(dataFrame: DataFrame, abnorlevel: Int, level: Int, typeid: String, status: Int, map: Map[String,String]): Unit = {
    var time: Long = 0L
    var jsonObj = new JSONObject()
    var BJtime:String = null
    var hour:String = null
    var BJtime2Long: String = null
    dataFrame.foreachPartition(partitionsOfRecords => {
      var connection: jsc = null
      var statement: Statement = null
      val redis: Jedis = DBredis.getConnections()
      try {
        connection = JDBCUtil.getConnection
        connection.setAutoCommit(false)
        statement = connection.createStatement()
        partitionsOfRecords.foreach(fields => {

          //异常时间段的判定 将流量中的时间转成东八区时间 提取出小时/分钟/秒与解析的异常时间段进行比较
          BJtime = new Utc2Timestamp().utcTimeToLocalTime(fields(1).toString)
          BJtime2Long = TranTime.tranTimeToLong(BJtime)
          hour = TranTime.tranTimeToHour(BJtime)
          //异常时间段内
          if (map.get("stime").get < hour && map.get("etime").get > hour) {
            time =System.currentTimeMillis()
            val pid = redis.incr("bmap_result_id")
            val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
              "values(" + pid + ",'" + fields(0) + "','" + fields(4) + "','" + fields(3) + "','" + BJtime2Long + "','" + fields(2) + "','" + MyConf.abnor_Time + "','" + abnorlevel + "','" + MyConf.abnor_status + "','" +time+ "','" + MyConf.abnor_status + "')"
            statement.execute(sql)
            connection.commit()
            println("执行到这里了吗")
            //异常时间段  向redis发送订阅消息
              jsonObj.put("pid", pid)
              jsonObj.put("username", fields(3))
              jsonObj.put("level", level)
              redis.publish("bmap_alarm_channel", jsonObj.toString)
          }else {
            time = System.currentTimeMillis()
            val pid = redis.incr("bmap_result_id")
            val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
              "values(" + pid + ",'" + fields(0) + "','" + fields(4) + "','" + fields(3) + "','" + BJtime2Long + "','" + fields(2) + "','" + typeid + "','" + level + "','" + status + "','" + time + "','" + status + "')"
            statement.execute(sql)
            connection.commit()
          }
        })
        redis.close()
      } catch {
        case e: Exception => {
          e.printStackTrace()
          try {
            connection.rollback()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      } finally {
        try {
          if (statement != null) statement.close()
          if (connection != null) connection.close()
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    })
  }

  /**
    * 用户名和IP映射关系写进redis
    * @param dataFrame
    */
  def insertUserIpIntoRedis(dataFrame: DataFrame): Unit = {
    dataFrame.foreachPartition(part =>{
      val redis: Jedis = DBredis.getConnections()
      part.foreach(line =>{
        redis.set("ip_" + line(0),line(5).toString)
      })
      redis.close()
    })
  }

  /**
    * 业务操作流量插入MySQL  用户名匹配插入
    * @param dataFrame
    */
  def insertBusinessIntoMysql(dataFrame: DataFrame):Unit = {
    var username: String = null
    var time: Long = 0L
    var BJtime:String = null
    var BJtime2Long:String = null
      dataFrame.foreachPartition(partitionsOfRecords => {
        var connection: jsc = null
        var statement: Statement = null
        val redis: Jedis = DBredis.getConnections()

        try {
          connection = JDBCUtil.getConnection
          connection.setAutoCommit(false)
          statement = connection.createStatement()
          partitionsOfRecords.foreach(fields => {

            BJtime = new Utc2Timestamp().utcTimeToLocalTime(fields(4).toString)
            BJtime2Long = TranTime.tranTimeToLong(BJtime)
            //得到ip对应的用户名
            username = redis.get("ip_" + fields(0))
            //测试获取的用户名
            println(username)
            time =System.currentTimeMillis()
            val pid = redis.incr("bmap_result_id")
            val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
              "values(" + pid + ",'" + fields(0) + "','" + username + "','" + fields(2) + "','" + BJtime2Long + "','" + fields(8) + "','" + MyConf.nor_type + "','" + MyConf.nor_level + "','" + MyConf.nor_status + "','" +time+ "','" + MyConf.nor_status + "')"
            statement.execute(sql)
            connection.commit()
          })
          redis.close()
        } catch {
          case e: Exception => {
            e.printStackTrace()
            try {
              connection.rollback()
            } catch {
              case e: Exception => e.printStackTrace()
            }
          }
        } finally {
          try {
            if (statement != null) statement.close()
            if (connection != null) connection.close()
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
      })
    }
  }