package test

import java.sql.Statement
import java.sql.{ResultSet, Statement, Connection => jsc}

import com.mysql.jdbc.Driver
import conf.MyConf
import db.DBredis
import javaUtil.Utc2Timestamp
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import redis.clients.jedis.Jedis
import util.{JDBCUtil, TranTime}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}
import cn.hutool.core.lang.Snowflake
import cn.hutool.core.util.IdUtil
import com.alibaba.fastjson.JSON
import method.AssociatedQueryConf.{AbnormalIpLevel, failLandingTimesLevel, fileOperLevel}
import method.QueryAbnorLevel

/**
  * @program: sparkstreaming
  * @author: LiChuntao
  * @create: 2020-06-22 15:11
  */
object toes {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[*]")
    //  conf.set("es.nodes", "s1")
    //  conf.set("es.port", "9200")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //      import spark.implicits._
    val sqlc = new SQLContext(sc)


    //判断IP属于区间的方法
    val tableA: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", MyConf.mysql_config("url"))
      .option("dbtable", MyConf.ResultTable)
      .option("user", MyConf.mysql_config("username"))
      .option("password", MyConf.mysql_config("password")).load()
    //  table.show()

    val a = tableA.groupBy("ip").count()
    a.show()


    var tableB: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", MyConf.mysql_config("url"))
      .option("dbtable", MyConf.ResultTable)
      .option("user", MyConf.mysql_config("username"))
      .option("password", MyConf.mysql_config("password")).load()
    val b = tableA.groupBy("ip").count()
    b.show()

    var tablec: DataFrame = null

    for (o <- 0 to 2) {
      tableB = tableB.union(tableA)
      val b = tableB.groupBy("ip").count()
      b.show()

    }


    println("看最终的数据")
    tableB.show()
    tableB.printSchema()




    //    val c = tableA.

    //  table.show()

    //
    //     val a = table.selectExpr("*", "ip BETWEEN '10.107.41.0' AND '10.107.41.225' as ipcheck")

    //     val a = sqlc.sql("SELECT * FROM aa where ip BETWEEN '10.107.41.0' AND '10.107.41.225'")
    //     a.show()


    /*val datawhite = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_white)
    val rddwhite = datawhite.rdd
    var mapIp: Map[String, String] = Map()
    //    var AbnormalIpLevel = 0
    rddwhite.collect().foreach(row => {
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      AbnormalIpLevel = row.getAs[Int]("level")
      //      AbnormalIpLevel = level
      val size: Int = data.size()
      for (i <- 0 to size - 1) {
        val nObject = data.getJSONObject(i)
        mapIp += ("startIP" -> nObject.getString("startIP"))
        mapIp += ("endIP" -> nObject.getString("endIP"))
      }
    })

    println(mapIp)
    println(mapIp.get("startIP").get)
    println(mapIp.get("endIP").get)

    //判断IP属于区间的方法
         val table: DataFrame = spark.read.format("jdbc")
           .option("driver", classOf[Driver].getName)
           .option("url", MyConf.mysql_config("url"))
           .option("dbtable", MyConf.ResultTable)
           .option("user", MyConf.mysql_config("username"))
           .option("password", MyConf.mysql_config("password")).load()
         //  table.show()
         table.createOrReplaceTempView("aa")

//         val a = table.selectExpr("*", "ip BETWEEN '10.107.41.0' AND '10.107.41.225' as ipcheck")

         val a = sqlc.sql(s"SELECT * FROM aa where ip BETWEEN '${mapIp.get("startIP").get}' AND '${mapIp.get("endIP").get}'")
         a.show()

*/


    /* val dataPort = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_download)
     //    val rddPort = dataPort.rdd

     dataPort.show()
     var mapLandTimesConf: Map[String, Int] = Map()
     val list = new ListBuffer[(String,String,String,String)]

     dataPort.rdd.collect().foreach(row =>{
       val jsonstr = row.getAs[String]("jsonstr").toString
       val data = JSON.parseArray(jsonstr)
       val level = row.getAs[Int]("level")
       failLandingTimesLevel = level
       val size: Int= data.size()
       for (i <- 0 to size-1) {
         val nObject = data.getJSONObject(i)
         list += ((nObject.getString("no"),nObject.getString("url"),nObject.getString("time"),nObject.getString("amount")))
       }
     })


     //得到异常配置  返回
     var fileOperList = new ListBuffer[String]()


     for (i <- 0 to list.size-1){
       println(s"第${i}次输出：" + list(i))
       fileOperList += (list(i)._2)

     }
     println(fileOperList)*/


    /*val time = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_time)
    time.show()
    val listtime = new ListBuffer[(String,String)]
    time.rdd.collect().foreach(row =>{
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      failLandingTimesLevel = level
      val size: Int= data.size()
      for (i <- 0 to size-1) {
        val nObject = data.getJSONObject(i)
        listtime += ((nObject.getString("starttime"),nObject.getString("endtime")))
      }
    })
    println(listtime)*/


    val timeConf = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_white)
    timeConf.show()
    val ipConfMap: Map[String, ListBuffer[String]] = Map()
    val ipConfListtest = new ListBuffer[(ListBuffer[String], ListBuffer[String])]
    val ipList = new ListBuffer[String]()
    val ipRangeList = new ListBuffer[String]()
    timeConf.rdd.collect().foreach(row => {
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      failLandingTimesLevel = level
      val size: Int = data.size()
      for (i <- 0 to size - 1) {
        val nObject = data.getJSONObject(i)
        //          listTime += (,nObject.getString("endtime")))
        ipList += (nObject.getString("ip"))
        ipRangeList += (nObject.getString("ipRange"))
      }
      ipConfMap += ("ip" -> ipList)
      ipConfMap += ("ipRange" -> ipRangeList)
    })

    println(ipConfMap)

    println("康康" + ipConfMap.get("ip").get.toList)
    println(ipConfMap.get("ipRange").get.toList)

    val size = ipConfMap.get("ipRange").get.toList
    println("测试:" + size.size)

    for (o <- 0 to ipConfMap.get("ipRange").get.toList.size - 1) {
      val str = ipConfMap.get("ipRange").get.toList(o)
      val c = str.indexOf("-")
      println(c)
      val start = str.substring(0, c)
      val end = str.substring(c + 1)
      println(s"第${o}个：" + start)
      println(s"第${o}个：" + end)

    }

    //    val normalIpAccurate = tableA.filter($"ip".isin(ipConfMap.get("ip").get.toList: _*))
  }


  //
  //     val data = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_download)
  //     //    val rddPort = dataPort.rdd
  //     var mapDownUrlConf: Map[String, String] = Map()
  //     data.rdd.collect().foreach(row =>{
  //       val jsonstr = row.getAs[String]("jsonstr").toString
  //       val data = JSON.parseArray(jsonstr)
  //       val level = row.getAs[Int]("level")
  //       fileOperLevel = level
  //       val size: Int= data.size()
  //       for (i <- 0 to size-1) {
  //         val nObject = data.getJSONObject(i)
  //         //        mapDownUrlConf += (nObject.getString("download") -> nObject.getString("up"))
  //         mapDownUrlConf += (nObject.getString("download") -> nObject.getString("up"))
  //       }
  //     })
  //     println(mapDownUrlConf)
  //
  //
  //   }
  //


  /*    var a = 0
for (a <- 1 to 10){
  table.rdd.mapPartitions(f =>{
    val rl = new ListBuffer[mutable.Map[String, String]]
    for (t <- f) {
      val id: String = t.getAs[Long]("id").toString
      val systemname: String = t.getAs("systemname")
      val rm = Map("id" -> id,
        "systemname" -> systemname,
        "updateTime" -> System.currentTimeMillis().toString)
      rl += rm

    }
    rl.toIterator
  }).saveToEs("test2es")}*/

  /*    table.foreachPartition(part => part.foreach(rows =>{
    val rl = new ListBuffer[mutable.Map[String, String]]

      val id: String = rows.getAs[Long]("id").toString
      val systemname: String = rows.getAs("systemname")
      val rm = Map("id" -> id,
        "systemname" -> systemname)
      rl += rm
    EsSpark.saveToEs()

  })

}*/


  /*  def insertBusinessIntoMysql(dataFrame: DataFrame): Unit = {
  var username: String = null
  var time: Long = 0L
  var BJtime: String = null
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

        BJtime = new Utc2Timestamp().utcTimeToLocalTime(fields(4).toString)
        BJtime2Long = TranTime.tranTimeToLong(BJtime)
        //得到ip对应的用户名
        username = redis.get("ip_" + fields(0))
        //测试获取的用户名
        println(username)
        time = System.currentTimeMillis()
        val pid = redis.incr("bmap_result_id")
        val sql = "insert into t_result(pid, ip, user, subsystem, requestTime, area, type, level, status, update_time, abnormal) " +
          "values(" + pid + ",'" + fields(0) + "','" + username + "','" + fields(2) + "','" + BJtime2Long + "','" + fields(8) + "','" + MyConf.nor_type + "','" + MyConf.nor_level + "','" + MyConf.nor_status + "','" + time + "','" + MyConf.nor_status + "')"
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


def insertData2Es(dataFrame: DataFrame): Unit = {

  var username: String = null
  var time: Long = 0L
  var BJtime: String = null
  var BJtime2Long: String = null
//    var c_area: String = null

  println("传入数据测试展示")
  dataFrame.show()


  dataFrame.rdd.mapPartitions(f =>{

    val rl = new ListBuffer[mutable.Map[String, String]]


    for (t <- f) {
      val redis: Jedis = DBredis.getConnections()
      BJtime = new Utc2Timestamp().utcTimeToLocalTime(t.getAs[String]("requestTime"))
      BJtime2Long = TranTime.tranTimeToLong(BJtime)

      val sysid: String = t.getAs[Int]("sysid").toString
//        c_area = t.getAs[String]("c_area")
      val serviceIp: String = t.getAs[String]("ip")
      val pid = redis.incr("bmap_result_id").toString

      username = redis.get("ip_" + t.getAs("ip"))

      val snowflake = IdUtil.createSnowflake(1, 1)
      val id = snowflake.nextId

      val rm = Map("pid" -> pid,
        "subsystem" -> sysid,
        "update_time" -> System.currentTimeMillis().toString,
//          "area" -> c_area,
        "requestTime" -> BJtime2Long,
        "type" -> MyConf.nor_type,
        "abnormal" -> MyConf.nor_status.toString,
        "status" -> MyConf.nor_status.toString,
        "user" -> username,
        "level" -> MyConf.nor_level.toString,
        "ip" -> serviceIp,
        "es.mapping.id" -> "id")
      rl += rm
      redis.close()
    }

    rl.toIterator
  }).saveToEs("test")

}*/

  //

}