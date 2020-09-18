package method

import com.alibaba.fastjson.JSON
import conf.MyConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.{ListBuffer, Map}

object AssociatedQueryConf {

  //异常时间异常等级
  var abnorTimeLevel: Int = _
  //异常IP异常等级
  var AbnormalIpLevel: Int = _
  //认证失败异常等级
  var failLevel: Int = _
  //敏感资源操作异常等级
  var fileOperLevel: Int = _
  //频繁认证失败异常等级
  var failLandingTimesLevel: Int = _
  //账号异地登陆异常等级
  var abnorAreaLevel: Int = _

  /**
    * 获取账号异地登陆等级
    */

  def getAbnorAreaLevel(conf: SparkConf, sqlc: SQLContext): Unit = {

    val data = QueryAbnorLevel.getAbnorAreaLevel(conf, sqlc, MyConf.modeld_area)
    data.foreachPartition(part => {
      part.foreach(o => {
        abnorAreaLevel = o.getAs[Int]("mleve")
      })
    })
  }

  /**
    * 获取时间异常配置
    *
    * @param conf
    * @param sqlc
    * @return
    */
  def getAbnormalTimeMap(conf: SparkConf, sqlc: SQLContext): ListBuffer[(String, String)] = {

    val timeConf = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_time)
    val listTime = new ListBuffer[(String, String)]
    timeConf.rdd.collect().foreach(row => {
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      abnorTimeLevel = level
      val size: Int = data.size()
      for (i <- 0 to size - 1) {
        val nObject = data.getJSONObject(i)
        listTime += ((nObject.getString("starttime"), nObject.getString("endtime")))
      }
    })
    listTime
  }

  /**
    * 获取白名单IP
    *
    * @param conf
    * @param sqlc
    * @return list
    */
  def getNormalIpList(conf: SparkConf, sqlc: SQLContext): Map[String, ListBuffer[String]] = {

    val ipConf = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_white)
    val ipConfMap: Map[String, ListBuffer[String]] = Map()
    val ipList = new ListBuffer[String]()
    val ipRangeList = new ListBuffer[String]()
    ipConf.rdd.collect().foreach(row => {
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      AbnormalIpLevel = level
      val size: Int = data.size()
      for (i <- 0 to size - 1) {
        val nObject = data.getJSONObject(i)
        ipList += (nObject.getString("ip"))
        ipRangeList += (nObject.getString("ipRange"))
      }
      ipConfMap += ("ip" -> ipList)
      ipConfMap += ("ipRange" -> ipRangeList)
    })
    ipConfMap
  }

  /**
    * 获取认证url 及错误返回码
    *
    * @param conf
    * @param sqlc
    * @return
    */
  def getIdentUrlConf(conf: SparkConf, sqlc: SQLContext): Map[String, List[String]] = {
    val dataPort = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_landing)
    //    val rddPort = dataPort.rdd
    var mapUrlConf: Map[String, List[String]] = Map()
    var mapSeverIp: Map[String, Int] = Map()
    var mapPort: Map[String, Int] = Map()
    var mapError: Map[String, Int] = Map()
    var mapUrl: Map[String, Int] = Map()

    dataPort.rdd.collect().foreach(row => {
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      failLevel = level
      val size: Int = data.size()
      for (i <- 0 to size - 1) {
        val nObject = data.getJSONObject(i)
        mapSeverIp += (nObject.getString("ip") -> level)
        mapPort += (nObject.getString("port") -> level)
        mapError += (nObject.getString("error") -> level)
        mapUrl += (nObject.getString("url") -> level)
      }
    })
    mapUrlConf += ("url" -> mapUrl.map(_._1).toList)
    mapUrlConf += ("error" -> mapError.map(_._1).toList)
    mapUrlConf += ("serverIp" -> mapSeverIp.map(_._1).toList)
    mapUrlConf += ("port" -> mapPort.map(_._1).toList)

    mapUrlConf
  }


  /**
    * 获取认证次数以及时间配置
    *
    * @param conf
    * @param sqlc
    * @return
    */
  def getLandingTimesConf(conf: SparkConf, sqlc: SQLContext): Map[String, Int] = {
    val dataPort = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_landTime)
    //    val rddPort = dataPort.rdd
    var mapLandTimesConf: Map[String, Int] = Map()

    dataPort.rdd.collect().foreach(row => {
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      failLandingTimesLevel = level
      val size: Int = data.size()
      for (i <- 0 to size - 1) {
        val nObject = data.getJSONObject(i)
        mapLandTimesConf += ("time" -> nObject.getString("time").toInt)
        mapLandTimesConf += ("error" -> nObject.getString("error").toInt)
      }
    })
    mapLandTimesConf
  }

  /**
    * 获取服务系统的地址和端口
    *
    * @param conf
    * @param sqlc
    * @param dataFrame
    */
  def getSystemIpPort(conf: SparkConf, sqlc: SQLContext, dataFrame: DataFrame): Map[String, List[String]] = {
    var mapPortIpList: Map[String, List[String]] = Map()
    var mapPortIp: Map[String, String] = Map()
    dataFrame.collect().foreach(row => {
      val ip = row.getAs[String]("ip")
      val port = row.getAs[String]("port")
      mapPortIp += (port -> ip)
    })
    mapPortIpList += ("iplist" -> mapPortIp.map(_._2).toList.distinct)
    mapPortIpList += ("portlist" -> mapPortIp.map(_._1).toList.distinct)
    mapPortIpList
  }


  /**
    * 获取文件下载上传操作url
    *
    * @param conf
    * @param sqlc
    * @return
    */
  def getDownAndUpUrlConf(conf: SparkConf, sqlc: SQLContext): ListBuffer[(String, String, String, String)] = {
    val dataPort = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_download)
    //var mapLandTimesConf: Map[String, Int] = Map()
    val list = new ListBuffer[(String, String, String, String)]
    //拉取数据到driver端 遍历获取配置信息  并合并分区，因为task过多
    dataPort.coalesce(3).rdd.collect().foreach(row => {
      val jsonstr = row.getAs[String]("jsonstr").toString
      val data = JSON.parseArray(jsonstr)
      val level = row.getAs[Int]("level")
      fileOperLevel = level
      val size: Int = data.size()
      for (i <- 0 to size - 1) {
        val nObject = data.getJSONObject(i)
        list += ((nObject.getString("name"), nObject.getString("url"), nObject.getString("time"), nObject.getString("amount")))
      }
    })
    list
  }
}
