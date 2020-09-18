package test

import com.alibaba.fastjson.JSON
import conf.MyConf
import method.AssociatedQueryConf.failLandingTimesLevel
import method.QueryAbnorLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable.{ListBuffer, Map}

/**
  * @program: sparkstreaming
  * @author: LiChuntao
  * @create: 2020-07-01 21:42
  */
object test_b {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[*]")
    //  conf.set("es.nodes", "s1")
    //  conf.set("es.port", "9200")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //      import spark.implicits._
    val sqlc = new SQLContext(sc)

    val ipConf = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_white)
    val ipConfMap: Map[String, ListBuffer[String]] = Map()
    val ipList = new ListBuffer[String]()
    val ipRangeList = new ListBuffer[String]()
    ipConf.rdd.collect().foreach(row => {
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
    println(ipConfMap.get("ipRange").get)



  if ("a".equals("a")){println("测试")}


    for (o <- 0 to ipConfMap.get("ipRange").get.toList.size - 1) {
      val str = ipConfMap.get("ipRange").get.toList(o)
      println(str)
    }
  }

}
