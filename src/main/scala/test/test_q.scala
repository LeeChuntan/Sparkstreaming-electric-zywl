package test

import com.alibaba.fastjson.JSON
import conf.MyConf
import method.AssociatedQueryConf.failLevel
import method.QueryAbnorLevel
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable.Map

/**
  * @program: sparkstreaming
  * @author: LiChuntao
  * @create: 2020-06-28 14:46
  */
object test_q {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("a").setMaster("local[*]")
    //  conf.set("es.nodes", "s1")
    //  conf.set("es.port", "9200")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //      import spark.implicits._
    val sqlc = new SQLContext(sc)

//    val dataPort = QueryAbnorLevel.getAbnormalConf(conf, sqlc, MyConf.modeld_download)
//    //    val rddPort = dataPort.rdd
//    var mapDownUrlConf: Map[String, Int] = Map()

//
//   dataPort.foreachPartition(part=>{
//     part.foreach(row =>{
//       val jsonstr = row.getAs[String]("jsonstr").toString
//       val data = JSON.parseArray(jsonstr)
//       val level = row.getAs[Int]("level")
//       failLevel = level
//       val size: Int= data.size()
//       for (i <- 0 to size-1) {
//         val nObject = data.getJSONObject(i)
//         mapDownUrlConf += (nObject.getString("download") -> level)
//     }
//   })
//   })
//    println(mapDownUrlConf)

//    dataPort.rdd.collect().foreach(row =>{
//      val jsonstr = row.getAs[String]("jsonstr").toString
//      val data = JSON.parseArray(jsonstr)
//      val level = row.getAs[Int]("level")
//      failLevel = level
//      val size: Int= data.size()
//      for (i <- 0 to size-1) {
//        val nObject = data.getJSONObject(i)
//        mapDownUrlConf += (nObject.getString("download") -> level)
//
//      }
//    })
//    println(mapDownUrlConf)                     //mapDownUrlConf


    val subnet = new SubnetUtils("192.168.1.0/24").getInfo()
    val test = subnet.isInRange("192.168.2.2")
    println(test)

}
}