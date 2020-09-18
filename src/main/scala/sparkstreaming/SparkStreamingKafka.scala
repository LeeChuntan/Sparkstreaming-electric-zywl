package sparkstreaming

import conf.MyConf
import conf.LoadParameter
import util.DBUtil
import util.KafkaUtil
import java.lang

import BusinessAnalysis.{SparkStreamingIdentification, SparkstreamingBusiness}
import org.I0Itec.zkclient.ZkClient
import com.alibaba.fastjson.JSON
import method.DataInsertIntoES
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.sql._
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import com.mysql.jdbc.Driver
import kafka.utils.ZKGroupTopicDirs
import method.{AssociatedQueryConf, QueryAbnorLevel, ReadTable}
import org.apache.spark.sql.types.DateType

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, Map}


/**
  * 主程序 读取消息流
  */
class SparkStreamingKafka

object SparkStreamingKafka {

  def main(args: Array[String]): Unit = {
    /**
      * kafka 配置参数 设置消费主体
      */
    val topic = MyConf.kafka_topic
    //kafka服务器
    val brokers = MyConf.kafka_brokers
    //消费者组
    val group = MyConf.kafka_group
    //多个topic 去重 消费
    val topics: Set[String] = Set(topic)
    //指定zk地址
    val zkQuorum = MyConf.zookeeper
    //topic在zk里面的数据路径 用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //得到zk中的数据路径
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    val conf: SparkConf = new SparkConf().setAppName("sparkstreamingkafka").setMaster("local[*]")
    //设置shuffle后的分区数目
    conf.set("spark.sql.shuffle.partitions","20")
    //    conf.set("spark.streaming.kafka.consumer.cache.enabled", "false")
    conf.set("es.nodes", MyConf.es_host)
    conf.set("es.port", MyConf.es_port)
    //    conf.set("es.index.auto.create", "true")

    //设置流间隔批次
    val sc = new SparkContext(conf)
    val streamingContext = new StreamingContext(sc, Durations.seconds(MyConf.Durations_seconds))
    val sqlc = new SQLContext(sc)


    //创建定时更新需要的对象
    var AreaTable: DataFrame = null //地区映射表
    var ipConfMap: Map[String, ListBuffer[String]] = Map() //得到IP配置信息
    var failLandMoreMap: Map[String, Int] = null //多次认证失败配置参数
    var mapUrlConf: Map[String, List[String]] = null //认证失败的url IP port
    var listUrl: List[String] = null //认证失败url
    var listError: List[String] = null //认证失败的状态码
    var listServerIp: List[String] = null //认证失败的服务器地址
    var listPort: List[String] = null //认证失败的服务器端口
    var systemCode: DataFrame = null //系统表
    var sysPortIp: Map[String, List[String]] = null //各系统服务器ip port
    var allServerIp: List[String] = null //所有系统IP地址
    var allServerport: List[String] = null //所有系统port
    var timeConfList = new ListBuffer[(String, String)] //异常时间模型列表
    var fileOperInfoList = new ListBuffer[(String, String, String, String)] //异常操作的信息 编号 url 次数 时间范围
    var fileOperUrlList = new ListBuffer[String]()

    var fileOperLevel: Int = 0 //敏感资源操作异常等级
    var failLevel: Int = 0 //登录失败异常等级
    var abnorAreaLevel: Int = 0 //获取异地异常等级
    var AbnormalIpLevel: Int = 0 //异常IP异常等级
    var AbnormalTimeLevel: Int = 0 //异常时间段异常等级
    var failLandingMoreLevel: Int = 0 //多次登录失败异常等级

    //最后更新时间
    var lastupdate: Long = 0L

    //隐式转换需要
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    /**
      * kafka参数配置
      */
    val kafkaParams = Map(
      "bootstrap.servers" -> brokers,
      "group.id" -> group,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: lang.Boolean),
      "auto.offset.reset" -> MyConf.kafka_offset_position
    )

    //定义一个空数据流 根据偏移量选择
    var kafkaStream: InputDStream[ConsumerRecord[String, String]] = null
    //创建客户端 读取 更新 偏移量
    val zkClient = new ZkClient(zkQuorum)
    val zkExist: Boolean = zkClient.exists(s"$zkTopicPath")
    //使用历史偏移量创建流
    if (zkExist) {
      val clusterEarliestOffsets: Map[Long, Long] = KafkaUtil.getPartitionOffset(topic)
      val nowOffsetMap: HashMap[TopicPartition, Long] = KafkaUtil.getPartitionOffsetZK(topic, zkTopicPath, clusterEarliestOffsets, zkClient)

      kafkaStream = KafkaUtils.createDirectStream(streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams, nowOffsetMap))
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String](streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    }

    //通过rdd转换得到偏移量的范围
    var offsetRanges = Array[OffsetRange]()

    //对数据流进行处理
    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val value = rdd.map(rd => rd.value())
        val df = spark.read.json(value)

        //判断时间
        if (System.currentTimeMillis() - lastupdate > MyConf.update_conf) {

          println("定时执行")
          //加载地区表
          AreaTable = ReadTable.getTable(conf, MyConf.mysql_table_area)
          AreaTable.createOrReplaceTempView("area")

          //2.得到白名单关联表 解析出允许网段
          ipConfMap = AssociatedQueryConf.getNormalIpList(conf, sqlc)
          //获取白名单异常等级
          AbnormalIpLevel = AssociatedQueryConf.AbnormalIpLevel

          //3.解析出url规则  错误状态返回码
          mapUrlConf = AssociatedQueryConf.getIdentUrlConf(conf, sqlc)
          //获取认证失败异常等级
          failLevel = AssociatedQueryConf.failLevel
          listUrl = mapUrlConf.get("url").get
          listError = mapUrlConf.get("error").get
          listServerIp = mapUrlConf.get("serverIp").get
          listPort = mapUrlConf.get("port").get

          //4.提取所有服务系统端口 服务地址
          systemCode = ReadTable.getTable(conf, MyConf.mysql_table_sys)
          systemCode.createOrReplaceTempView("sys")
          sysPortIp = AssociatedQueryConf.getSystemIpPort(conf, sqlc, systemCode)
          allServerIp = sysPortIp.get("iplist").get
          allServerport = sysPortIp.get("portlist").get

          println("测试")
          println(allServerIp)
          println(allServerport)

          //5.获取异常时间模型
          timeConfList = AssociatedQueryConf.getAbnormalTimeMap(conf, sqlc)
          AbnormalTimeLevel = AssociatedQueryConf.abnorTimeLevel

          //6.获取文件操作url
          fileOperInfoList = AssociatedQueryConf.getDownAndUpUrlConf(conf, sqlc)
          fileOperLevel = AssociatedQueryConf.fileOperLevel

          //7.获取频繁认证模型配置
          failLandMoreMap = AssociatedQueryConf.getLandingTimesConf(conf, sqlc)
          failLandingMoreLevel = AssociatedQueryConf.failLandingTimesLevel

          //8.获取异地异常等级
          AssociatedQueryConf.getAbnorAreaLevel(conf, sqlc)
          abnorAreaLevel = AssociatedQueryConf.abnorAreaLevel

          lastupdate = System.currentTimeMillis()
        }

        //将dataframe识别的字段提取出来，观察是否存在需要字段
        val ColumnsList = df.columns.toList
        if (ColumnsList.contains("query") && ColumnsList.contains("http") && ColumnsList.contains("destination")) {

          //得到访问服务的全部流量   匹配上面提取出的服务系统地址和端口
          val dfData = df.filter($"destination.port".isin(allServerport: _*) && $"destination.ip".isin(allServerIp: _*))
          val data = dfData.select($"source.ip" as "ip",
            $"destination.ip" as "serviceIp",
            $"destination.port" as "port",
            $"@timestamp" as "requestTime",
            $"query" as "url", //判断是否认证
            $"url.full" as "query", //用来提取用户名  测试将url.query改成了url.full  query有时并不存在
            $"http.response.status_code" as "status_code"
          )

          if (!data.rdd.isEmpty()) {
            data.createOrReplaceTempView("flow")

            //将访问服务的流量与地址信息进行匹配
            val AreaIpMatching = sqlc.sql(
              "SELECT w.ip, serviceIp, port, requestTime, url, query, status_code, c_area from flow w LEFT JOIN area a ON w.ip = a.c_ip"
            )
            AreaIpMatching.createOrReplaceTempView("mat")

            //将端口和系统进行匹配 分辨访问了哪些系统
            val PortMatching = sqlc.sql(
              "SELECT m.ip, serviceIp, monitordict_id as sysid, m.port, requestTime, url, query, status_code, c_area from mat m LEFT JOIN sys s ON m.port = s.port"
            )

            println("查看访问系统")
            PortMatching.show()

            //第一次过滤  判断是否进行单点登陆认证的流量  使用coalesce合并分区 200->5
            val VisitFlow = PortMatching.filter($"port".isin(listPort: _*) && $"serviceIp".isin(listServerIp: _*) && $"status_code".isNotNull)

            VisitFlow.createOrReplaceTempView("visit")

            //第二次过滤  判断是否精确指定的IP 如果不是 进行下一步检测
            val AbnormalIpAccurate = VisitFlow.filter(!$"ip".isin(ipConfMap.get("ip").get.toList: _*)).coalesce(20)

            var AbnormalIp: DataFrame = AbnormalIpAccurate
            //精准IP过滤完之后，开始IP段过滤，将不在IP段内的定为异常输出
            for (i <- 0 to ipConfMap.get("ipRange").get.size - 1) {
              AbnormalIp.createOrReplaceTempView("ipPart")
              val str = ipConfMap.get("ipRange").get.toList(i)

              //防止模型输入为空
              if (!str.equals("null")) {
                val index = str.indexOf("-")
                val start = str.substring(0, index)
                val end = str.substring(index + 1)
                //表示循环查询是否在
                AbnormalIp = sqlc.sql(s"SELECT * FROM ipPart where ip NOT BETWEEN '${start}' AND '${end}'")
              }
            }
            if (!AbnormalIp.rdd.isEmpty()) {
              DataInsertIntoES.insertAborIpIntoES(AbnormalIp, AbnormalIpLevel, MyConf.whiteList, MyConf.abnor_status)
            }

            //精确查出白名单内的IP
            val normalIpAccurate = VisitFlow.filter($"ip".isin(ipConfMap.get("ip").get.toList: _*))
            for (i <- 0 to ipConfMap.get("ipRange").get.size - 1) {
              val str = ipConfMap.get("ipRange").get.toList(i)
              if (!str.equals("null")) {
                val index = str.indexOf("-")
                val start = str.substring(0, index)
                val end = str.substring(index + 1)
                //表示循环查询是否在
                val normalIpPart = sqlc.sql(s"SELECT * FROM visit where ip BETWEEN '${start}' AND '${end}'")

                //得到部分网段正常IP
                val normalIp = normalIpPart

                //第四次过滤 状态码和url进行匹配  匹配上为认证失败 输出
                if (!normalIp.rdd.isEmpty()) {

                  val IdentificationFlow = normalIp.filter($"status_code".isin(listError: _*) && $"url".isin(listUrl: _*)).coalesce(10)

                  val identFail = SparkStreamingIdentification.getIdentificationFail(IdentificationFlow, conf, sqlc)
                  //传入数据集、异常等级、异常类型、异常状态
                  if (!identFail.rdd.isEmpty()) {
                    //DBUtil.insertAnorDataIntoMysqlByJdbc(identFail, failLevel, MyConf.landingFail, MyConf.abnor_status)

                    //插入es
                    DataInsertIntoES.insertAnorDataIntoES(identFail, failLevel, failLandingMoreLevel, failLandMoreMap)
                  }

                  //第五次过滤 正常流量匹配
                  val identSuccessFlow = normalIp.filter(!$"status_code".isin(listError: _*) && $"url".isin(listUrl: _*))
                  val identSuccess = SparkStreamingIdentification.getIdentificationSuccess(identSuccessFlow, conf, sqlc)

                  //将用户名和IP映射关系存进redis
                  DBUtil.insertUserIpIntoRedis(identSuccess)

                  //DBUtil.insertNnorDataIntoMysqlByJdbc(identSuccess, AbnormalTimeLevel, MyConf.nor_level, MyConf.nor_type, MyConf.nor_status, timeMap)
                  //将数据插入es
                  DataInsertIntoES.insertAborTimeAndIdenSuccessIntoES(identSuccess,
                    AbnormalTimeLevel,
                    abnorAreaLevel,
                    MyConf.nor_level,
                    MyConf.nor_type,
                    timeConfList)
                }
              }
            }
            //登录成功 进入业务操作的流量
            val business = PortMatching.filter($"sysid" !== 1)
            //循环配置文件 得到每一个url 以及时间次数配置  不同的规则不同的输出   后面进行分区合并
            for (i <- 0 to fileOperInfoList.size - 1) {
              val fileOperData = business.filter($"url" === fileOperInfoList(i)._2).coalesce(3)
              if (!fileOperData.rdd.isEmpty()) {
                DataInsertIntoES.insertFileOperDataIntoES(fileOperData,
                  fileOperLevel,
                  MyConf.oper_file,
                  fileOperInfoList(i)._3.toInt,
                  fileOperInfoList(i)._4.toInt
                )
              }
              //得到所有的url集合
              fileOperUrlList += (fileOperInfoList(i)._2)
            }
            //正常业务流量
            val noOperFileFlow = business.filter(!$"url".isin(fileOperUrlList.toList: _*))
            if (!noOperFileFlow.rdd.isEmpty()) {
              DataInsertIntoES.insertData2Es(noOperFileFlow)
            }

            //把名单内的精准IP检测
            if (!normalIpAccurate.rdd.isEmpty()) {
              val IdentificationFlow = normalIpAccurate.filter($"status_code".isin(listError: _*) && $"url".isin(listUrl: _*)).coalesce(10)
              val identFail = SparkStreamingIdentification.getIdentificationFail(IdentificationFlow, conf, sqlc)
              if (!identFail.rdd.isEmpty()) {
                DataInsertIntoES.insertAnorDataIntoES(identFail, failLevel, failLandingMoreLevel, failLandMoreMap)
              }
              val identSuccessFlow = normalIpAccurate.filter(!$"status_code".isin(listError: _*) && $"url".isin(listUrl: _*))
              val identSuccess = SparkStreamingIdentification.getIdentificationSuccess(identSuccessFlow, conf, sqlc)
              DBUtil.insertUserIpIntoRedis(identSuccess)
              DataInsertIntoES.insertAborTimeAndIdenSuccessIntoES(identSuccess,
                AbnormalTimeLevel,
                abnorAreaLevel,
                MyConf.nor_level,
                MyConf.nor_type,
                timeConfList)
            }
          }
        }
        for (o <- offsetRanges) {
          val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
          //          println(s"${zkPath}_${o.untilOffset.toString}")
          ZkUtils(zkClient, false).updatePersistentPath(zkPath, o.untilOffset.toString)
        }
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
