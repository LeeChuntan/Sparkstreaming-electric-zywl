package conf

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

object MyConf {
  /*
    //kafka配置文件
    val kafka_topic: String = "sparkstreaming"

    val kafka_group: String = "packetbeat_test"

    val kafka_brokers: String = "s1:9092"

    val zookeeper: String = "s1:2181"

   //kafka的offset读取位置
    final val kafka_offset_position: String = "earliest"

    // mysql 配置
  //  final val mysql_config: Map[String, String] = Map("url" -> "jdbc:mysql://localhost/test", "username" -> "root", "password" -> "")
    final val mysql_config: Map[String, String] = Map("url" -> "jdbc:mysql://10.107.42.150:3306/bmap?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useSSL=false", "username" -> "root", "password" -> "123456")

    final val mysql_table_whitelist: String = "t_analsmodel_json"

    final val mysql_table_status: String  = "status_list"

    final val mysql_table_area: String = "t_device"

    final val ResultTable: String  = "t_result"

    final val mysql_table_model: String = "t_analsmodel"

    final val mysql_table_sys: String = "t_monitor_info"

    final val mysql_table_usernameIp: String = "t_usernameIp"

    //异常状态配置 '类型1-异常ip认证2-异常时间认证3-认证失败4-越权访问5-访问超时',
    final val abnor_status: Int = 1             //异常状态值
    final val nor_status: Int = 0               //正常状态值
    final val nor_type: String = "0"            //正常类型
    final val whiteList: String = "1"           //白名单异常类型
    final val abnor_Time: String = "2"          //异常时间段异常类型
    final val landingFail: String = "3"         //认证失败异常类型
  //  final val
    final val landing_timeOut: String = "5"     //持续登录失败认证
    final val nor_level: Int = 0                //正常情况下异常等级
    final val num_landing: Int = 5               //认证失败次数阀值

    //读取异常等级和规则配置分类设置
    final val modeld_white: Int = 1              //白名单异常模型编号
    final val modeld_time: Int = 3              //异常时间点异常模型编号
    final val modeld_landing: Int = 4            //认证url配置模型编号
    final val modeld_download: Int = 5
    final val ident_timeOut: Int = 300        //认证时间阀值 设定五分钟


    //ES的host
    final val es_host:String = "s1"

    //ES的端口
    final val es_port:String = "9200"

    //ES的index和type
    final val es_index_type:String = "test2esnew"

    //配置更新时间 三分钟
    final val update_conf: Long = 180000L
    //redis配置
    final val REDIS_CONFIG: Map[String, String] = Map("host" -> "s3", "port" -> "6379", "timeout" -> "10000", "passwd" -> "123456")*/


  val postgprop = new Properties
   val ipstream = new BufferedInputStream(new FileInputStream("C:\\Users\\asus\\Desktop\\MyConf.properties"))
//  val ipstream = new BufferedInputStream(new FileInputStream("/home/zywl/jar_spark/conf/MyConf.properties"))
  postgprop.load(ipstream)

  val Durations_seconds: Int = postgprop.getProperty("Durations_seconds").toInt
  val kafka_topic: String = postgprop.getProperty("kafka_topic")
  val kafka_group: String = postgprop.getProperty("kafka_group")
  val kafka_brokers: String = postgprop.getProperty("kafka_brokers")
  val zookeeper: String = postgprop.getProperty("zookeeper")

  //kafka的offset读取位置
  final val kafka_offset_position: String = postgprop.getProperty("kafka_offset_position")

  // mysql 配置
  //  final val mysql_config: Map[String, String] = Map("url" -> "jdbc:mysql://localhost/test", "username" -> "root", "password" -> "")
  final val mysql_config: Map[String, String] = Map("url" -> postgprop.getProperty("jdbc_url"), "username" -> postgprop.getProperty("jdbc_username"), "password" -> postgprop.getProperty("jdbc_password"))

  final val mysql_table_whitelist: String = postgprop.getProperty("mysql_table_whitelist")

  final val mysql_table_status: String = postgprop.getProperty("mysql_table_status")

  final val mysql_table_area: String = postgprop.getProperty("mysql_table_area")

  final val ResultTable: String = postgprop.getProperty("ResultTable")

  final val mysql_table_model: String = postgprop.getProperty("mysql_table_model")

  final val mysql_table_sys: String = postgprop.getProperty("mysql_table_sys")

  final val mysql_table_usernameIp: String = postgprop.getProperty("mysql_table_usernameIp")

  //异常状态配置 '类型1-异常ip认证2-异常时间认证3-认证失败4-越权访问5-访问超时',
  final val abnor_status: String = postgprop.getProperty("abnor_status") //异常状态值
  final val nor_status: String = postgprop.getProperty("nor_status") //正常状态值
  final val nor_type: String = postgprop.getProperty("nor_type") //正常类型

  final val whiteList: String = postgprop.getProperty("whiteList") //白名单异常类型
  final val abnor_Time: String = postgprop.getProperty("abnor_Time") //异常时间段异常类型
  final val landingFail: String = postgprop.getProperty("landingFail") //认证失败异常类型
  final val landing_timeOut: String = postgprop.getProperty("landing_timeOut") //持续登录失败异常类型
  final val abnormal_area: String = postgprop.getProperty("abnormal_area") //异地登陆异常类型
  final val oper_file: String = postgprop.getProperty("oper_file") //文件操作频繁异常类型

  final val nor_level: Int = postgprop.getProperty("nor_level").toInt //正常情况下异常等级


  //读取异常等级和规则配置分类设置
  final val modeld_white: Int = postgprop.getProperty("modeld_white").toInt //白名单异常模型编号
  final val modeld_area: Int = postgprop.getProperty("modeld_area").toInt //白名单异常模型编号
  final val modeld_time: Int = postgprop.getProperty("modeld_time").toInt //异常时间点异常模型编号
  final val modeld_landing: Int = postgprop.getProperty("modeld_landing").toInt //认证url配置模型编号
  final val modeld_download: Int = postgprop.getProperty("modeld_download").toInt //文件操作模型设置编号
  final val modeld_landTime: Int = postgprop.getProperty("model_landTime").toInt //认证次数以及时间设置模型


  // final val ident_timeOut: Int = postgprop.getProperty("ident_timeOut").toInt        //认证时间阀值 设定五分钟
  // final val num_landing: Int = postgprop.getProperty("num_landing").toInt              //认证失败次数阀值

  // final val num_operFile: Int = postgprop.getProperty("num_operFile").toInt           //文件操作次数阀值设置
  // final val time_operFile: Int = postgprop.getProperty("time_operFile").toInt           //文件操作时间阀值设置

  //ES的host
  final val es_host: String = postgprop.getProperty("es_host")

  //ES的端口
  final val es_port: String = postgprop.getProperty("es_port")

  //ES的index和type
  final val es_index_type: String = postgprop.getProperty("es_index_type")

  //配置更新时间 三分钟
  final val update_conf: Long = postgprop.getProperty("update_conf").toLong
  //redis配置
  final val REDIS_CONFIG: Map[String, String] = Map("host" -> postgprop.getProperty("redis_host"), "port" -> postgprop.getProperty("redis_port"), "timeout" -> postgprop.getProperty("redis_timeout"), "passwd" -> postgprop.getProperty("redis_passwd"))

}
