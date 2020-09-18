package method

import com.mysql.jdbc.Driver
import conf.MyConf
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object ReadTable {

  /**
    * 读表方法
    *
    * @param conf
    * @return
    */
  def getTable(conf: SparkConf, tablename: String): DataFrame = {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val table: DataFrame = spark.read.format("jdbc")
      .option("driver", classOf[Driver].getName)
      .option("url", MyConf.mysql_config("url"))
      .option("dbtable", tablename)
      .option("user", MyConf.mysql_config("username"))
      .option("password", MyConf.mysql_config("password")).load()
    table
  }
}
