package BusinessAnalysis


import conf.MyConf
import method.DataInsertIntoES
import org.apache.spark.sql.{DataFrame, SQLContext, _}
import org.apache.spark.SparkConf


/**
  * 业务操作流量
  */
object SparkstreamingBusiness {

  def businessAnalysis(conf: SparkConf, sqlc: SQLContext, dataFrame: DataFrame): Unit = {

  }
}
