package util

import org.apache.hadoop.util.ProgramDriver
import sparkstreaming.SparkStreamingKafka
object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("SparkStreamingKafka",classOf[SparkStreamingKafka],"异常检测分析")
    driver.run(args)
  }
}
