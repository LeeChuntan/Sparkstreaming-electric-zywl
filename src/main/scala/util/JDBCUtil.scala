package util

import conf.MyConf.mysql_config
import java.sql.{Connection, DriverManager}
object JDBCUtil {
  classOf[com.mysql.cj.jdbc.Driver]

  def getConnection: Connection = {
    DriverManager.getConnection(mysql_config("url"), mysql_config("username"),mysql_config("password"))
  }
}
