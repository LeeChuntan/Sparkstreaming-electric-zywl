package db

import conf.MyConf
import java.sql.{Connection, DriverManager}

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}


object MysqlConnectionPool {

  private val pool = new GenericObjectPool[Connection](new MysqlConnectionFactory(

    MyConf.mysql_config("url"),
    MyConf.mysql_config("username"),
    MyConf.mysql_config("password"),
    MyConf.mysql_config("com.mysql.jdbc.Driver")))

  def getConnection:Connection = {
    pool.borrowObject()
  }

  def returnConnection(conn: Connection): Unit = {
    pool.returnObject(conn)
  }


  class MysqlConnectionFactory(url: String, username: String, password: String, className: String)
    extends BasePooledObjectFactory[Connection]{

    override def create(): Connection = {
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection(url, username, password)
    }

    override def wrap(conn: Connection): PooledObject[Connection] = {
      new DefaultPooledObject[Connection](conn)
    }

    override def validateObject(p: PooledObject[Connection]): Boolean = !p.getObject.isClosed

    override def destroyObject(p: PooledObject[Connection]): Unit = p.getObject.close()
  }
}