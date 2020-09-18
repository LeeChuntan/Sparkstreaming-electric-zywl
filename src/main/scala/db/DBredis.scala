package db

import conf.MyConf

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object DBredis {

  //  连接配置
  private val config = new JedisPoolConfig
  //  连接最大数
  config.setMaxTotal(200)
  //  最大空闲连接数
  config.setMaxIdle(10)

  //  最大空闲连接数
  config.setTestOnBorrow(true)

  //  设置连接池属性   配置  主机名 端口号 连接超时时间  redis密码
  val pool = new JedisPool(config, MyConf.REDIS_CONFIG("host"), MyConf.REDIS_CONFIG("port").toInt, MyConf.REDIS_CONFIG("timeout").toInt, MyConf.REDIS_CONFIG("passwd"))

  //  连接池
  def getConnections(): Jedis = {
    pool.getResource
  }


  def main(args: Array[String]): Unit = {

    //连接池的使用
    val jedis: Jedis = DBredis.getConnections()

    jedis.select(0)
    //
    //    // 指定键在集合的分值排名，从0开始算
    //    import scala.collection.convert.wrapAll._
    //    val r: util.Set[String] = jedis.zrevrange("txpath:news.stnn.cc", 0, 0)
    //    println(r)
    //
    //    for (i <- r) {
    //      val score: lang.Double = jedis.zscore("txpath:news.stnn.cc", i)
    //      println(score)
    //    }
    jedis.close()
  }
}
