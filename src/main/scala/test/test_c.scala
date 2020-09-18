package test

import com.alibaba.fastjson.JSONObject
import db.DBredis
import redis.clients.jedis.Jedis

/**
  * @program: sparkstreaming
  * @author: LiChuntao
  * @create: 2020-07-02 16:30
  */
object test_c {
  def main(args: Array[String]): Unit = {

    var jsonObj = new JSONObject()
    val redis: Jedis = DBredis.getConnections()
    jsonObj.put("pid", 1)
    jsonObj.put("username", "uuu")
    jsonObj.put("level", 2)
    redis.publish("bmap_alarm_channel", jsonObj.toString)

  }
}
