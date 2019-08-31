package test

import com.utils.JedisUtils
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import redis.clients.jedis.Jedis

object test {
  def main(args: Array[String]): Unit = {
//    val df: DateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHH")
//    println(df.parseDateTime("2019082908"))

    val jedis: Jedis = JedisUtils.getJedis()
    import scala.collection.JavaConversions._
    val list: List[(String, String)] = jedis.hgetAll("bs:offset:spa").toList
    list.foreach(_._1.split("[-]").foreach(println))
  }

}
