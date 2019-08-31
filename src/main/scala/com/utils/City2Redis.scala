package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object City2Redis {
  def main(args: Array[String]): Unit = {
    val ssc: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    val ret: RDD[Array[String]] = ssc.sparkContext
      .textFile("D:\\input\\city.txt")
      .map(_.split(" "))

    val jedis: Jedis = JedisUtils.getJedis()
    ret.collect().foreach(a=>jedis.hset("dict:city",a(0),a(1)))
    jedis.close()


  }

}
