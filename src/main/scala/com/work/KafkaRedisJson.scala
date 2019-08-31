package com.work

import java.lang
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bus.JedisOffset
import com.utils.JedisUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object KafkaRedisJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","600")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(2))
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "work"
    // topic
    val topic = "JsonData"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "node1:9092,node2:9092,node3:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }

    stream.foreachRDD(rdd=>{
      val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //业务

      val info: RDD[(String, Double,String)] = rdd.map(json => {
        val jSONObject: JSONObject = JSON.parseObject(json.value())
        val money: Double = jSONObject.getDouble("money")
        val date: String = jSONObject.getString("date")
        val phone: String = jSONObject.getString("phoneNum")

        (date, money,phone)
      })

      if(!info.isEmpty()){
        //每天充值总金额
        dayPayment(info.map(tup=>(tup._1.substring(0,10),tup._2)))
        //每月各手机号充值的平均金额
        avgPayOfPhone(info.map(tup=>((tup._1.substring(0,7),tup._3),tup._2)))



      }



      val jedis = JedisUtils.getJedis()
      for (or<-offestRange){
        jedis.hset("bs:offset:"+groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
      }
      jedis.close()

    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 每天充值总金额
    */

  def dayPayment(rdd: RDD[(String, Double)]): Unit ={
    rdd.reduceByKey(_+_)
      .foreachPartition(part=>{
        val jedis: Jedis = JedisUtils.getJedis()

        part.foreach(pay=>{
          jedis.hincrByFloat("work:DayPayment", pay._1,pay._2)
        })

        jedis.close()
      })

  }

  def avgPayOfPhone(rdd: RDD[((String, String), Double)]): Unit ={
    //日期，手机 money
    rdd.foreachPartition(part=>{
        val jedis: Jedis = JedisUtils.getJedis()

        part.foreach(tup=>{
          //检查手机:日期
          if(jedis.hexists("work:"+tup._1._2+":"+tup._1._1,"cnt")){
            val fee: Double = jedis.hget("work:"+tup._1._2+":"+tup._1._1,"money").toDouble
            val cnt: Int = jedis.hget("work:"+tup._1._2+":"+tup._1._1,"cnt").toInt

            jedis.hset("work:"+tup._1._2+":"+tup._1._1,"money",((fee+tup._2)/(cnt+1)).toString)
            jedis.hset("work:"+tup._1._2+":"+tup._1._1,"cnt",(cnt+1).toString)
          }else{
            jedis.hset("work:"+tup._1._2+":"+tup._1._1,"money",tup._2.toString)
            jedis.hset("work:"+tup._1._2+":"+tup._1._1,"cnt","1")
          }
        })
        jedis.close()
      })
  }


}
