package com.bus

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.utils.{JedisUtils, String2DateTime}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import redis.clients.jedis.Jedis
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(1))
    //检查点
    ssc.checkpoint("C:\\Users\\13996\\Desktop\\op\\checkpoint")

    val city: RDD[(String, String)] = ssc.sparkContext.textFile("D:\\input\\city.txt")
      .map(_.split(" "))
      .map(arr => (arr(0), arr(1)))


    val cityMp: Broadcast[Map[String, String]] = ssc.sparkContext.broadcast(city.collect().toMap)
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "spa"
    // topic
    val topic = "cmccpay"
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
    stream.foreachRDD({
      rdd=>
        //创建Jedis连接


        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 业务处理
        //rdd.map(_.value()).foreach(println)
        //val info: RDD[(Double, Int, Int)] =
        val info: RDD[List[String]] = rdd.mapPartitions(part => {
          val jedis = JedisUtils.getJedis()
          try {
            part.map(json => {
              val jSONObject: JSONObject = JSON.parseObject(json.value())
              //服务请求结果
              val rst: String = jSONObject.getString("bussinessRst")
              //接口服务名
              val serName: String = jSONObject.getString("serviceName")

              //充值金额(成功)
              val fee: String = if (serName.equals("reChargeNotifyReq")&&rst.equals("0000")) jSONObject.getString("chargefee") else "0.0"
              //订单数量(成功)
              val sucOrders: String = if (serName.equals("reChargeNotifyReq")&&rst.equals("0000")) "1" else "0"
             //所有订单
              val orders: String = if (serName.equals("reChargeNotifyReq")) "1" else "0"

              //充值失败数量
              val failPay: String = if(serName.equals("reChargeNotifyReq")&&rst.equals("0000")) "0" else "1"

              //省份Code
              val provinceCode: String =if(serName.equals("reChargeNotifyReq")) jSONObject.getString("provinceCode") else ""



              //充值时间
              val payTime: String = jSONObject.getString("requestId")
              //响应时间
              val rspTime: String = jSONObject.getString("receiveNotifyTime")

              //分钟
              val min: String = payTime.substring(0, 12)


              //
              //每分钟统计次数

              //(请求结果,充值金额，成功订单,分钟,失败订单，省份,所有订单)
              List(rst, fee, sucOrders, min,failPay,provinceCode,orders)


            })
          } finally {
            jedis.close()
          }
        }).cache()

        if(!info.isEmpty()) {
          //统计全网的充值订单量, 充值金额, 充值成功数
          //info.map(list=>(list(1).toDouble,list(2).toInt,1))
          facts(info.map(list=>(list(1).toDouble,list(2).toInt,list(6).toInt)))

          //实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
          OrdersPerMin(info.map(list=>list(3)))

          //统计每小时各个省份的充值失败数据量
          failOrdersPerHour(info.map(list=>(list(5),list(4).toInt,list(3).substring(0,10))).filter(_._1.size>0),cityMp.value)

          //以省份为维度统计订单量排名前 10 的省份数据
          Top10(info.map(list=>(list(5),list(6).toDouble,list(4).toDouble)),cityMp.value)

          //实时统计每小时的充值笔数和充值金额。
          hourlyFlow(info.map(list=>(list(3).substring(0,10),list(2).toInt,list(1).toDouble)))



        }

        // 将偏移量进行更新

        val jedis = JedisUtils.getJedis()
        for (or<-offestRange){

          jedis.hset("bs:offset:"+groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 1)	统计全网的充值订单量, 充值金额, 充值成功数
    * 待优化
    */
  def facts(rdd: RDD[(Double, Int, Int)])={

    rdd.foreachPartition(list=>{
      val jedis: Jedis = JedisUtils.getJedis()
      try {
        list.foreach(ret => {
          //充值金额
          jedis.hincrByFloat("bs:facts", "paymentSum",ret._1)
          jedis.hincrBy("bs:facts", "orderSucCount", ret._2.toLong)
          jedis.hincrBy("bs:facts", "orderCount", ret._3.toLong)

//          if (jedis.hexists("bs:facts", "paymentSum")) {
//            val last: Double = jedis.hget("bs:facts", "paymentSum").toDouble + ret._1
//            jedis.hset("bs:facts", "paymentSum", last.toString)
//          } else {
//            jedis.hset("bs:facts", "paymentSum", ret._1.toString)
//          }
//
//          if (jedis.hexists("bs:facts", "orderSucCount")) {
//            val last: Int = jedis.hget("bs:facts", "orderSucCount").toInt + ret._2
//            jedis.hset("bs:facts", "orderSucCount", last.toString)
//          } else {
//            jedis.hset("bs:facts", "orderSucCount", ret._2.toString)
//          }
//          if (jedis.hexists("bs:facts", "orderCount")) {
//            val last: Int = jedis.hget("bs:facts", "orderCount").toInt + ret._3
//            jedis.hset("bs:facts", "orderCount", last.toString)
//          } else {
//            jedis.hset("bs:facts", "orderCount", ret._3.toString)
//          }

        })
      }finally {
        jedis.close()
      }
    })

  }

  /**
    * 2)	实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
    *
    */

  def OrdersPerMin(rdd :RDD[String])={
      rdd.foreachPartition(str=>{
        val jedis: Jedis = JedisUtils.getJedis()
        try {
          str.foreach(time => {
            jedis.hincrBy("bs:countPerMin", time, 1)
            //检查分钟是否存在
//            if (jedis.hexists("bs:countPerMin", time)) {
//              //存在就获取到+1
//              val cnt: Long = jedis.hget("bs:countPerMin", time).toLong + 1
//              jedis.hset("bs:countPerMin", time, cnt.toString)
//            } else {
//              //没有就直接set
//              jedis.hset("bs:countPerMin", time, "1")
//            }
          })
        }finally {
          jedis.close()
        }

      })


  }

  /**
    * 统计每小时各个省份的充值失败数据量
    * @param list
    * @param map
    */


  def failOrdersPerHour(list: RDD[(String,Int,String)],map: Map[String, String])={

      //(省份，订单，小时)
    DBs.setup()
    val ret: RDD[((String, String), Int)] = list
        .filter(!_._1.equals(""))
      .map(tup => ((tup._1, tup._3), tup._2))
      .reduceByKey(_ + _).map(tup=>((map.getOrElse(tup._1._1,""),tup._1._2),tup._2))
      ret.foreachPartition(part=>{
        part.foreach(rt=>{
//          val pro: List[String] = DB readOnly { implicit session =>
//            SQL(s"select province,hour,failorders from t_provFail where province=? and hour=? ")
//              .bind(rt._1._1,rt._1._2)
//              .map(rs => rs.string("failorders"))
//              .list()
//              .apply()
//          }
//
//          if(pro.isEmpty){
            DB autoCommit {implicit session =>
              SQL("insert into t_provFail(province,hour,failorders) values(?,?,?)")
                .bind(rt._1._1,String2DateTime.get2Hour(rt._1._2),rt._2.toInt)
                .update()
                .apply()
            }
//          }else{
//            DB localTx {implicit session =>
//              SQL("update t_provFail  set failorders=? where province=? and hour=?")
//                .bind(pro(0).toInt+rt._2,rt._1._1,rt._1._2)
//                .update()
//                .apply()
//            }
//          }
        })

      })

  }

  /**
    * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。
    */

  def Top10(rdd:RDD[(String, Double, Double)],city: Map[String, String]): Unit ={
    //(省份code,所有订单,失败订单)
    DBs.setup()

    rdd.filter(!_._1.equals(""))
      .map(tup=>(city.getOrElse(tup._1,""),(tup._2,tup._3)))
      .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
      .foreachPartition(part=>{
        part.foreach(rt=>{
//          val pro: List[(Int, Int)] = DB readOnly { implicit session =>
//            SQL("select province,orders,failedPercent,failOrders from t_ordersCount where province=? ")
//              .bind(rt._1)
//              .map(rs => (rs.int("orders"), rs.int("failOrders")))
//              .list()
//              .apply()
//          }
//
//          if(pro.isEmpty){
          val dou: Double = if(rt._2._1==0.0) 0.0 else (rt._2._2/rt._2._1).formatted("%.1f").toDouble


            DB autoCommit  {implicit session =>
              SQL("insert into t_ordersCount(province,orders,failedPercent,failOrders) values(?,?,?,?)")
                .bind(rt._1,rt._2._1,dou,rt._2._2)
                .update()
                .apply()
            }
//          }else{
//            DB localTx {implicit session =>
//              SQL("update t_provFail  set orders=?,failedPercent=?,failOrders=? where province=?")
//                .bind(rt._2._1,(rt._2._2/rt._2._1).formatted("%.1f"),rt._2._2,rt._1)
//                .update()
//                .apply()
//            }
//          }

        })
      })
  }


  /**
    * 实时统计每小时的充值笔数和充值金额。
    */

  def hourlyFlow(rdd:RDD[(String, Int, Double)]): Unit ={

    DBs.setup()
    //(小时,订单数,订单金额)
    rdd.map(tup=>(tup._1,(tup._2,tup._3)))
      .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
      .foreachPartition(part=>{
      part.foreach(rt=>{
        DB autoCommit {implicit session =>
          SQL("insert into t_hourlyFlow(hour,orders,fee) values(?,?,?)")
            .bind(String2DateTime.get2Hour(rt._1),rt._2._1,rt._2._2)
            .update()
            .apply()
        }
      })
    })
  }





}
