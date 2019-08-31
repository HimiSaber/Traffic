package exam

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bus.JedisOffset
import com.bus.KafkaRedisOffset.{OrdersPerMin, Top10, facts, failOrdersPerHour, hourlyFlow}
import com.utils.JedisUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import redis.clients.jedis.Jedis

object ExamKafkaRedis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(1))
    //检查点
    ssc.checkpoint("C:\\Users\\13996\\Desktop\\op\\checkpoint")

    val city: RDD[(String, String, String)] = ssc.sparkContext.textFile("C:\\Users\\13996\\Desktop\\op\\exam\\ip.txt")
      .map(_.split("\\|"))
      .map(arr => (arr(2), arr(3), arr(6)))
    city



     val cityArr: Broadcast[Array[(String, String, String)]] = ssc.sparkContext.broadcast(city.collect())
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "mall1"
    // topic
    val topic = "exam"
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
        val info: RDD[List[String]] = rdd.mapPartitions(part => {
          part.map(order => {
            val infos: Array[String] = order.value().split(" ")

            val ip: String = infos(1)
            val kind: String = infos(2)
            val fee: String = infos(4)

            val lon: Long = ip2Long(ip)
            val i: Int = binarySearch(cityArr.value,lon)
            val value: Array[(String, String, String)] = cityArr.value
            val pro: String = value(i)._3
            //(ip,种类,金额)
            List(ip, kind, fee,pro)
          })

        })
        info.cache()

        if(!info.isEmpty()) {
          //总的成交量
          allFee(info.map(list=>(list(2).toDouble)))

          //每个商品分类的成交量
          kindFee(info.map(list=>(list(1),list(2).toDouble)))

          //计算每个地域的商品成交总金额
          provFee(info.map(list=>(list(3),list(2).toDouble)))






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
    * 总金额
    * @param rdd
    */
  def allFee(rdd:RDD[Double]): Unit ={
    val fee: Double = rdd.reduce(_+_)
    val jedis: Jedis = JedisUtils.getJedis()
    jedis.hincrByFloat("exam:fee","allFee",fee)
    jedis.close()
  }


  def kindFee(rdd:RDD[(String, Double)]): Unit ={
    rdd.reduceByKey(_+_)
      .foreachPartition(part=>{
        val jedis: Jedis = JedisUtils.getJedis()
        part.foreach(tup=>{

          jedis.hincrByFloat("exam:kindFee",tup._1,tup._2)
        })

        jedis.close()
      })

  }


  def provFee(rdd: RDD[(String, Double)]): Unit ={

    rdd.reduceByKey(_+_)
      .foreachPartition(part=>{
        val jedis: Jedis = JedisUtils.getJedis()


        part.foreach(t=>{

          jedis.hincrByFloat("exam:prov",t._1,t._2)
        })

        jedis.close()
      })



  }


  /**
    * ip转十进制
    * @param ip
    * @return
    */

  def ip2Long(ip:String):Long ={
    val s = ip.split("[.]")
    var ipNum =0L
    for(i<-0 until s.length){
      ipNum = s(i).toLong | ipNum << 8L
    }
    ipNum
  }

//  def binarySearch(a:Array[(Long,String)],k:Long,n:Int): Int ={
//
//    var low:  Int = 0
//    var high: Int = n - 1
//
//    var ret :Int = -1
//    while (low <= high) {
//      val mid: Int = low + ((high - low) >> 1)
//      if (a(mid)._1>k) high = mid - 1
//      else if ((mid == n - 1) || (a(mid + 1)._1 > k)) ret = mid
//      else low = mid + 1
//    }
//
//    ret
//  }

  def binarySearch(arr: Array[(String, String, String)], ip: Long): Int = {
    //开始和结束值
    var start = 0
    var end = arr.length - 1
    while (start <= end) {
      //求中间值
      val middle = (start + end) / 2

      if ((ip >= arr(middle)._1.toLong) && (ip <= arr(middle)._2.toLong)) {
        return middle //return 在scala中作为中断,不写return则会继续往下执行，造成报错
      }
      if (ip < arr(middle)._1.toLong) {
        end = middle - 1
      } else {
        start = middle + 1
      }
    }
    -1
  }

}
