package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisUtils {
  private val conf:JedisPoolConfig = new JedisPoolConfig()

  private def init(): Unit ={
    conf.setMaxTotal(60)
    conf.setMaxIdle(30)
  }

  private val pool = new JedisPool(conf,"node3",6379,10000)


  def getJedis(): Jedis={
    pool.getResource
  }




}
