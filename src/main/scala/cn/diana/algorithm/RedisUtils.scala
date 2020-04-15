package cn.diana.algorithm

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import redis.clients.jedis.util.Pool

import redis.clients.jedis.{Jedis, JedisPool}

class RedisUtils() {
  @transient lazy val log = Logger.getLogger(this.getClass)

  private[this] var jedisPool: Pool[Jedis] = _

  def init(host: String, port: Int, timeout: Int, password: String, database: Int = 0): Unit = {
    jedisPool = new JedisPool(new GenericObjectPoolConfig, host, port, timeout, password, database)
  }

  def saveModel(key: String, value: String): Unit = {
    log.info("redis中key为：" + key)
    val jedis = jedisPool.getResource
    jedis.set(key, value)
    jedis.close()
  }

  def getModel(key: String): String = {
    log.info("redis中key为：" + key)
    val jedis = jedisPool.getResource
    val value = jedis.get(key)
    jedis.close()
    value
  }

}