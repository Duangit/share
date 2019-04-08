import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription
import redis.clients.jedis.JedisCluster
import sinks.MyRedisRichMapper

class MyRedisMapperDemo extends MyRedisRichMapper[(String,(String,String))] with Serializable {

  private val serialVersionUID=456L

  override def getCommandDescription: RedisCommandDescription ={
    null
  }


  override def getKeyFromData(data: (String, (String, String))): String = ???

  override def getValueFromData(data: (String, (String, String))): String = ???

  /**
    * 自定义的通用方法
    * @param input
    * @param jedisCluster
    */
  override def handlFunction(input: (String, (String, String)), jedisCluster: JedisCluster): Unit = {

    /**
      * 处理JedisCluster具体逻辑
      */
    //    jedisCluster.hset(key,"uv",uv)
  }
}
