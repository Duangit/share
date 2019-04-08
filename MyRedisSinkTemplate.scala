package sinks

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.slf4j.LoggerFactory
import redis.clients.jedis.{HostAndPort, JedisCluster}


/**
  *
  * 通用sink  Template  具体处理逻辑在RedisMapper中实现
  *
  * @tparam IN
  */
class MyRedisSinkTemplate[IN] extends RichSinkFunction[IN] with Serializable {

  private val serialVersionUID = 1L
  private val LOG = LoggerFactory.getLogger(classOf[MyRedisSinkTemplate[_]])
  private val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
  private var myRedisRichMapper:MyRedisRichMapper[IN]=null


  private var jedisCluster:JedisCluster=null
  private var nodes:String=null
  private var password:String=null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //初始化连接
    val nodelist = nodes.trim.split("\\,")
    for(nodeport :String<-nodelist){
      val args = nodeport.split("\\:")
      val host=args(0)
      val port=args(1)
      jedisClusterNodes.add(new HostAndPort(host,port.toInt))
    }
    //创建连接对象
    if (password==null){
      jedisCluster=new JedisCluster(jedisClusterNodes)
    }else{
      jedisCluster=new JedisCluster(jedisClusterNodes, 10000, 1000, 1, password, new GenericObjectPoolConfig)
    }
  }

  /**
    * 带密码的集群连接创建方式
    * @param nodes
    * @param redisRichMapper 逻辑mapper
    * @param password
    */
  def this(nodes:String,redisRichMapper: MyRedisRichMapper[IN],password:String)={
    this()
    myRedisRichMapper = redisRichMapper
    this.nodes=nodes
    this.password=password
  }

  /**
    * 不带密码的集群连接创建方式
    * @param nodes
    * @param redisRichMapper 逻辑mapper
    * @return
    */
  def this(nodes:String,redisRichMapper: MyRedisRichMapper[IN])={
    this(nodes,redisRichMapper,null)
  }

  /**
    * 通用处理逻辑
    * @param input
    */
  override def invoke(input: IN): Unit = {
    /**
      * 对具体的每一条数据，执行处理具体的redis执行逻辑
      */
    myRedisRichMapper.handlFunction(input,jedisCluster)
  }


  override def close(): Unit = {
    super.close()
    try {
      jedisCluster.close()
    }catch {
      case e:Exception=>{
        println(e.getCause)
        jedisCluster=null
      }
    }
  }
}
