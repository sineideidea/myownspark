import java.util

import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

object Test8 {
  def main(args: Array[String]): Unit = {
    val jedis = new Jedis("hadoop105",6379)
    val str: String = jedis.ping()
    jedis.set("key1","a")
    jedis.lpush("key2","a","b","c")
    val map = new util.HashMap[String,String]()
    map.put("a","1")
    map.put("b","2")


    jedis.hmset("hash1",map)
    jedis.hgetAll("hash1")
    jedis.hmget("hash1","a","b")

    val ports: util.HashSet[HostAndPort] = new util.HashSet[HostAndPort]()
    ports.add(new HostAndPort("hadoop105",6379))
    new JedisCluster(ports)

    val keys: util.Set[String] = jedis.keys("*")
      val keyiterator: util.Iterator[String] = keys.iterator()
    while (keyiterator.hasNext){
      println(keyiterator.next())
    }
    val lrange: util.List[String] = jedis.lrange("key2",0,-1)
    println(lrange)
  }
}
