import java.{lang, util}

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Durations, StreamingContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable

object ManageOffSetUseRedis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("ManageOffSetUseRedis")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "10")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val kafkaParam: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "group.id" -> "mymygoupid",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics: Array[String] = Array[String]("test1")
    val dbIndex = 3
    val currentTopicOffset: mutable.Map[String, String] = getOffSetFromReis(3, "test1")
    for (elem <- currentTopicOffset) {
      println(s"初始读到的offset:$elem")
    }
    val fromOffsets: Map[TopicPartition, Long] = currentTopicOffset.map {
      case (partiton, offset) =>
        new TopicPartition("test1", partiton.toInt) -> offset.toLong
    }.toMap

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String,String](fromOffsets.keys, kafkaParam, fromOffsets)
    )

    val lines: DStream[String] = stream.map(_.value())
    lines.flatMap(_.split("\t")).map((_, 1)).reduceByKey((_ + _)).print()

    stream.foreachRDD(rdd => {

      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("业务处理完成")

      rdd.foreachPartition(iter => {
        val o: OffsetRange = offsetRanges(TaskContext.getPartitionId())
      println(s"topic:${o.topic} partition:${o.partition} fromOffset:${o.fromOffset} untilOffset:${o.untilOffset}")

      saveOffSetFromRedis(dbIndex,offsetRanges)


      }
      )


    }


    )

  }

  def saveOffSetFromRedis(db: Int, offserRanges: Array[OffsetRange]) = {
    val pool = new JedisPool(new GenericObjectPoolConfig(), "hadoop105", 6379, 30000)
    val jedis: Jedis = pool.getResource
    jedis.select(db)
    for (elem <- offserRanges) {
      jedis.hset(elem.topic, elem.partition.toString, elem.untilOffset.toString)
    }
    println("保存成功")
    pool.returnResource(jedis)


  }

  def getOffSetFromReis(db: Int, topic: String) = {
    lazy val pool: JedisPool = new JedisPool(new GenericObjectPoolConfig(), "hadoop105", 6379, 30000)
    val jedis: Jedis = pool.getResource
    jedis.select(db)
    val result: util.Map[String, String] = jedis.hgetAll(topic)
    pool.returnResource(jedis)
    if (result.size() == 0) {
      result.put("0", "0")
      result.put("1", "0")
      result.put("2", "0")

    }
    import scala.collection.JavaConversions.mapAsScalaMap
    val offsetMap: scala.collection.mutable.Map[String, String] = result

    offsetMap
  }

}
