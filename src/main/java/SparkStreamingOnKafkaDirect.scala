import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkStreamingOnKafkaDirect {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(" SparkStreamingOnKafkaDirect").setMaster("local")

    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val kafkaParameter: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "mynode1:9092,mynode2:9092,mynode3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mygroupid",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    val topics: Array[String] = Array[String]("test1")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topics,
        kafkaParameter
      )
    )
    val lines: DStream[String] = stream.map(_.value())
    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey((_ + _)).print()
    stream.foreachRDD(rdd => {

      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (e <- offsetRanges) {
        println(s"topic = ${e.topic}, parititon = ${e.partition},fromoffset = ${e.fromOffset},untilOffset = ${e.untilOffset}")

      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)


    }
    )
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
