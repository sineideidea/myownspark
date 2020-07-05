import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp

import scala.util.Random

object ProduceDataToKafka {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.put("key.deserializer", classOf[StringSerializer])
    properties.put("value.deserializer", classOf[StringSerializer])
    val producer = new KafkaProducer[String, String](properties)
    var counter = 0
    var keyFlag = 0
    while (true) {
      counter = counter + 1
      keyFlag = keyFlag + 1
      val content: String = userLogs()
      producer.send(new ProducerRecord[String,String]("test1",3,s"key-$keyFlag",content))
      if(0 == counter%100){
        counter = 0
        Thread.sleep(5000)
      }
    }

    producer.close()
  }

  def userLogs() = {
    val userLogBuffer = new StringBuffer("")
    val timestamp: Long = System.currentTimeMillis()
    var userID = 0L
    var pageID = 0L
    userID = Random.nextInt(2000)
    pageID = Random.nextInt(2000)
    val channelNames: Array[String] = Array[String]("spark","Scala","Kafka","Flink","Hadoop","Storm","Hive","Impala","HBase","ML")
    val channel: String = channelNames(Random.nextInt(10))
    val actionNames: Array[String] = Array[String]("View","Register")
    val action: String = actionNames(Random.nextInt(2))
    val dateToday: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    userLogBuffer.append(dateToday)
      .append("\t")
      .append(timestamp)
      .append("\t")
      .append(userID)
      .append("\t")
      .append(pageID)
      .append("\t")
      .append(channel)
      .append("\t")
      .append(action)
    println(userLogBuffer.toString)
    userLogBuffer.toString





  }
}
