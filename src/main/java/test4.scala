import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.jets3t.service.model.cloudfront.StreamingDistributionConfig

object test4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test4")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val result: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_))
    result.print()






    ssc.start()
    ssc.awaitTermination()


  }
}
