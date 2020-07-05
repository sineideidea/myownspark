import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Test5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test5").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Durations.seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    ssc.checkpoint("./data/checkpoint")
    println("**************************************************************************")
    val pairWords: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))
//    val result: DStream[(String, Int)] = pairWords.updateStateByKey {
//      case (seq, option) =>
//        var preValue: Int = option.getOrElse(0)
//        for (elem <- seq) {
//          preValue = preValue + elem
//        }
//        Option(preValue)
//    }
      val result: DStream[(String, Int)] = pairWords.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Durations.seconds(15),Durations.seconds(5))
    val result1: DStream[(String, Int)] = pairWords.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      (a: Int, b: Int) => a - b,
      Durations.seconds(15),
      Durations.seconds(5)
    )
    result1.print()
   // result.print()
    ssc.start()
    ssc.awaitTermination()



  }
}
