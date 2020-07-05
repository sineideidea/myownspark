
import org.apache.spark.{SparkConf, SparkContext}

object test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test2")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("hdfs://hadoop102:9000/datatest",4)
    val rdd2 = rdd1.flatMap(_.split("\t")).map((_,1))
    val rdd3 = rdd1.flatMap(_.split("\t")).map((_,1))
    rdd2.join(rdd3).reduceByKey{
      case (a,b)=>{
         val c1 = math.max(a._1,a._2) + math.max(b._1,b._2)
        val c2 = math.min(a._1,b._2)
        (c1,c2)

      }
    }.count()
    println("git_learning")

  }
}
