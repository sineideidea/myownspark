import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable

object test2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("test2").master("local[*]").getOrCreate()

    //    sparkSession.read.json("")
    //    val options = new mutable.HashMap[String,String]()
    //    options.put("url","jdbc:mysql://hadoop104:3306/spark")
    //    options.put("driver","com.mysql.jdbc.Driver")
    //    options.put("password","132302")
    //    options.put("dbtable","person")
    //    options.put("user","root")
    //    import sparkSession.implicits._
    //    val frame2 = List(1,2,3).toDF()
    //    frame2.printSchema()
    //    val frame1 = sparkSession.read.format("jdbc").options(options).load()
    //    frame1.write.format("jdbc")
    //    .options(options).mode(SaveMode.Append)
    //    frame1.write.saveAsTable()
    //    sparkSession.stop()
    //  }
    val frame1 = sparkSession.read.json("./data/json")

    val person = frame1.rdd.map {
      case row =>
        Person(row.getAs[String]("name"), row.getAs[Long]("age").toInt)
    }.repartition(3).aggregate(Person("zhangsan", 0))((p1, p2) =>
    if ("zhangsan".equals(p2.name)) {
      Person("zhangsan", p1.age + 1)
    } else p1

      , (p1, p2) => Person("zhangsan", p1.age + p2.age))
    println(person)
//      .reduce {
//      case (p1, p2) =>
//        if ("zhangsan".equals(p1.name)) {
//          Person(p1.name + p2.name, p1.age + p2.age)
//        }else {
//          p1
//        }
//    }
      sparkSession.udf.register("agg",new MyUdaf)
    frame1.createTempView("t1")
    sparkSession.sql(
      """
        |select agg(name,age) from t1
      """.stripMargin).show()
    new LongAccumulator
  }
}
case class Person(name:String,age:Int)
