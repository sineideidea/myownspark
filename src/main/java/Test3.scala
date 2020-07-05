import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test3 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[*]").appName("test3").getOrCreate()
    val frame1 = session.read.json("./data/NestJsonFile")
    frame1.select("infos.age","infos.gender").show()
    frame1.printSchema()
    val list = List[String](
      "{\"name\":\"zhangsan\",\"age\":\"18\"}",
      "{\"name\":\"lisi\",\"age\":\"19\"}",
      "{\"name\":\"wangwu\",\"age\":\"20\"}"
    )
    import session.implicits._
    import org.apache.spark.sql.functions._
    val jsonRDd: RDD[String] = session.sparkContext.parallelize(list)
    val frame3 = session.read.json(jsonRDd)

    val frame2 = list.toDF("value")
    frame2.select(get_json_object($"value","$.name")).show()
    frame3.show()
    val frame4 = session.read.json("./data/jsonArrayFile")
    frame4.createTempView("t1")
    session.sql(
      """
        |select name,age,lag(age,1,0) over(partition by name order by age)from(
        |select age,name,col.yuwen,col.dili,col.shuxue,col.yingyu from (
        |select age,name,explode(scores) as col from t1)t2)t3
      """.stripMargin).show(false)

  }
}
