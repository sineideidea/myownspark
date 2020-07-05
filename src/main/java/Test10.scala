import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Test10 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("test10").master("local[*]").enableHiveSupport().getOrCreate()
    val f1: DataFrame = spark.read.json("D:\\project\\myownspark\\data\\jsonArrayFile")
    f1.printSchema()
    f1.show(false)
    val rdd1: RDD[String] = spark.sparkContext.textFile("D:\\project\\myownspark\\data\\test1")
    spark.udf.register("my_split", (str: String) => {
      val jsonStr: String = str.split("\\|")(1)
      jsonStr

    })
    val rdd2: RDD[String] = rdd1.map(str => {
      str.split("\\|")(1)
    })
    import spark.implicits._
    val df1: DataFrame = spark.read.json(rdd2)
    df1.printSchema()
    val str_dataSet: Dataset[String] = rdd2.toDS()
    val df2: DataFrame = spark.read.json(str_dataSet)
    df2.printSchema()
    df2.createTempView("tmp")
    spark.sql(
      """
        |drop table if exists dwd_favorites_log
      """.stripMargin)
    spark.sql(
      """
         |CREATE EXTERNAL TABLE dwd_favorites_log(
         |`mid_id` string,
         |`user_id` string,
         |`version_code` string,
         |`version_name` string,
         |`lang` string,
         |`source` string,
         |`os` string,
 |`area` string,
 |`model` string,
 |`brand` string,
 |`sdk_version` string,
 |`gmail` string,
 |`height_width` string,
 |`app_time` string,
 |`network` string,
 |`lng` string,
 |`lat` string,
 |`id` int,
 |`course_id` int,
 |`userid` int,
 |`add_time` string,
 |`server_time` string
 |)
 |PARTITIONED BY (dt string)
 |location '/warehouse/mygmall/dwd/dwd_favorites_log/'


      """.stripMargin)
    val df3: DataFrame = spark.sql(
     """
       |select mid_id,user_id,et.en,et.ett,et.kv.id from
       |
       |(select
       |cm.mid mid_id,
       |cm.uid  user_id,
       |explode(et) as et
       |from tmp
       |)tmp2

     """
        .stripMargin)
    df3.printSchema()
    df3.show(false)


  }
}
