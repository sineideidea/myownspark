import com.google.gson.{JsonElement, JsonObject}
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



object test9 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test9")
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
   val rdd1: RDD[String] = session.sparkContext.textFile("D:\\project\\myownspark\\data\\test1")
    val rdd2: RDD[String] = rdd1.map(str=>str.split("\\|")(1))
    session.udf.register("udf",(str:String)=>{
      val logContens: Array[String] = str.split("\\|")
      if (logContens.length !=2||StringUtils.isBlank(logContens(1))){
        ""
      }else{
        val jsonObject = new JsonObject()

        val json: JsonObject = jsonObject.getAsJsonObject(logContens(1))
        val element: JsonElement = json.get("et")
        element.toString
      }
    })
    import session.implicits._
    rdd2.toDF("str").createTempView("table1")
//
//    session.sql(
    //      """
    //        |select et[1]from (
    //        |select get_json_object(str,'$.et')as et ,get_json_object(str,'$.ap') from table1)
    //      """.stripMargin).show()
    session.sql(
      """
        |select get_json_object(str,'$.et')as et ,get_json_object(str,'$.ap') from table1

      """.stripMargin)

    val str: RDD[String] = session.sql(
      """
select get_json_object(str,'$.et') from table1

      """.stripMargin).rdd.map(row => row.getString(0))
    val df1: DataFrame = session.read.json(str)

    session.sql(
      """
select array(get_json_object(str,'$.et'))[1] from table1

      """.stripMargin).printSchema()


  }
}
