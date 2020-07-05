import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp

object Test7 {
  def main(args: Array[String]): Unit = {
    println(System.currentTimeMillis())
    println( new SimpleDateFormat("yyyy-MM-dd")format (new Date()))
    println(s"new Date().getTime = ${new Date().getTime}")
    println(CurrentTimestamp)
  }
}
