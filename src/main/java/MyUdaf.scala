import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MyUdaf extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("i1", StringType, true),
      DataTypes.createStructField("i2", LongType, true)
    ))
  }

  override def bufferSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("i1", StringType, true),
      DataTypes.createStructField("i2", LongType, true)
    ))
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = "zhangsan"
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(input.getString(0).contains("zhangsan")) {
      buffer(0) = "zhangsan"
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit ={
    buffer1(0)="zhangsan"
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = buffer(0)+buffer(1).toString
}
