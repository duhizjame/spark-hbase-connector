package am

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class ReadFromHBaseTest extends AnyFlatSpec with SparkSuite {

  it should "test generic read from hbase" in {
    val df: DataFrame = spark.read.format("hbase.spark").load()
    df.show()
  }
}
