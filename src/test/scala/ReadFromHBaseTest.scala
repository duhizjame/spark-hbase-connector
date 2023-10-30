package am

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec

class ReadFromHBaseTest extends AnyFlatSpec with SparkSuite {

  it should "test generic read from hbase" in {
    val df: DataFrame = spark.read.format("am.hbase.spark").option("table", "sample_table")
    .option("namespace", "dev").option("catalog", """{"myCatalog"}""").load()
    df.select(col("rowkey").cast("string"),
            col("cf").cast("string"),
      col("cq").cast("string"),
      col("value").cast("string")).show()
  }
}
