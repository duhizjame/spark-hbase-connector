package am
package hbase.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

// catalogConf: CaseInsensitiveStringMap
case class HBaseTable(tableName: String,  namespace: String, catalog: String, optionalSchema: Option[StructType] = None)
  extends Table
  with SupportsRead
  with SupportsWrite {
  override def name(): String = tableName

  val schemaForLongRead: StructType =
    StructType(Seq(StructField("rowkey", BinaryType, nullable = true),
      StructField("cf", BinaryType, nullable = true),
      StructField("cq", BinaryType, nullable = true),
      StructField("value", BinaryType, nullable = true)))

  override def schema(): StructType = optionalSchema
    .getOrElse(schemaForLongRead)

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.STREAMING_WRITE,
    TableCapability.TRUNCATE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val combinedOptions = (options.asScala).asJava
    HBaseScanBuilder(tableName, namespace, catalog)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = ???
}
