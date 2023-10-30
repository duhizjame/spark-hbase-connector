package am
package hbase.spark

import am.hbase.spark.DefaultSource.{HBaseCatalogProperty, HBaseNamespaceProperty, HBaseTableNameProperty}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, ResultScanner}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownFilters}
import org.apache.spark.sql.types._
import org.apache.hadoop.hbase.client
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import java.util
/*
  * Default source should some kind of relation provider
  */
class DefaultSource extends TableProvider{

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null,Array.empty[Transform],caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table = {
    val catalog = map.get(HBaseCatalogProperty)
    val tableName = map.get(HBaseTableNameProperty)
    val namespace = map.get(HBaseNamespaceProperty)

    HBaseTable(tableName, namespace, catalog)
  }
}

object DefaultSource {
  val HBaseDataSourcePushdownEnableProperty = "pushdown"
  val HBaseCatalogProperty = "catalog"
  val HBaseNamespaceProperty = "namespace"
  val HBaseTableNameProperty = "table"
}