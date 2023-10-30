package am
package hbase.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


// optionMap: CaseInsensitiveStringMap bring back if needed?
case class HBaseScanBuilder(tableName:String, namespace: String, catalog: String)
  extends ScanBuilder
    with SupportsPushDownFilters
//    with SupportsPushDownRequiredColumns
    with Logging {

  override def build(): Scan = HBaseScan(tableName, namespace, catalog)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = ???
  // handled by Spark: all else

  override def pushedFilters(): Array[Filter] = ???
  // handled by HBase: rowkeyEQ, rowkeyPrefix, rowkey where rowkey < and >, rowkey where rowkey >, rowkey where rowkey <, rowkey where rowkey in => Gets or Scan with Start/Stop rows

//  override def pruneColumns(requiredSchema: StructType): Unit = ???
//  // handle columns that are in the select + cq if clustering column
}
