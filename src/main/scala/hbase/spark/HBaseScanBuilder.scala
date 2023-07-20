package am
package hbase.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class HBaseScanBuilder(sparkSession: SparkSession, optionMap: CaseInsensitiveStringMap) extends ScanBuilder with SupportsPushDownFilters with Logging{
  override def build(): Scan = ???

  override def pushFilters(filters: Array[Filter]): Array[Filter] = ???
  // handled by Spark: all else

  override def pushedFilters(): Array[Filter] = ???
  // handled by HBase: rowkeyEQ, rowkeyPrefix, rowkey where rowkey < and >, rowkey where rowkey >, rowkey where rowkey <, rowkey where rowkey in => Gets or Scan with Start/Stop rows
}
