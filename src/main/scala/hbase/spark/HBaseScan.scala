package am
package hbase.spark

import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, SupportsReportPartitioning}
import org.apache.spark.sql.types.StructType

case class HBaseScan() extends Scan with Batch with SupportsReportPartitioning {
  override def readSchema(): StructType = ???

  override def planInputPartitions(): Array[InputPartition] = ???

  override def createReaderFactory(): PartitionReaderFactory = ???

  override def outputPartitioning(): Partitioning = ???
}