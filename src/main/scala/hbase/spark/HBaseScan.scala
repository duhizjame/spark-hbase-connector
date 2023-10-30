package am
package hbase.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, Table}
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, SupportsReportPartitioning}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

// simple class to organise the partition
case class HBasePartition(startKey: Array[Byte], endKey: Array[Byte]) extends InputPartition

case class HBaseScan(tableName:String, namespace: String, catalog: String)
  extends Scan
    with Batch
    with SupportsReportPartitioning {

  val connection: Connection = HBaseConnector.openSession()

  val table: Table = connection.getTable(TableName.valueOf(s"$namespace:$tableName"))
  val columnFamilies: List[String] = table.getDescriptor.getColumnFamilies.map(cf => cf.getNameAsString).toList
  override def readSchema(): StructType = {

    // use catalog to match
    // prune columns?
    StructType(Seq())
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val table = connection.getTable(TableName.valueOf(s"$namespace:$tableName"))
    table.getRegionLocator.getAllRegionLocations.asScala.map(r =>
      HBasePartition(r.getRegion.getStartKey, r.getRegion.getEndKey)
    ).toArray
  }


  override def toBatch: Batch = this

  override def createReaderFactory(): PartitionReaderFactory = new HBasePartitionReaderFactory(HBaseTableDef(namespace, tableName, "rowKey", columnFamilies, ifNotExists = false, Map.empty))

  override def outputPartitioning(): Partitioning = ???
}