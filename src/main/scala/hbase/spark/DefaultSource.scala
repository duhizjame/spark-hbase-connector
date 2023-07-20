package hbase.spark

import am.hbase.spark.{HBaseConnector, HBaseTableDef}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
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
import scala.collection.JavaConverters._

/*
  * Default source should some kind of relation provider
  */
class DefaultSource extends TableProvider {

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null,Array.empty[Transform],caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table =
    new SimpleHBaseTable()
}

object DefaultSource {
  val HBaseDataSourcePushdownEnableProperty = "pushdown"

}


/*
  Defines Read Support and Initial Schema
 */

//case
class SimpleHBaseTable //(session: SparkSession, catalogConf: CaseInsensitiveStringMap, catalogName: String, optionalSchema: Option[StructType] = None)
 extends Table with SupportsRead {

  def getTableInfoFromHBase(namespace: String, tableName: String, connector: HBaseConnector): HBaseTableDef = {
    connector.withSessionDo(conn => {
      val table = conn.getTable(TableName.valueOf(namespace, tableName))
      table.getDescriptor
      HBaseTableDef(
        namespace,
        tableName,
        "rowkey", // need only name right now
        table.getDescriptor.getColumnFamilies.map(cf => cf.getNameAsString).toList, // need only name right now
      )
    }
    )
  }

  lazy val conf = HBaseConfiguration.create().asInstanceOf[HBaseConfiguration]
  lazy val tableName = "devicesync"
  lazy val namespace = "dev"
  lazy val connector = new HBaseConnector(conf)
  // retrieve table info from HBase? possible?
  lazy val tableDef: HBaseTableDef = getTableInfoFromHBase(namespace, tableName, connector)

  // try to partition by region if region is 128MB or split differently?
  override def partitioning(): Array[Transform] = super.partitioning()

  override def name(): String = this.getClass.toString

  override def schema(): StructType = StructType(Array(StructField("value", StringType)))

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new HBaseScanBuilder(tableDef)
}



/*
   Scan object with no mixins
 */
case class HBaseScanBuilder(tableDef: HBaseTableDef) extends ScanBuilder
with SupportsPushDownFilters
{

  override def pushFilters(filters: Array[Filter]): Array[Filter] = ???

  override def pushedFilters(): Array[Filter] = ???

  override def build(): Scan = new HBaseScan
}

class HBaseScan extends Scan with Batch {
  override def readSchema(): StructType =  StructType(Array(StructField("value", StringType)))

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new HBasePartition())
  }
  override def createReaderFactory(): PartitionReaderFactory = new HBasePartitionReaderFactory()
}

// simple class to organise the partition
class HBasePartition extends InputPartition

// reader factory
class HBasePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new HBasePartitionReader
}


// parathion reader
class HBasePartitionReader extends PartitionReader[InternalRow] {


  val connection: Connection = ConnectionFactory.createConnection()
  val tableName = TableName.valueOf("dev:devicesync")
  val table = connection.getTable(tableName)
  val scan = new client.Scan()
  val scanner = table.getScanner(scan)
  val rowIterator = scanner.asScala.toIterator

  protected var lastRow: InternalRow = InternalRow()

//  override def next(): Boolean = {
//    if (rowIterator.hasNext) {
//      lastRow = rowIterator.next()
//      true
//    } else {
//      false
//    }
//  }

//  override def get(): InternalRow = lastRow
3
  override def close(): Unit = {
    scanner.close()
  }


  def next: Boolean = rowIterator.hasNext

  def get: InternalRow = {
    val stringUtf = Bytes.toString(rowIterator.next().getRow)
    val row = InternalRow(stringUtf)
    row
  }
//
//  def close(): Unit = Unit

}
