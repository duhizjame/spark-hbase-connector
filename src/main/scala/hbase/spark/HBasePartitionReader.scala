package am
package hbase.spark

import com.vodafone.automotive.commons.wasp.hbase.HBaseUtils
import org.apache.hadoop.hbase.client.{Connection, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, client}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

import java.util
import scala.collection.JavaConverters.asScalaIteratorConverter

class HBasePartitionReaderFactory(tableDef: HBaseTableDef) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new HBasePartitionReader(partition.asInstanceOf[HBasePartition], tableDef)
}

class HBasePartitionReader(partition: HBasePartition, tableDef: HBaseTableDef)
  extends PartitionReader[InternalRow] {

  val connection: Connection = HBaseConnector.openSession()

  protected val table: Table = connection.getTable(TableName.valueOf(tableDef.namespace, tableDef.tableName))
  val scan: Scan = new client.Scan().withStartRow(partition.startKey).withStopRow(partition.endKey)
  protected val scanner: ResultScanner = table.getScanner(scan)
  protected val rowIterator: Iterator[InternalRow] = getIterator

  case class HBaseRecords(records: List[HBaseRecord])
  case class HBaseRecord(key: Array[Byte], cf: Array[Byte], cq: Array[Byte], value: Array[Byte])
  object HBaseRecords {
    def apply(key: Array[Byte], cells:  util.NavigableMap[Array[Byte], util.NavigableMap[Array[Byte], Array[Byte]]]): HBaseRecords = {
      val records = cells.keySet().iterator().asScala.flatMap { cf =>
        val cqs = cells.get(cf)
        cqs.keySet().iterator().asScala.map{ cq =>
          HBaseRecord(key, cf, cq, cqs.get(cq))
        }
      }
      HBaseRecords(records.toList)
    }
  }

  def getIterator: Iterator[InternalRow] = {
    val res = scanner.iterator().asScala.toList
    val rows = res.map(r => HBaseRecords(r.getRow, r.getNoVersionMap))
    rows.flatMap(hr => hr.records.map( r => InternalRow(r.key, r.cf, r.cq, r.value))).iterator
  }

  override def next(): Boolean = rowIterator.hasNext

  override def get(): InternalRow = rowIterator.next()

  override def close(): Unit = scanner.close()
}
