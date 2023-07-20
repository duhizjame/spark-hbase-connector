package am

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.scalatest.flatspec.AnyFlatSpec

class HBaseClientTesting extends AnyFlatSpec {
  it should "test client capabilities" in {
    val connection = ConnectionFactory.createConnection()
    val tableName = TableName.valueOf("dev:devicesync")
    val descriptor = connection.getTable(tableName).getDescriptor
    val cfs = descriptor.getColumnFamilies
    val scan = new Scan()
    val d = descriptor.getNormalizerTargetRegionCount
  }
}
