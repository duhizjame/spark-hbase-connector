package am
package hbase.spark

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BinaryType

class RowReader {

  def buildRow(fields: Seq[Field], result: Result): Row = ???

//  {
//    val r = result.getRow
//    val keySeq = parseRowKey(r, catalog.getRowKey)
//    //TODO denormalized fields not reconstructed. We should
//    val valueSeq = fields
//      .filter(f => !f.isRowKey)
//      .map { x =>
//        val kv = result.getColumnLatestCell(Bytes.toBytes(x.cf), Bytes.toBytes(x.col))
//        if (kv == null || kv.getValueLength == 0) {
//          (x, null)
//        } else {
//          val v = CellUtil.cloneValue(kv)
//          (x, x.dt match {
//            // Here, to avoid arraycopy, return v directly instead of calling hbaseFieldToScalaType
//            case BinaryType => v
//            case _ => Utils.hbaseFieldToScalaType(x, v, 0, v.length)
//          })
//        }
//      }.toMap
//    val unionedRow = keySeq ++ valueSeq
//    // Return the row ordered by the requested order
//    Row.fromSeq(fields.map(unionedRow.get(_).getOrElse(null)))
//  }

}
