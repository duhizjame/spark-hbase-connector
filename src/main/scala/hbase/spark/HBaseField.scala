package am
package hbase.spark


import org.apache.avro.Schema
import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField}

import scala.collection.mutable

trait SerDes {
  def serialize(value: Any): Array[Byte]
  def deserialize(bytes: Array[Byte], start: Int, end: Int): Any
}

case class Field(
                  colName: String,
                  cf: String,
                  col: String,
                  sType: Option[String] = None,
                  avroSchema: Option[String] = None,
                  serdes: Option[SerDes]= None,
                  len: Int = -1) extends Logging {
  override def toString = s"$colName $cf $col"
  val isRowKey: Boolean = cf == "rowkey"
  var start: Int = _
  def schema: Option[Schema] = avroSchema.map { x =>
    logDebug(s"avro: $x")
    val p = new Schema.Parser
    p.parse(x)
  }

  lazy val exeSchema = schema

  // converter from avro to catalyst structure
  lazy val avroToCatalyst: Option[Any => Any] = {
    schema.map(SchemaConverters.createConverterToSQL)
  }

  // converter from catalyst to avro
  lazy val catalystToAvro: (Any) => Any ={
    SchemaConverters.createConverterToAvro(dt, colName, "recordNamespace")
  }

  def cfBytes: Array[Byte] = {
    if (isRowKey) {
      Bytes.toBytes("")
    } else {
      Bytes.toBytes(cf)
    }
  }
  def colBytes: Array[Byte] = {
    if (isRowKey) {
      Bytes.toBytes("key")
    } else {
      Bytes.toBytes(col)
    }
  }

  val dt: DataType = {
    sType.map(CatalystSqlParser.parseDataType).getOrElse {
      schema.map { x =>
        SchemaConverters.toSqlType(x).dataType
      }.getOrElse(throw new RuntimeException(s"Cannot find dataType for $colName"))
    }
  }

  var length: Int = {
    if (len == -1) {
      dt match {
        case BinaryType | StringType => -1
        case BooleanType => Bytes.SIZEOF_BOOLEAN
        case ByteType => 1
        case DoubleType => Bytes.SIZEOF_DOUBLE
        case FloatType => Bytes.SIZEOF_FLOAT
        case IntegerType => Bytes.SIZEOF_INT
        case LongType => Bytes.SIZEOF_LONG
        case ShortType => Bytes.SIZEOF_SHORT
        case _ => -1
      }
    } else {
      len
    }

  }

  override def equals(other: Any): Boolean = other match {
    case that: Field =>
      colName == that.colName && cf == that.cf && col == that.col
    case _ => false
  }
}

// The row key definition, with each key refer to the col defined in Field, e.g.,
// key1:key2:key3

case class RowKey(k: String) {
  val keys = k.split(":")
  var fields: Seq[Field] = _
  var varLength = false

  def length: Int = {
    if (varLength) {
      -1
    } else {
      fields.foldLeft(0) { case (x, y) =>
        x + y.length
      }
    }
  }
}


// The map between the column presented to Spark and the HBase field
case class SchemaMap(map: mutable.HashMap[String, Field]) {
  def toFields: Seq[StructField] = map.map { case (name, field) =>
    StructField(name, field.dt)
  }.toSeq

  def fields = map.values

  def getField(name: String): Field = map(name)
  def exists(name: String) = map.keySet.contains(name)
}
