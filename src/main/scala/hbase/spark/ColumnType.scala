package am
package hbase.spark

import org.apache.spark.sql.types.DataType

import scala.reflect.runtime.universe._

trait ColumnType[T] extends Serializable {

  /** Returns a converter that converts values to the Scala type associated with this column. */
  //  lazy val converterToScala: TypeConverter[T] =
  //    TypeConverter.forType(scalaTypeTag)
  // TODO: only to bytes?

  //  /** Returns a converter that converts this column to type that can be saved by TableWriter. */
  //  def converterToCassandra: TypeConverter[_ <: AnyRef]

  /** Returns the TypeTag of the Scala type recommended to represent values of this column. */
  def scalaTypeTag: TypeTag[T]

  /** Name of the Scala type. Useful for source generation. */
  def scalaTypeName: String
  = scalaTypeTag.tpe.toString

}
object ColumnType {


  /** Returns natural Cassandra type for representing data of the given Scala type */
  def fromScalaType(
                     dataType: Type): ColumnType[_] = {

//    def unsupportedType() = throw new IllegalArgumentException(s"Unsupported type: $dataType")
    ???
  }

  /** Returns a converter that converts values to the type of this column expected by the
   * Cassandra Java driver when saving the row.*/
//  def converterToCassandra(dataType: DataType)
//  : TypeConverter[_ <: AnyRef] = {
//
}

