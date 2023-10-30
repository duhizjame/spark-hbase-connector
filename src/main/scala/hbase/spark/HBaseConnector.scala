package am
package hbase.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.spark.internal.Logging

import scala.language.reflectiveCalls

object HBaseConnector
  extends Serializable with Logging {

  /** Connection configurator */
//  def connectionFactory: ConnectionFactory = ConnectionFactory

  /** Returns a shared session to HBase and increases the internal open
   * reference counter. It does not release the session automatically,
   * so please remember to close it after use. Closing a shared session
   * decreases the session reference counter. If the reference count drops to zero,
   * the session may be physically closed. */
  def openSession(): Connection = {
    try {
      val threadLocal: ThreadLocal[Connection] = new ThreadLocal[Connection]()
      var conn = threadLocal.get
      if (conn == null || conn.isClosed || conn.isAborted) {
        conn = ConnectionFactory.createConnection
        threadLocal.set(conn)
      }
      conn
    }
    catch {
      case e: Throwable =>
        throw e
    }
  }

  def withTable[T](tableName: String)(code: Table => T): T = {
    closeResourceAfterUse(openSession()) { session =>
      val table = session.getTable(TableName.valueOf(tableName))
      code(table)
    }
  }

  def withSessionDo[T](code: Connection => T): T = {
    closeResourceAfterUse(openSession()) { session =>
      code(session)
    }
  }

  /** Automatically closes resource after use. Handy for closing streams, files, sessions etc.
   * Similar to try-with-resources in Java 7. */
  def closeResourceAfterUse[T, C <: { def close(): Unit }](closeable: C)(code: C => T): T =
    try code(closeable) finally {
      closeable.close()
    }

}
