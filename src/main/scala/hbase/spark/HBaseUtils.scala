package com.vodafone.automotive.commons.wasp.hbase

import am.hbase.spark.HBaseConnector

import java.util
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

object HBaseUtils {

  def using[A <: AutoCloseable, B](a: A)(f: A => B): B = {
    try {
      f(a)
    } finally {
      a.close()
    }
  }

  def composeKey(ccn: String, date: String, tripId: String): String = {
    s"$ccn|$date|$tripId"
  }


  def getLastByIdCf(tableName: String, key: String, cf: String): Map[String, Map[String, Array[Byte]]] = {
    val getLastByIdCfFunc: Table => Map[String, Map[String, Map[String, Array[Byte]]]] = (t: Table) => {
      val get            = new Get(Bytes.toBytes(key)).addFamily(Bytes.toBytes(cf))
      val result: Result = t.get(get)
      Map(Bytes.toString(result.getRow) -> convertMapHbaseToImmutableWithString(result.getNoVersionMap))
    }

    HBaseConnector
      .withTable(tableName)(getLastByIdCfFunc)
      .getOrElse(key, Map.empty[String, Map[String, Array[Byte]]])
  }

  // scalastyle:off
  def getLastClosedTripKeys(
                             tableName: String,
                             ccn: String,
                             currentTimestamp: Long,
                             startRow: Array[Byte],
                             endRow: Array[Byte],
                             cf: String,
                             cq: String,
                             limit: Int
                           ): Iterable[String] = {
    val getClosedTripKeys = (t: Table) => {
      val scan =new Scan().withStartRow(startRow).withStopRow(endRow)
        .addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq))
        .setFilter(
          new FilterList(
            FilterList.Operator.MUST_PASS_ALL,
            util.Arrays.asList(new KeyOnlyFilter().asInstanceOf[Filter])
          )
        )
      scan.setCaching(limit)
      val result = using(t.getScanner(scan)) { scanner =>
        scanner
          .iterator()
          .asScala
          .take(limit)
          .map(r => Bytes.toString(r.getRow))
          .toList
      }
      result

    }
    HBaseConnector
      .withTable(tableName)(getClosedTripKeys)
  }
  // scalastyle:off

  /**
   *
   * @param tableName
   * @param currentTimestamp
   * @param cf
   * @param startRow
   * @param endRow
   * @param cq
   * @return
   */
  def getLastByIdCfScan(
                         tableName: String,
                         currentTimestamp: Long,
                         cf: String,
                         startRow: Array[Byte],
                         endRow: Array[Byte],
                         cq: Option[String] = None
                       ): Option[(String, Map[String, Map[String, Array[Byte]]])] = {
    val getLastByIdCfFunc: Table => Option[(String, Map[String, Map[String, Array[Byte]]])] = (t: Table) => {

      /*
      If startrow is lexicographically after stoprow, and you set Scan setReversed(boolean reversed) to true, t
      he results are returned in reverse order.
      Given a table with rows a, b, c, d, e, f, if you specify c as the stoprow and f as the startrow,
      rows f, e, and d are returned.
       */
      val scan =  new Scan().withStartRow(startRow).withStopRow(endRow)
        .addFamily(Bytes.toBytes(cf))
      if (cq.isDefined) {
        scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq.get))
      }
      scan.setCaching(1)
      scan.setReversed(true)
      val resultOpt = using(t.getScanner(scan)) { scanner => scanner.next() }
      if (resultOpt != null) {
        Some(Bytes.toString(resultOpt.getRow) -> convertMapHbaseToImmutableWithString(resultOpt.getNoVersionMap))
      } else {
        None
      }
    }
    HBaseConnector
      .withTable(tableName)(getLastByIdCfFunc)
  }

  /** Partial Scan data from Hbase from startkey to endKey
   *
   * @param tableName     the name of the table
   * @param startKey      the start key of range to scan
   * @param endKey        the end key of range to scan
   * @param filterList    the filters to be eventually applied
   * @param cf            The eventual cf to filter on
   * @param cq            The eventual cq to filter on
   * @param reverseResult If true, the results will be returned reversed. The start-end key will be inverted.
   * @return a Seq [Map[ColumnnFamily, Map[ColumnQualifier, Data ] ] ] with the relevant row
   */
  def scanByPartialId(
                       tableName: String,
                       startKey: String,
                       endKey: String,
                       filterList: Option[FilterList],
                       cf: Option[String],
                       cq: List[String],
                       reverseResult: Boolean,
                       limitOpt: Option[Int] = None
                     ): Seq[(String, Map[String, Map[String, Array[Byte]]])] = {

    val scanByPartialIdFunc: Table => Map[String, Map[String, Map[String, Array[Byte]]]] = (t: Table) => {

      val scan = {
        // Automatically manages the inversed scan range
        val newStartKey = if (reverseResult) endKey else startKey

        val newEndKey = (if (reverseResult) startKey else endKey) + Char.MaxValue

        filterList match {
          case None         => new Scan().withStartRow(newStartKey.getBytes()).withStopRow(newEndKey.getBytes())
          case Some(filter) => new Scan().withStartRow(newStartKey.getBytes()).withStopRow(newEndKey.getBytes()).setFilter(filter)
        }
      }
      if (cf.isDefined)
        scan.addFamily(Bytes.toBytes(cf.get))
      cq.foreach(q => scan.addColumn(Bytes.toBytes(cf.get), Bytes.toBytes(q)))
      limitOpt.foreach(limit => scan.setCaching(limit))
      if (reverseResult)
        scan.setReversed(true)
      val result = using(t.getScanner(scan)) { scanner =>
        limitOpt
          .map(limit => scanner.asScala.take(limit))
          .getOrElse(scanner.asScala.toList)
          .map(r => Bytes.toString(r.getRow) -> convertMapHbaseToImmutableWithString(r.getNoVersionMap))
          .toMap
      }
      result
    }

    HBaseConnector
      .withTable(tableName)(scanByPartialIdFunc).toSeq
  }

  /** Partial Scan data from Hbase from startkey to endKey
   *
   * @param tableName     the name of the table
   * @param startKey      the start key of range to scan
   * @param endKey        the end key of range to scan
   * @param filterList    the filters to be eventually applied
   * @param optCf         The eventual cf to filter on
   * @param cqs           The eventual cq to filter on
   * @param reverseResult If true, the results will be returned reversed. The start-end key will be inverted.
   * @return a Seq [Map[ColumnnFamily, Map[ColumnQualifier, Data ] ] ] with the relevant row
   */
  def onlyKeyScanByPartialId(
                              tableName: String,
                              startKey: String,
                              endKey: String,
                              filterList: Option[FilterList],
                              optCf: Option[String],
                              cqs: List[String],
                              reverseResult: Boolean,
                              limit: Option[Int] = None
                            ): Set[String] = {

    val scanByPartialIdFunc: Table => Set[String] = (t: Table) => {

      val scan = {

        // Automatically manages the inversed scan range
        val newStartKey = if (reverseResult) endKey else startKey

        val newEndKey = (if (reverseResult) startKey else endKey) + Char.MaxValue

        filterList match {
          case None         => new Scan().withStartRow(newStartKey.getBytes()).withStopRow(newEndKey.getBytes())
          case Some(filter) => new Scan().withStartRow(newStartKey.getBytes()).withStopRow(newEndKey.getBytes()).setFilter(filter)
        }
      }
      optCf.foreach { cf =>
        val cfBytes = Bytes.toBytes(cf)
        cqs.foreach(cq => scan.addColumn(cfBytes, Bytes.toBytes(cq)))
      }

      limit.foreach(lim => scan.setCaching(lim))

      if (reverseResult)
        scan.setReversed(true)

      val res = using(t.getScanner(scan)) { scanner =>
        val resultsIt = limit.map(l => scanner.asScala.take(l)).getOrElse(scanner.asScala.toList)
        resultsIt.map(r => Bytes.toString(r.getRow)).toSet
      }
      res

    }
    HBaseConnector
      .withTable(tableName)(scanByPartialIdFunc)
  }

  def getRowById(tableName: String, key: String): Map[String, Map[String, Array[Byte]]] = {
    val getRowByIdFunc: Table => Map[String, Map[String, Map[String, Array[Byte]]]] = (t: Table) => {
      val g         = new Get(Bytes.toBytes(key))
      val r: Result = t.get(g)
      Map(Bytes.toString(r.getRow) -> convertMapHbaseToImmutableWithString(r.getNoVersionMap))
    }

    // Using executeQuery to handle connection lifecycle
    HBaseConnector
      .withTable(tableName)(getRowByIdFunc)
      .getOrElse(key, Map.empty[String, Map[String, Array[Byte]]])
  }

  def scanTableOnlyKeys(tableName: String, cf: String): Iterable[String] = {
    val scanAllFunc: Table => Iterable[String] = (t: Table) => {
      val scan = new Scan().addFamily(Bytes.toBytes(cf)).setFilter(new KeyOnlyFilter())
      val out  = using(t.getScanner(scan)) { scanner => scanner.asScala.toList.map(r => Bytes.toString(r.getRow)) }
      out
    }
    // Using executeQuery to handle connection lifecycle
    HBaseConnector
      .withTable(tableName)(scanAllFunc)

  }

  def getRowsByIds(tableName: String, keys: Seq[String], families: Seq[String])
  : Map[String, Map[String, Map[String, Array[Byte]]]] = {
    val familiesBytes = families.map(Bytes.toBytes)
    val getRowsByIdsFunc: Table => Map[String, Map[String, Map[String, Array[Byte]]]] = (t: Table) => {
      val gets = keys.map { k =>
        familiesBytes.foldLeft(new Get(Bytes.toBytes(k))) { (g, family) => g.addFamily(family) }
      }.asJava
      val r: Array[Result] = t.get(gets)
      r.map { res => Bytes.toString(res.getRow) -> convertMapHbaseToImmutableWithString(res.getNoVersionMap) }.toMap
    }
    val results = HBaseConnector
      .withTable(tableName)(getRowsByIdsFunc)

    keys
      .map(k => k -> results.getOrElse(k, Map.empty[String, Map[String, Array[Byte]]]))
      .toMap
  }

  /**
   *
   *
   * @param tableName the table to be queried
   * @param keys the keys to be retrieved
   * @param families the families to be retrieved
   * @param hbaseConfig the configuration that must be used to connect to hbase
   * @return a list ordered with the exact same order of th input keys Seq of collected results in form of
   *         {{{
   *         (RowKey, Map[ColumnFamily, Map[ColumnQualifier, Value]])
   *         }}}
   */
  def multiGet(tableName: String, keys: Seq[String], families: Seq[String])
  : Seq[(String, Map[String, Map[String, Array[Byte]]])] = {
    val familiesBytes = families.map(Bytes.toBytes)
    val getRowsByIdsFunc: Table => Array[(String, Map[String, Map[String, Array[Byte]]])] = (t: Table) => {
      val gets = keys.map { k =>
        familiesBytes.foldLeft(new Get(Bytes.toBytes(k))) { (g, family) => g.addFamily(family) }
      }.asJava
      val r: Array[Result] = t.get(gets)
      r.zip(keys).map {
        case (res, key) =>
          key -> convertMapHbaseToImmutableWithString(res.getNoVersionMap)
      }
    }
    HBaseConnector
      .withTable(tableName)(getRowsByIdsFunc).toSeq
  }

  def getPartialByIdCfCq(tableName: String, key: String, columns: Seq[(String, String)])
  : Map[String, Map[String, Array[Byte]]] = {
    val getPartialRowByIdCfCqFunc: Table => Map[String, Map[String, Map[String, Array[Byte]]]] = (t: Table) => {
      val g = new Get(Bytes.toBytes(key))
      val r = t.get(g)
      val columnsSelection = columns
        .groupBy(_._1)
        .map { x => x._1 -> x._2.map(_._2) }
        .map {
          case (cf, seqCq) =>
            cf -> seqCq
              .map { cq => cq -> r.getValue(Bytes.toBytes(cf), Bytes.toBytes(cq)) }
              .filter(_._2 != null)
              .toMap
        }
      Map(Bytes.toString(r.getRow) -> columnsSelection)
    }
    // Using executeQuery to handle connection lifecycle
    HBaseConnector
      .withTable(tableName)(getPartialRowByIdCfCqFunc)
      .getOrElse(key, Map.empty[String, Map[String, Array[Byte]]])
  }

  def convertMapHbaseToImmutableWithString(
                                            input: util.NavigableMap[Array[Byte], util.NavigableMap[Array[Byte], Array[Byte]]]
                                          ): Map[String, Map[String, Array[Byte]]] = {
    if (input == null) {
      Map.empty[String, Map[String, Array[Byte]]]
    } else {
      input.asScala.map {
        case (cf, columnMap) =>
          Bytes.toString(cf) -> SortedMap(columnMap.asScala.map(x => Bytes.toString(x._1) -> x._2).toSeq: _*)
      }.toMap
    }
  }

  def withScan[A](result: Result)(f: Map[String, Map[String, Array[Byte]]] => A): A = {
    f(convertMapHbaseToImmutableWithString(result.getNoVersionMap))
  }

}
