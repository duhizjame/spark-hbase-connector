package am
package hbase.spark

object HBaseConf {
    /** Set to false to disable server-side caching of blocks for this scan,
     * false by default, since full table scans generate too much BC churn.
     */
    val QUERY_CACHEBLOCKS = "hbase.spark.query.cacheblocks"
    val DEFAULT_QUERY_CACHEBLOCKS = false
    /** The number of rows for caching that will be passed to scan. */
    val QUERY_CACHEDROWS = "hbase.spark.query.cachedrows"
    /** Set the maximum number of values to return for each call to next() in scan. */
    val QUERY_BATCHSIZE = "hbase.spark.query.batchsize"
    /** The number of BulkGets send to HBase. */
    val BULKGET_SIZE = "hbase.spark.bulkget.size"
    val DEFAULT_BULKGET_SIZE = 1000
    /** Set to specify the location of hbase configuration file. */
    val HBASE_CONFIG_LOCATION = "hbase.spark.config.location"
    /** Set to specify whether create or use latest cached HBaseContext */
    val USE_HBASECONTEXT = "hbase.spark.use.hbasecontext"
    val DEFAULT_USE_HBASECONTEXT = false
    /** Pushdown the filter to data source engine to increase the performance of queries. */
    val PUSHDOWN_COLUMN_FILTER = "hbase.spark.pushdown.columnfilter"
    val DEFAULT_PUSHDOWN_COLUMN_FILTER = true
//    /** Class name of the encoder, which encode data types from Spark to HBase bytes. */
//    val QUERY_ENCODER = "hbase.spark.query.encoder"
//    val DEFAULT_QUERY_ENCODER = classOf[NaiveEncoder].getCanonicalName
    /** The timestamp used to filter columns with a specific timestamp. */
    val TIMESTAMP = "hbase.spark.query.timestamp"
    /** The starting timestamp used to filter columns with a specific range of versions. */
    val TIMERANGE_START = "hbase.spark.query.timerange.start"
    /** The ending timestamp used to filter columns with a specific range of versions. */
    val TIMERANGE_END = "hbase.spark.query.timerange.end"
    /** The maximum number of version to return. */
    val MAX_VERSIONS = "hbase.spark.query.maxVersions"
    /** Delayed time to close hbase-spark connection when no reference to this connection, in milliseconds. */
    val DEFAULT_CONNECTION_CLOSE_DELAY: Int = 10 * 60 * 1000

}
