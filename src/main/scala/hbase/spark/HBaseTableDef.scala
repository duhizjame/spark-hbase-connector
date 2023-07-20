package am
package hbase.spark

case class HBaseTableDef(
           namespace: String,
           tableName: String,
           rowKey: String, // need only name right now
           columnFamilies: List[String], // need only name right now
           ifNotExists: Boolean = false,
           tableOptions: Map[String, String] = Map())

// cfs(0).getNameAsString
// cfs(0).getBloomFilterType