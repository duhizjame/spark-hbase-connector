package am
package hbase.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.sources.Filter


case class AnalyzedPredicates(
                               handledByHBase: Set[Filter],
                               handledBySpark: Set[Filter] ){
  override def toString(): String = {
    s"""HBase Filters: [${handledByHBase.mkString(", ")}]
       |Spark Filters [${handledBySpark.mkString(", ")}]""".stripMargin
  }
}

trait HBasePredicateRules{
  def apply(predicates: AnalyzedPredicates, conf: SparkConf): AnalyzedPredicates
}
