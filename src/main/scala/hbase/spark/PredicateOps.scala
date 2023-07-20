package am
package hbase.spark

import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression

trait PredicateOps[Predicate] {
  def isRangePredicate(p: Predicate): Boolean
  def isEqualPredicate(p: Predicate): Boolean
  def isInPredicate(p: Predicate): Boolean
  def isSingleColumnPredicate(p: Predicate): Boolean
}

object PredicateOps {
  implicit object ExpressionOps extends PredicateOps[Expression] {
    override def isRangePredicate(p: Expression): Boolean =
      p match {
        case _: expressions.LessThan => true
        case _: expressions.LessThanOrEqual => true
        case _: expressions.GreaterThan => true
        case _: expressions.GreaterThanOrEqual => true
        case _ => false
      }

    override def isSingleColumnPredicate(p: Expression): Boolean =
      p.references.size == 1

    override def isEqualPredicate(p: Expression): Boolean =
      p.isInstanceOf[expressions.EqualTo]

    override def isInPredicate(p: Expression): Boolean =
      p.isInstanceOf[expressions.In] || p.isInstanceOf[expressions.InSet]
  }
}