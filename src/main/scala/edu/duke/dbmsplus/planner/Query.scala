package edu.duke.dbmsplus.planner

/**
 * A representation of a Query
 */
class Query(val input: String, val operation: QueryOperation.Value, val groupCol: Int, val aggCol: Int, val separator: String, val parallelism: Int)
{
  //Supported operation
  //count (global)
  //sum
  //max
  //min
  //countbykey
  //mean
  //variance
}

/**
 * An enumeration of the type of operations the CachePlanner currently supports
 */
object QueryOperation extends Enumeration
{
  type QueryOperation = Value
  val Count, CountByKey, Sum, Max, Min, Mean, Variance = Value
}
