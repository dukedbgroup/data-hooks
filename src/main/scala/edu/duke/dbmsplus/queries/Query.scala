/**
 *
 */
package edu.duke.dbmsplus.queries

/**
 * @author mayuresh
 *
 */
class Query extends Serializable {
  /**
   * datasets to process
   * We support either single table aggregation and a two table join only
   * dataset1 is necessary
   * dataset2 is optional
   */
  var dataset1: Dataset
  var dataset2: Dataset
  
  /**
   * projections
   * At least one is necessary
   */
  var projections: List[Aggregation]
  
  /**
   * selections
   * optional
   * Will be used to recommend partitioning by learning historical patterns
   */
  var selections: List[Selection]
  
  /**
   * Grouping columns
   * optional
   */
  var grouping: List[String]
  
  /**
   * Degree of parallelism
   */
  var parallelism: Int
}

class Dataset extends Serializable {
  /**
   * name of dataset
   */
  var name: String
  /**
   * size of dataset
   */
  var size: Long
}

class Aggregation extends Serializable {
  /**
   * type of aggregation
   * optional, no aggregation if not provided
   */
  var operation: QueryOperation.Value
  /**
   * Name of the column
   * optional, all columns if not provided
   */
  var column: String
}

/**
 * An enumeration of the type of operations the CachePlanner currently supports
 */
object QueryOperation extends Enumeration with Serializable
{
  type QueryOperation = Value
  val Count, CountByKey = Value
}

class Selection extends Serializable {
  /**
   * Name of the column
   */
  var column: String
  /**
   * Value
   */
  var value: Any
  /**
   * Condition
   */
  var condition: SelectionCondition.Value
}


/**
 * An enumeration of the selection conditions
 */
object SelectionCondition extends Enumeration with Serializable
{
  type SelectionCondition = Value
  val Greater, Lesser, Equal = Value
}
