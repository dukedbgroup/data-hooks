/**
 *
 */
package edu.duke.dbmsplus.queries

/**
 * @author mayuresh
 *
 */
object QueryUtils {

  /**
   * We currently support only single table aggregation or a two table join 
   * queries.
   */
  def isJoinQuery(query: Query): Boolean = {
    false
  }
  
  /**
   * Sanity checks to see if the query is well-formed
   */
  def isWellFormed(query: Query): Boolean = {
    true
  }
}