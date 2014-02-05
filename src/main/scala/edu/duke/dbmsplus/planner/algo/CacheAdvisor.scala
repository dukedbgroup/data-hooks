/**
 *
 */
package edu.duke.dbmsplus.planner.algo

import scala.collection.immutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import edu.duke.dbmsplus.queries.Query

/**
 * @author mayuresh
 *
 */
trait CacheAdvisor {
	/**
	 * Return datasets to cache
	 */
	def datasetsToCache(poolNameToQueries: HashMap[String, ArrayBuffer[Query]]):
	(String,TreeSet[String])
}