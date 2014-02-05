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
class GreedyCacheAdvisor extends CacheAdvisor {
  
  //The number of jobs using the same dataset, before it is considered cacheable
  var minSharedjobs: Int = 3

  /**
   * Currently only considers at single table aggregation queries
   */
  def datasetsToCache(poolNameToQueries: HashMap[String, ArrayBuffer[Query]]):
	(String,TreeSet[String]) = {
    //search for all possible cacheable datasets
    //Traverse all queries, create frequency map for inputs
    val datasetCount = new HashMap[String, (Int,TreeSet[String])]
    for(key <- poolNameToQueries.keys) {
      for(query <- poolNameToQueries(key)) {
        if(datasetCount.contains(query.dataset1)) {
          val poolNameSet = datasetCount(query.dataset1)._2 + key
          datasetCount(query.dataset1) = (datasetCount(query.dataset1)._1+1,poolNameSet)
        } else {
          val poolNameSet = TreeSet[String](key)
          datasetCount(query.dataset1) = (1, poolNameSet)
        }
      }
    }
      
    //Filters the frequencyMap to entries that satisfies the minSharedJobs constraint
    //Find the max
    val filteredDatasetCount = datasetCount.filter(_._2._1 >= minSharedjobs)
    var maxDataset: (String,TreeSet[String]) = null
    if(filteredDatasetCount.size > 0)
      maxDataset = (filteredDatasetCount.maxBy(_._2._1)._1,filteredDatasetCount.maxBy(_._2._1)._2._2)
    return maxDataset  
    
  }
}