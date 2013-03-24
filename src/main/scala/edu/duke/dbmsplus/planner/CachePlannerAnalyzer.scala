package edu.duke.dbmsplus.planner

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.TreeSet
import scala.math._


import edu.duke.dbmsplus.planner.utils._
import edu.duke.dbmsplus.planner.QueryOperation._

/**
* This is an Analyzer Class. It performs analysis on the cache planning algorithm.
* This class does not have to make any connection to the Spark Cluster.
* It calculates statistics of the cache planning algorithm, such as number times it is able to cache, 
* average number of pools using cached dataset, and average number of jobs using cached dataset
*/
class CachePlannerAnalyzer extends Planner{

  //This is the time (in seconds) for the Planner to batch the queries in queues (i.e., it will sleep for batchPeriodicity seconds after processing each batch)
  var batchPeriodicity: Int = 1
  //The number of queries to consider in each queue in each batch, -1 is consider the whole queue
  var maxQueriesPerQueueinBatch: Int = -1
  //The number of jobs using the same dataset, before it is considered cacheable
  var minSharedjobs: Int = 3

  //Start the planner
  //strat - 1 baseline, no cache optimization
  //strat - 2 batch head of queues, pick most commonly used dataset
  def analyze(strat: Int) {
    if(strat == 1) {
    } else if(strat == 2) {
      analyzeCacheSingle()
    } 
  }
  
  //This strategy performs batch processing. Every iteration, it grabs at most X queries in the queues
  //It searches for a possible dataset to cache
  //The algorithm uses a simple heuristic to decide whether to cache a dataset. It finds the dataset that is read by the most number of jobs > minSharedJobs
  //This method will just printout the statistics, while "simulating" the algorithm
  def analyzeCacheSingle() {
    println("Analyzing Planner with single caching, batch periodicity:"+batchPeriodicity+", max queries per queue in batch:"+maxQueriesPerQueueinBatch)
    val poolNameToQueries  = new HashMap[String, ArrayBuffer[Query]]
    var numTimesCached: Int = 0
    var totalJobsUsingCached: Int = 0
    var totalPoolsUsingCached: Int = 0
    var hasEnded: Boolean = false
    while(!hasEnded) {
      //Grab the elements of queues
      pools.synchronized {
        for(key <-poolNameToPool.keys) {
          val pool = poolNameToPool(key)
          if(pool.queryQueue.size > 0) {
                if(maxQueriesPerQueueinBatch < 0 || (maxQueriesPerQueueinBatch > pool.queryQueue.size)) {
              poolNameToQueries(key) = pool.queryQueue.clone()
              pool.queryQueue.clear
            } else {
              poolNameToQueries(key) = pool.queryQueue.take(maxQueriesPerQueueinBatch)
              pool.queryQueue.trimStart(maxQueriesPerQueueinBatch)
            }
          }
        }
        if(poolNameToQueries.size == 0) {
          hasEnded = true
        }
      }

      //search for all possible cacheable datasets
      //Traverse all queries, create frequency map for inputs
      val datasetCount = new HashMap[String, (Int,TreeSet[String])]
      for(key <- poolNameToQueries.keys) {
        for(query <- poolNameToQueries(key)) {
          if(datasetCount.contains(query.input)) {
            val poolNameSet = datasetCount(query.input)._2 + key
            datasetCount(query.input) = (datasetCount(query.input)._1+1,poolNameSet)
          } else {
            val poolNameSet = TreeSet[String](key)
            datasetCount(query.input) = (1, poolNameSet)
          }
        }
      }

      //Filters the frequencyMap to entries that satisfies the minSharedJobs constraint
      //Find the max
      val filteredDatasetCount = datasetCount.filter(_._2._1 >= minSharedjobs)
      var maxDataset: (String,TreeSet[String]) = null
      if(filteredDatasetCount.size > 0)
        maxDataset = (filteredDatasetCount.maxBy(_._2._1)._1,filteredDatasetCount.maxBy(_._2._1)._2._2)

      //There will be a separate thread launched to process each queue (called PoolSubmissionThread)
      //Within each thread, it launches separate threads for each query in the pool
      if(maxDataset != null) {
        numTimesCached = numTimesCached + 1
        totalJobsUsingCached = totalJobsUsingCached + filteredDatasetCount.maxBy(_._2._1)._2._1
        totalPoolsUsingCached = totalPoolsUsingCached + maxDataset._2.size
      }
      poolNameToQueries.clear
    }
    println("Num Times Cached:"+numTimesCached+", Average # Job Per Cached Dataset: "+(totalJobsUsingCached.toDouble/numTimesCached.toDouble)+", Average # Pools Per Cached Dataset: "+(totalPoolsUsingCached.toDouble/numTimesCached.toDouble))
  }





}
