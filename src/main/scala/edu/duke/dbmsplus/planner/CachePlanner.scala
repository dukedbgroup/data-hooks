package edu.duke.dbmsplus.planner

import java.util.concurrent.{Executors, ExecutorService, Future}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.TreeSet
import scala.math._

import spark._
import SparkContext._

import edu.duke.dbmsplus.planner.utils._
import edu.duke.dbmsplus.planner.QueryOperation._

/**
 * This is the Query Cache Planner class. The driver interacts with this class
 * It maintains "external" queues that users submit queries into. 
 * This class is responsible for looking at the queues and submitting queries to Spark's corresponding internal queues
 * We currently support 2 strategies: 
 *    1. The baseline strategy simply grabs the queries in the queue and submits to Spark
 *    2. The single cache strategy batches the submission process. It periodically grabs at most X number of queries in each queue then decide which dataset to cache
 *       It submits the dataset caching query and rewrites the queries that read from this dataset to use the cached dataset instead. 
 */
class CachePlanner(programName: String, sparkMaster: String, hdfsMaster: String) extends Planner{
  //SparkContext
  val sc = new SparkContext(sparkMaster, programName, "/usr/local/spark-0.6.1", List("/root/sparkqueriesplanner_2.9.2-1.0.jar"))
  var sparkEnv: SparkEnv = SparkEnv.get
  
  //Cached RDDs
  val datasetToRDD  = new HashMap[String, RDD[Array[String]]]

  var hasEnded = false
  var hasStarted = false

  //This is the time (in seconds) for the Planner to batch the queries in queues (i.e., it will sleep for batchPeriodicity seconds after processing each batch)
  var batchPeriodicity: Int = 1
  //The number of queries to consider in each queue in each batch, -1 is consider the whole queue
  var maxQueriesPerQueueinBatch: Int = -1
  //The number of jobs using the same dataset, before it is considered cacheable
  var minSharedjobs: Int = 3

  //Max Thread in pool
  var maxThreads: Int = 32
  var threadPool: ExecutorService = null
  
  //A toggle where to simply exit the thread when we have exhausted all of the queries in the queue 
  var exitWhenEmpty: Boolean = true

  implicit def whateverToRunnable[F](f: => F) = new Runnable() { def run() { f } }

  //Start the planner
  //strat - 1 baseline, no cache optimization
  //strat - 2 batch head of queues, pick most commonly used dataset
  def start(strat: Int) {
    if(!hasStarted) {
      hasEnded = false
      hasStarted = true
      threadPool = Executors.newFixedThreadPool(maxThreads)
      
      //preprocess (hacky way to fix the bug where the first job does not get any preferred locations (locations are not registered until after 1 job has been run)
      val data = Array.range(1,100,1)
      val distData = sc.parallelize(data,100)
      distData.reduce(_ + _)

      if(strat == 1) {
        val t = new Thread(startNoCaching())
        t.start
      } else if(strat == 2) {
        val t = new Thread(startCacheSingle())
        t.start
      } else { //Strategy not found
        hasStarted = false
        hasEnded = true
      }
    }
  }

  //Stop the planner
  def stop() {
    hasEnded = true
    if(threadPool != null)
      threadPool.shutdown
  }

  //This planning strategy simply submits queries in the queue to spark
  //It uses a separate thread for each query submission
  def startNoCaching() {
    println("Starting Planner with no caching (Baseline case)")
    val poolNameToQuery  = new HashMap[String, Query]

    while(!hasEnded) {
      //Grab the head of queues
      pools.synchronized {
        for(key <-poolNameToPool.keys) {
          val pool = poolNameToPool(key)
          if(pool.queryQueue.size > 0) {
            val query = pool.queryQueue.remove(0)
            poolNameToQuery(key) = query
          }           
         }
         if(poolNameToQuery.size == 0) {
           if(!exitWhenEmpty) {
             println("No queries so waiting")
             pools.wait 
           } else {
             println("No more queries in queues, so exiting Planner thread")
             hasEnded = true
             threadPool.shutdown
           }
         }
      }
      //We submit query in a separate thread
      //Should we use threadpool instead?
      for(key <- poolNameToQuery.keys) {
        val query = poolNameToQuery(key)
        threadPool.execute(new QueryToSpark(query, key))
      }  
      poolNameToQuery.clear
    }
    hasStarted = false
  }

  //This strategy performs batch processing. Every batchPeriodicity, it grabs all of the queries in the queues
  //It searches for a possible dataset to cache
  //If found, it submits a separate thread to cache the dataset
  //It launches a separate thread (PoolSubmissionThread) for each queue that is responsible for submitting queries to spark
  //Within the PoolSubmissionThread, it launches a separate thread for each query submission
  //The algorithm uses a simple heuristic to decide whether to cache a dataset. It finds the dataset that is read by the most number of jobs > minSharedJobs
  def startCacheSingle() {
    println("Starting Planner with single caching, batch periodicity:"+batchPeriodicity+", max queries per queue in batch:"+maxQueriesPerQueueinBatch)
    val poolNameToQueries  = new HashMap[String, ArrayBuffer[Query]]
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
        if(poolNameToQueries.size == 0 && exitWhenEmpty) {
          println("No more queries in queues, so exiting Planner thread")
          hasEnded = true
          threadPool.shutdown
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
        //We also need to keep track which pool depends on the cachedDataset 
        //Or we can have N running threads, each responsible for processing each pool
        //If the pools are dependent on a cached dataset, then it waits for that to finish
        var cachePoolName = ""
        for(poolName <- maxDataset._2) {
           cachePoolName += poolName
        }

        val futures = new ArrayBuffer[Future[_]]
        
        //First check if the dataset is already in the cache?
        var alreadyInCache: Boolean = false
        datasetToRDD.synchronized {
          if(datasetToRDD.contains(maxDataset._1))
            alreadyInCache = true
          else
            datasetToRDD.clear //we clear the hashMap (because we will now overwrite this)  
        }
        for(key <- poolNameToQueries.keys) {
          if(!maxDataset._2.contains(key)) {
            val future = threadPool.submit(new PoolSubmissionThread(poolNameToQueries(key),key))
            futures += future
          }
        }
        if(!alreadyInCache) {
          println("Caching "+maxDataset._1+" into memory")
          val cacheFuture = threadPool.submit(new CacheDataset(maxDataset._1, cachePoolName))
          cacheFuture.get
        }
        for(poolName <- maxDataset._2) {
          val future = threadPool.submit(new PoolSubmissionThread(poolNameToQueries(poolName),poolName,(maxDataset._1,datasetToRDD(maxDataset._1))))
          futures += future
        }
        for(future <- futures) {
          future.get
        }        
      } else { //no common dataset to cache so just submit everything
        if(!poolNameToQueries.isEmpty) {
          val futures = new ArrayBuffer[Future[_]]
          for(key <- poolNameToQueries.keys) {
            val future = threadPool.submit(new PoolSubmissionThread(poolNameToQueries(key),key))
            futures += future
          }
          for(future <- futures) {
            future.get
          }
        }
      }
      poolNameToQueries.clear
      
      println("Sleeping for"+ batchPeriodicity+"s before checking queues again")
      Thread.sleep(batchPeriodicity * 1000)
    }
    hasStarted = false
  }


  //A Thread that goes through a list of queries and submits it sequentially by launching separate submission threads for each query
  class PoolSubmissionThread(queries: ArrayBuffer[Query], poolName: String, inputRDDPair: (String,RDD[Array[String]]) = null) extends Runnable {
    override def run(): Unit = {
/*      SparkEnv.set(sparkEnv)
      sc.initLocalProperties
      sc.addLocalProperties("spark.scheduler.cluster.fair.pool", poolName)
      for(query <- queries) {
        if(inputRDDPair != null && query.input == inputRDDPair._1) {
          queryToSpark(query, poolName, inputRDDPair._2)
        } else {
          queryToSpark(query, poolName)
        }      
      }
*/

      val futures = new ArrayBuffer[Future[_]]
      for(i <- 0 until queries.size) {
        if(inputRDDPair != null && queries(i).input == inputRDDPair._1) {
          val future = threadPool.submit(new QueryToSpark(queries(i), poolName, inputRDDPair._2))
          futures += future
        } else {
          val future = threadPool.submit(new QueryToSpark(queries(i), poolName))
          futures += future          
        }
      }
      for(future <- futures) {
        future.get
      }

      queries.clear
    }
  }

  //A Utility method that submits the query to Spark
  def queryToSpark(query: Query, poolName: String, rdd: RDD[String] = null) {
    if(rdd == null) { //directly read from HDFS
      val file = sc.textFile(hdfsMaster+"/"+query.input)
      if(query.operation == Count) {
        file.count
      } else {
        val separator = query.separator
        val groupCol = query.groupCol
        val aggCol = query.aggCol
        if(query.operation == Sum || query.operation == Min || query.operation == Max) {
          val mapToKV = file.map(line => {
            val splits = line.split(separator)
            (splits(groupCol),splits(aggCol).toDouble) }
          )
          if(query.operation == Sum) {
            val reduceByKey = mapToKV.reduceByKey(_ + _, query.parallelism)
            sc.runJob(reduceByKey, (iter: Iterator[(String,Double)]) => {})
          } else if(query.operation == Max) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {max(a,b)},query.parallelism)
            sc.runJob(reduceByKey, (iter: Iterator[(String,Double)]) => {})
          } else if(query.operation == Min) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {min(a,b)},query.parallelism)
            sc.runJob(reduceByKey, (iter: Iterator[(String,Double)]) => {})
          }
        } else {
          val mapToKV = file.map(line => {
            val splits = line.split(separator)
            (splits(groupCol),new Stats(splits(aggCol).toDouble)) }            
          )
          if(query.operation == CountByKey) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)}, query.parallelism)
            val countByKey = reduceByKey.map(line => { (line._1,line._2.count)})
            sc.runJob(countByKey, (iter: Iterator[(String,Long)]) => {})            
          } else if(query.operation == Mean) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)},query.parallelism)
            val meanByKey = reduceByKey.map(line => { (line._1,line._2.mean)})
            sc.runJob(meanByKey, (iter: Iterator[(String,Double)]) => {})
          } else if(query.operation == Variance) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)},query.parallelism)
            val varianceByKey = reduceByKey.map(line => { (line._1,line._2.variance)})
            sc.runJob(varianceByKey, (iter: Iterator[(String,Double)]) => {})
          }
        }
      }
    }
    else {
      SparkEnv.set(sparkEnv)
      if(query.operation == Count) {
        rdd.count
      }
      else {
        val separator = query.separator
        val groupCol = query.groupCol
        val aggCol = query.aggCol
        val mapToKV = rdd.map(line => {
          val splits = line.split(separator)
          (splits(groupCol),splits(aggCol).toDouble) }
        )
        if(query.operation == Sum || query.operation == Min || query.operation == Max) {
          if(query.operation == Sum) {
            val reduceByKey = mapToKV.reduceByKey(_ + _, query.parallelism)
            sc.runJob(reduceByKey, (iter: Iterator[(String,Double)]) => {})
          } else if(query.operation == Max) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {max(a,b)},query.parallelism)
            sc.runJob(reduceByKey, (iter: Iterator[(String,Double)]) => {})
          } else if(query.operation == Min) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {min(a,b)},query.parallelism)
            sc.runJob(reduceByKey, (iter: Iterator[(String,Double)]) => {})
          }
        } else {
          val mapToKV = rdd.map(line => {
            val splits = line.split(separator)
            (splits(groupCol),new Stats(splits(aggCol).toDouble)) }            
          )
          if(query.operation == CountByKey) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)}, query.parallelism)
            val countByKey = reduceByKey.map(line => { (line._1,line._2.count)})
            sc.runJob(countByKey, (iter: Iterator[(String,Long)]) => {})            
          } else if(query.operation == Mean) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)},query.parallelism)
            val meanByKey = reduceByKey.map(line => { (line._1,line._2.mean)})
            sc.runJob(meanByKey, (iter: Iterator[(String,Double)]) => {})
          } else if(query.operation == Variance) {
            val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)},query.parallelism)
            val varianceByKey = reduceByKey.map(line => { (line._1,line._2.variance)})
            sc.runJob(varianceByKey, (iter: Iterator[(String,Double)]) => {})
          }
        }
      }
    }
  }

  //This class is used to submit a query in a separate thread to Spark
  class QueryToSpark(query: Query, poolName: String, rdd: RDD[Array[String]] = null) extends Runnable {
    override def run(): Unit = {
      SparkEnv.set(sparkEnv)
      sc.initLocalProperties
      sc.addLocalProperties("spark.scheduler.cluster.fair.pool", poolName)
      if(rdd == null) { //directly read from HDFS
        val file = sc.textFile(hdfsMaster+"/"+query.input)
        if(query.operation == Count) {
          file.count
        } else {
          val separator = query.separator
          val groupCol = query.groupCol
          val aggCol = query.aggCol
          if(query.operation == Sum || query.operation == Min || query.operation == Max) {
            val mapToKV = file.map(line => {
              val splits = line.split(separator)
              (splits(groupCol).toLong,splits(aggCol).toDouble) 
            }).filter(input => input._1 < 20)
            if(query.operation == Sum) {
              val reduceByKey = mapToKV.reduceByKey(_ + _, query.parallelism)
              sc.runJob(reduceByKey, (iter: Iterator[(Long,Double)]) => {})            
            } else if(query.operation == Max) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {max(a,b)},query.parallelism)
              sc.runJob(reduceByKey, (iter: Iterator[(Long,Double)]) => {})
            } else if(query.operation == Min) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {min(a,b)},query.parallelism)
              sc.runJob(reduceByKey, (iter: Iterator[(Long,Double)]) => {})
            } 
          } else {
            val mapToKV = file.map(line => {
              val splits = line.split(separator)
              (splits(groupCol).toLong,new Stats(splits(aggCol).toDouble)) 
            }).filter(input => input._1 < 20)
            if(query.operation == CountByKey) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)}, query.parallelism)
              val countByKey = reduceByKey.map(line => { (line._1,line._2.count)})
              sc.runJob(countByKey, (iter: Iterator[(Long,Long)]) => {})            
            } else if(query.operation == Mean) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)},query.parallelism)
              val meanByKey = reduceByKey.map(line => { (line._1,line._2.mean)})
              sc.runJob(meanByKey, (iter: Iterator[(Long,Double)]) => {})
            } else if(query.operation == Variance) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)},query.parallelism)
              val varianceByKey = reduceByKey.map(line => { (line._1,line._2.variance)})
              sc.runJob(varianceByKey, (iter: Iterator[(Long,Double)]) => {})
            }
          }
        }
      }
      else {
        SparkEnv.set(sparkEnv)
        if(query.operation == Count) {
          rdd.count
        } else {
          val separator = query.separator
          val groupCol = query.groupCol
          val aggCol = query.aggCol          
          if(query.operation == Sum || query.operation == Min || query.operation == Max) {
            val mapToKV = rdd.map(line => {
                (line(groupCol).toLong,line(aggCol).toDouble)}
            )
            if(query.operation == Sum) {
              val reduceByKey = mapToKV.reduceByKey(_ + _, query.parallelism)
              sc.runJob(reduceByKey, (iter: Iterator[(Long,Double)]) => {})
            } else if(query.operation == Max) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {max(a,b)},query.parallelism)
              sc.runJob(reduceByKey, (iter: Iterator[(Long,Double)]) => {})
            } else if(query.operation == Min) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {min(a,b)},query.parallelism)
              sc.runJob(reduceByKey, (iter: Iterator[(Long,Double)]) => {})
            }
          } else {
            val mapToKV = rdd.map(line => {
                (line(groupCol).toLong,new Stats(line(aggCol).toDouble))}
            )
            if(query.operation == CountByKey) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)}, query.parallelism)
              val countByKey = reduceByKey.map(line => { (line._1,line._2.count)})
              sc.runJob(countByKey, (iter: Iterator[(Long,Long)]) => {})            
            } else if(query.operation == Mean) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)},query.parallelism)
              val meanByKey = reduceByKey.map(line => { (line._1,line._2.mean)})
              sc.runJob(meanByKey, (iter: Iterator[(Long,Double)]) => {})
            } else if(query.operation == Variance) {
              val reduceByKey = mapToKV.reduceByKey((a,b) => {a.merge(b)},query.parallelism)
              val varianceByKey = reduceByKey.map(line => { (line._1,line._2.variance)})
              sc.runJob(varianceByKey, (iter: Iterator[(Long,Double)]) => {})
            }
          }
        }        
      }
    }
  }

  //This class is used to submit a data loading to memory query to spark (cache dataset)
  class CacheDataset(dataset: String, poolName: String) extends Runnable {
  //Method that Caches a Dataset and sets the RDD into a hashmap
    override def run(): Unit = {
      SparkEnv.set(sparkEnv)
      val cachedDataset = sc.textFile(hdfsMaster+"/"+dataset)
      val mapToArray = cachedDataset.map(line => {
              line.split("\\|")
              }
            )

      sc.initLocalProperties
      sc.addLocalProperties("spark.scheduler.cluster.fair.pool", poolName)
      mapToArray.persist(spark.storage.StorageLevel.MEMORY_ONLY)
      sc.runJob(mapToArray, (iter: Iterator[Array[String]]) => {})
      datasetToRDD.synchronized {
          datasetToRDD(dataset) = mapToArray
      }
    }
  }
}


