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
  val datasetToPartitionedRDD  = new HashMap[(String,Int), RDD[(Long,Seq[Array[String]])]]

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
  //strat - 2 batch with N items of queues, pick most commonly used dataset to cache (Cache dataset as is)
  //strat - 3 batch with N items of queues, pick most commonly used dataset to cache (Cache dataset as is), reorder queries by pushing queries using cache to the start and only wait for them
  //strat - 4 batch with N items of queues, pick most commonly used dataset to cache (cache a partitioned version of dataset)
  //strat - 5 batch with N items of queues, pick most commonly used dataset to cache (cache a partitioned version of dataset), reorder queries by pushing queries using cache to the start and only wait for them
  def start(strat: Int) {
    if(!hasStarted) {
      hasEnded = false
      hasStarted = true
      threadPool = Executors.newFixedThreadPool(maxThreads)
      
      //preprocess (hacky way to fix the bug where the first job does not get any preferred locations (locations are not registered until after 1 job has been run)
      val data = Array.range(1,200,1)
      val distData = sc.parallelize(data,200)
      distData.reduce(_ + _)

      if(strat == 1) {
        val t = new Thread(startNoCaching())
        t.start
      } else if(strat == 2) {
        val t = new Thread(startCacheSingle())
        t.start
      } else if(strat == 3) {
        val t = new Thread(startCacheSingle(true))
        t.start
      } else if(strat == 4) {
        val t = new Thread(startCachePartitionedSingle())
        t.start
      } else if(strat == 5) {
        val t = new Thread(startCachePartitionedSingle(true))
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
  //boundReorderEnabled, specify whether to push queries that use cached dataet into the start of the queue and then proceed with the next batch after these queries have finished
  def startCacheSingle(boundedReorderEnabled: Boolean = false) {
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
        var delim = ""
        for(poolName <- maxDataset._2) {
           cachePoolName += delim+poolName
           delim = "|"
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
          
          val currentPool = poolNameToQueries(poolName)
          if(boundedReorderEnabled) {
            //We re-arrange the queries, to put the queries that uses the cached dataset to the front of the queue
            val cachedQueries = new ArrayBuffer[Query]
            for(query <- currentPool) {
              if(query.input == maxDataset._1) {
                cachedQueries += query
                currentPool -= query
              }
            }
            currentPool.prependAll(cachedQueries)
          }
          val future = threadPool.submit(new PoolSubmissionThread(currentPool,poolName,boundedReorderEnabled,(maxDataset._1,datasetToRDD(maxDataset._1))))
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
  
  //This strategy performs batch processing. Every batchPeriodicity, it grabs all of the queries in the queues
  //It searches for a possible dataset to cache
  //It then decides how best to store the cached dataset (how to partition them) 
  //If found, it submits a separate thread to cache the partitioned dataset
  //It launches a separate thread (PoolSubmissionThread) for each queue that is responsible for submitting queries to spark
  //Within the PoolSubmissionThread, it launches a separate thread for each query submission
  //The algorithm uses a simple heuristic to decide whether to cache a dataset. It finds the dataset that is read by the most number of jobs > minSharedJobs
  //boundReorderEnabled, specify whether to push queries that use cached dataet into the start of the queue and then proceed with the next batch after these queries have finished
  def startCachePartitionedSingle(boundedReorderEnabled: Boolean = false) {
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
      //The datasetCount is a HashMap with key = dataset name
      // The value is a tuple3 containing _1 = # of occurence of dataset, _2 = Set of Pools, _3 = HashMap of groupCol as Key and # occurence as value
      val datasetCount = new HashMap[String, (Int,TreeSet[String], HashMap[Int,Int])]
      for(key <- poolNameToQueries.keys) {
        for(query <- poolNameToQueries(key)) {
          if(datasetCount.contains(query.input)) {
            val poolNameSet = datasetCount(query.input)._2 + key
            val groupKeyMap = datasetCount(query.input)._3
            if(groupKeyMap.contains(query.groupCol)) {
              groupKeyMap(query.groupCol) = groupKeyMap(query.groupCol) + 1
              datasetCount(query.input) = (datasetCount(query.input)._1+1,poolNameSet,groupKeyMap)
            }
            else {
              groupKeyMap(query.groupCol) = 1
              datasetCount(query.input) = (datasetCount(query.input)._1+1,poolNameSet,groupKeyMap)
            }
          } else {
            val poolNameSet = TreeSet[String](key)
            val groupKeyMap = new HashMap[Int,Int]
            groupKeyMap(query.groupCol) = 1 
            datasetCount(query.input) = (1, poolNameSet, groupKeyMap)
          }
        }
      }
      
      //Filters the frequencyMap to entries that satisfies the minSharedJobs constraint
      val filteredDatasetCount = datasetCount.filter(_._2._1 >= minSharedjobs)
      //We find the dataset that occurs the most
      var maxDataset: (String,TreeSet[String],Int) = null
      if(filteredDatasetCount.size > 0) {
        val chosenDataset = filteredDatasetCount.maxBy(_._2._1)
        //We also pick the partitioning on the grouping column that is used by most queries
        val groupKey = chosenDataset._2._3.maxBy(_._2)._1
        maxDataset = (chosenDataset._1,chosenDataset._2._2, groupKey)
      }
        
      //There will be a separate thread launched to process each queue (called PoolSubmissionThread)
      //Within each thread, it launches separate threads for each query in the pool
      if(maxDataset != null) {
        //We also need to keep track which pool depends on the cachedDataset 
        //Or we can have N running threads, each responsible for processing each pool
        //If the pools are dependent on a cached dataset, then it waits for that to finish
        var cachePoolName = ""
        var delim = ""
        for(poolName <- maxDataset._2) {
           cachePoolName += delim+poolName
           delim = "|"
        }

        val futures = new ArrayBuffer[Future[_]]
        
        //First check if the dataset is already in the cache?
        var alreadyInCache: Boolean = false
        datasetToPartitionedRDD.synchronized {
          if(datasetToPartitionedRDD.contains((maxDataset._1,maxDataset._3)))
            alreadyInCache = true
          else
            datasetToPartitionedRDD.clear //we clear the hashMap (because we will now overwrite this)  
        }
        for(key <- poolNameToQueries.keys) {
          if(!maxDataset._2.contains(key)) {
            val future = threadPool.submit(new PoolSubmissionThread(poolNameToQueries(key),key))
            futures += future
          }
        }
        if(!alreadyInCache) {
          println("Caching "+maxDataset._1+" into memory")
          val cacheFuture = threadPool.submit(new CachePartitionedDataset(maxDataset._1,maxDataset._3,cachePoolName))
          cacheFuture.get
        }
       
        for(poolName <- maxDataset._2) {
          val currentPool = poolNameToQueries(poolName)
          if(boundedReorderEnabled) {
            //We re-arrange the queries, to put the queries that uses the cached dataset to the front of the queue
            val cachedQueries = new ArrayBuffer[Query]
            for(query <- currentPool) {
              if(query.input == maxDataset._1) {
                cachedQueries += query
                currentPool -= query
              }
            }
            currentPool.prependAll(cachedQueries)          
          }
          val future = threadPool.submit(new PoolSubmissionThread(currentPool,poolName,boundedReorderEnabled, null, (maxDataset._1,datasetToPartitionedRDD(maxDataset._1,maxDataset._3),maxDataset._3)))
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
  class PoolSubmissionThread(queries: ArrayBuffer[Query], poolName: String, onlyWaitForCached: Boolean = false, inputRDDPair: (String,RDD[Array[String]]) = null, inputPartitionedRDDThrice: (String,RDD[(Long,Seq[Array[String]])],Int) = null) extends Runnable {
    override def run(): Unit = {
      val futures = new ArrayBuffer[Future[_]]
     
      for(i <- 0 until queries.size) {
        if(inputRDDPair != null && queries(i).input == inputRDDPair._1) {
          val future = threadPool.submit(new QueryToSpark(queries(i), poolName, inputRDDPair._2))
          futures += future
        } else if(inputPartitionedRDDThrice != null && queries(i).input == inputPartitionedRDDThrice._1) {
          val future = threadPool.submit(new QueryToSpark(queries(i), poolName, null, (inputPartitionedRDDThrice._2,inputPartitionedRDDThrice._3)))
          futures += future
        } else {
          val future = threadPool.submit(new QueryToSpark(queries(i), poolName))
          if(!onlyWaitForCached)
            futures += future          
        }
      }
      for(future <- futures) {
        future.get
      }

      queries.clear
    }
  }

   
  //This class is used to submit a query in a separate thread to Spark
  //partitionedRDD is a pair pointing to the RDD itself and the groupCol that the data is partitioned on
  class QueryToSpark(query: Query, poolName: String, rdd: RDD[Array[String]] = null, partitionedRDD: (RDD[(Long,Seq[Array[String]])],Int) = null) extends Runnable {
    override def run(): Unit = {
      SparkEnv.set(sparkEnv)
      sc.initLocalProperties
      sc.addLocalProperties("spark.scheduler.cluster.fair.pool", poolName)
      if(rdd == null && partitionedRDD == null) { //directly read from HDFS
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
            })
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
            })
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
      else if (rdd != null) { //We have a non-partitioned RDD (The RDD is composed of rows of Array of Strings)
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
      else if (partitionedRDD != null){ // partitioned RDD - The RDD is already partitioned by GroupCol, Key is the groupCol, value is the array of strings
        val cachedRDD = partitionedRDD._1
        val cachedGroupCol = partitionedRDD._2
        if(query.operation == Count) {
          sc.runJob(cachedRDD, (iter: Iterator[(Long,Seq[Array[String]])]) => {
            var result = 0L
            while (iter.hasNext) {
              result += iter.next()._2.size              
            }
            result
          }).sum
        } else {
          val separator = query.separator
          val groupCol = query.groupCol
          val aggCol = query.aggCol          
          if(query.operation == Sum || query.operation == Min || query.operation == Max) {
            if(cachedGroupCol == groupCol) {//If group col of query is the same as group col of RDD, no need to do reduce
              //RDD is RDD[(Long,Seq[Array[String]])]
              if(query.operation == Sum) {
                val sumRDD = cachedRDD.map(line => {
                  var sumOutput = 0.0
                  for(value <- line._2) {
                    sumOutput += value(aggCol).toDouble
                  }
                  (line._1,sumOutput)
                })
                sc.runJob(sumRDD, (iter: Iterator[(Long,Double)]) => {})
              } else if(query.operation == Max) {
                val maxRDD = cachedRDD.map(line => {
                  var maxOutput = Double.MinValue
                  for(value <- line._2) {
                    val currVal = value(aggCol).toDouble
                    if(currVal > maxOutput) {
                      maxOutput = currVal 
                    }
                  }
                  (line._1,maxOutput)
                })
                sc.runJob(maxRDD, (iter: Iterator[(Long,Double)]) => {})                                
              } else if(query.operation == Min) {
                val minRDD = cachedRDD.map(line => {
                  var minOutput = Double.MaxValue
                  for(value <- line._2) {
                    val currVal = value(aggCol).toDouble
                    if(currVal < minOutput) {
                      minOutput = currVal 
                    }
                  }
                  (line._1,minOutput)
                })
                sc.runJob(minRDD, (iter: Iterator[(Long,Double)]) => {})                  
              }               
            } else { //Not the same group col so have to do reduce (repartition)
              val mapToKV = cachedRDD.flatMap(line => {
                val returnList = new ArrayBuffer[(Long, Double)]
                for(value <- line._2) {
                  returnList += ((value(groupCol).toLong, value(aggCol).toDouble))
                }
                returnList
              })
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
            }
          } else {
            if(cachedGroupCol == groupCol) {//If group col of query is the same as group col of RDD, no need to do reduce
              //RDD is RDD[(Long,Seq[Array[String]])]
              val mapToKV = cachedRDD.map(line => {
                val stats = new Stats(0)
                for(value <- line._2) {
                  stats.merge(value(aggCol).toDouble)
                }
                (line._1, stats)
              })
              if(query.operation == CountByKey) {
                val countByKey = mapToKV.map(line => { (line._1,line._2.count)})
                sc.runJob(countByKey, (iter: Iterator[(Long,Long)]) => {})            
              } else if(query.operation == Mean) {
                val meanByKey = mapToKV.map(line => { (line._1,line._2.mean)})
                sc.runJob(meanByKey, (iter: Iterator[(Long,Double)]) => {})
              } else if(query.operation == Variance) {
                val varianceByKey = mapToKV.map(line => { (line._1,line._2.variance)})
                sc.runJob(varianceByKey, (iter: Iterator[(Long,Double)]) => {})
              }  
            } else { // not the same partition key
              val mapToKV = cachedRDD.flatMap(line => {
                val returnList = new ArrayBuffer[(Long, Stats)]
                for(value <- line._2) {
                  returnList += ((value(groupCol).toLong, new Stats(value(aggCol).toDouble)))
                }
                returnList
              })
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
  }

  //This class is used to submit a data loading to memory query to spark (cache dataset)
  class CacheDataset(dataset: String, poolName: String, separator: String = "\\|") extends Runnable {
  //Method that Caches a Dataset and sets the RDD into a hashmap
    override def run(): Unit = {
      SparkEnv.set(sparkEnv)
      val cachedDataset = sc.textFile(hdfsMaster+"/"+dataset)
      val colSeparator = separator
      val mapToArray = cachedDataset.map(line => {
              line.split(colSeparator)
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
  
  //This class is used to submit a data loading to memory query to spark (cache dataset)
  //It first partitions the dataset by a grouping key
  class CachePartitionedDataset(dataset: String, groupCol: Int,poolName: String, separator: String = "\\|") extends Runnable {
  //Method that Caches a Dataset and sets the RDD into a hashmap
    override def run(): Unit = {
      SparkEnv.set(sparkEnv)
      val file = sc.textFile(hdfsMaster+"/"+dataset)
      val colSeparator = separator
      val groupColIndex = groupCol
      val mapToKV = file.map(line => {
        val splits = line.split(colSeparator)      
        (splits(groupColIndex).toLong,splits) 
      })
      
      
      val partitionedDataset = mapToKV.groupByKey      

      sc.initLocalProperties
      sc.addLocalProperties("spark.scheduler.cluster.fair.pool", poolName)
      partitionedDataset.persist(spark.storage.StorageLevel.MEMORY_ONLY)
      sc.runJob(partitionedDataset, (iter: Iterator[(Long,Seq[Array[String]])]) => {})
      datasetToPartitionedRDD.synchronized {
          datasetToPartitionedRDD((dataset,groupCol)) = partitionedDataset
      }
    }
  }
}


