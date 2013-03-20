package edu.duke.dbmsplus

import spark._
import SparkContext._

import java.io._

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform
import scala.util.Random

import edu.duke.dbmsplus.planner._
import edu.duke.dbmsplus.planner.QueryOperation._

/**
 * This is the main driver class (main program)
 * It parses the user input arguments
 * It starts the CachePlanner and can run workloads on it
 */
object SparkQueriesPlannerDriver {
  def main(args: Array[String]) {
    //SparkQueriesPlannerDriver <spark address> <hdfs address> 
    //parse the args
    var i = 0
    var sparkAddress: String = ""
    var hdfsAddress: String = ""
    var cacheOption: String = "nocache"
    var batchPeriodicity: Int = 0
    var queriesPerQueue:Int = -1
    var minSharedJobs:Int = 3
    var loadFile: String = ""
    var saveFile: String = ""
    var numQueues: Int = 4
    var printOutQueues: Boolean = false
    var generateNumQueries: Int = 40
    var generateShareProb: Double = .2
    var generatePeriod: Int = 5
    var norun: Boolean = false
    while (i < args.length) {
      if(args(i) == "-s") {
        sparkAddress = args(i+1)
        i = i+1
      } else if (args(i) == "-h") {
        hdfsAddress = args(i+1)
        i = i+1
      } else if (args(i) == "-c") {
        cacheOption = args(i+1)
        i = i+1
      } else if (args(i) == "-p") {
        batchPeriodicity = args(i+1).toInt
        i = i+1
      } else if (args(i) == "-q") {
        queriesPerQueue = args(i+1).toInt
        i = i+1
      } else if (args(i) == "-j") {
        minSharedJobs = args(i+1).toInt
        i = i+1
      } else if (args(i) == "-num_queues") {
        numQueues = args(i+1).toInt
        i = i+1
      } else if (args(i) == "-load") {
        loadFile = args(i+1)
        i = i+1
      } else if (args(i) == "-save") {
        saveFile = args(i+1)
        i = i+1
      } else if (args(i) == "-print") {
        printOutQueues = true
      } else if (args(i) == "-generate") {
        generateNumQueries = args(i+1).toInt
        generateShareProb = args(i+2).toDouble
        generatePeriod = args(i+3).toInt
        i = i + 3
      } else if (args(i) == "-norun") {
        norun = true
      }       
      i = i + 1
    }
    if(sparkAddress == "" || hdfsAddress == "") {
      printHelp()
      System.exit(0)
    } else {
      val cachePlanner = new CachePlanner(sparkAddress, hdfsAddress)
      cachePlanner.batchPeriodicity = batchPeriodicity
      cachePlanner.maxQueriesPerQueueinBatch = queriesPerQueue
      cachePlanner.minSharedjobs = minSharedJobs
      
      if(loadFile == "") {
        val poolNames = new ArrayBuffer[String]
        for(i <- 1 until (numQueues+1))
          poolNames += ("pool"+i)
          cachePlanner.initialize(poolNames: _*)
        //prepopulateQueues(cachePlanner)
          generateWorkload(cachePlanner, generateNumQueries, generateShareProb, generatePeriod)
      } else {
        loadQueriesIntoQueuesFromFile(cachePlanner, loadFile)
      }
      if(saveFile != "") {
        saveQueuesToFile(cachePlanner, saveFile)
      }
      if(printOutQueues)
        printQueues(cachePlanner)
      
      if(!norun)  
        if(cacheOption == "nocache")
          cachePlanner.start(1)
        else
          cachePlanner.start(2)
                   
    }    
  }
  
  //Query(val input: String, val operation: String, val groupCol: Int, val aggCol: Int, val separator: String, val parallelism: Int)
  def prepopulateQueues(cachePlanner: CachePlanner) {
    cachePlanner.addQueryToPool("pool1", new Query("/tpch/partsupp",Count,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool2", new Query("/tpch/partsupp",Sum,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool3", new Query("/tpch/partsupp",Max,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool4", new Query("/tpch/partsupp",Min,0,1,"\\|",10))    
  }
  
  //Query(val input: String, val operation: String, val groupCol: Int, val aggCol: Int, val separator: String, val parallelism: Int)
  def prepopulateQueues2(cachePlanner: CachePlanner) {
    cachePlanner.addQueryToPool("pool1", new Query("/tpch2/partsupp",CountByKey,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool2", new Query("/tpch2/partsupp",Mean,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool3", new Query("/tpch2/partsupp",Variance,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool4", new Query("/tpch2/partsupp",Sum,0,1,"\\|",10))    
  }
  
  //Placeholder for a function that generates queries and put into queues based on some workload properties
  //This will populate each queue with numQueriesPerQueue queries
  //Assumes that basedatasets are in /tpch/
  //probShare is the probability that you will have a shared dataset within a period (period > 1)
  def generateWorkload(cachePlanner: CachePlanner, numQueriesPerQueue: Int, probShare: Double, period: Int) {
    //randomly picks a dataset, then decide whether there will be sharing
    val datasets = Array("/tpch/partsupp", "/tpch/orders", "/tpch/lineitem", "/tpch/customer", "/tpch/part", 
                         "/tpch2/partsupp", "/tpch2/orders", "/tpch2/lineitem", "/tpch2/customer", "/tpch2/part",
                         "/tpch3/partsupp", "/tpch3/orders", "/tpch3/lineitem", "/tpch3/customer", "/tpch3/part",
                         "/tpch4/partsupp", "/tpch4/orders", "/tpch4/lineitem", "/tpch4/customer", "/tpch4/part")
    val random = new Random(Platform.currentTime)
    
    var index: Int = 0
    while (index < numQueriesPerQueue) {
      var currentPeriod = period
      if(numQueriesPerQueue - index < period)
        currentPeriod = numQueriesPerQueue - index
      
      //The number of queries to assign in the current iteration
      val numQueries = currentPeriod * cachePlanner.pools.size
      
      var jobIndex: Int = 0
      while(jobIndex < numQueries) {
        //Randomly pick a dataset
        val dataset = datasets(random.nextInt(datasets.length))
        var groupCol:Int = 0
        var aggCol:Int = 1
        if(dataset.contains("customer")) {
          groupCol = 3
          aggCol = 5
        } else if (dataset.contains("orders")) {
          groupCol = 1
          aggCol = 0
        } else if (dataset.contains("part") && !dataset.contains("partsupp")) {
          groupCol = 3
          aggCol = 7
        }
        //Check whether the current dataset will be shared
        if(random.nextDouble() < probShare) { //sharing
          //randomly picks the number of jobs to share
          var numJobs = random.nextInt(numQueries - jobIndex) + 1 //ranges from 1 to (numQueries - jobIndex)          
          if (numJobs < 2 && numQueries - jobIndex > 1)
            numJobs = 2
          for(i <- 0 until numJobs) {
            cachePlanner.addQueryToPool("pool"+((jobIndex % cachePlanner.pools.size)+1),new Query(dataset,QueryOperation(random.nextInt(QueryOperation.values.size)),groupCol,aggCol,"\\|",10))
            jobIndex = jobIndex + 1
          }          
        } else { //no sharing
          //randomly pick an operation
          cachePlanner.addQueryToPool("pool"+((jobIndex % cachePlanner.pools.size)+1),new Query(dataset,QueryOperation(random.nextInt(QueryOperation.values.size)),groupCol,aggCol,"\\|",10))
          jobIndex = jobIndex + 1
        }        
      }      
      index = index + currentPeriod        
    }                        
  }
  
  //prints out the contents of the queues
  def printQueues(cachePlanner: CachePlanner) {
    val keys = cachePlanner.poolNameToPool.keys.toList.sorted//new ArrayBuffer[String]    
    
    for(key <- keys) {
      printf("%-32s",key)
    }
    println
    var index: Int = 0
    var exhausted: Int = 0
    while(exhausted < cachePlanner.pools.size) {
      exhausted = 0
      for(key <- keys) {
        if(index < cachePlanner.poolNameToPool(key).queryQueue.size) {
          printf("%-32s",cachePlanner.poolNameToPool(key).queryQueue(index).operation+"("+cachePlanner.poolNameToPool(key).queryQueue(index).input+","+cachePlanner.poolNameToPool(key).queryQueue(index).groupCol+","+cachePlanner.poolNameToPool(key).queryQueue(index).aggCol+")")
           //print(cachePlanner.poolNameToPool(key).queryQueue(index).operation+"("+cachePlanner.poolNameToPool(key).queryQueue(index).input+","+cachePlanner.poolNameToPool(key).queryQueue(index).groupCol+","+cachePlanner.poolNameToPool(key).queryQueue(index).aggCol+")"+"\t")
        } else {
          printf("%-32s"," ")
          //print("\t")
          exhausted = exhausted + 1
        }
      }
      println
      index = index + 1
    }    
  }
  
  //Function that deserializes a file and loads the queries into the queues
  def loadQueriesIntoQueuesFromFile(cachePlanner: CachePlanner, file: String) {
    val input = new ObjectInputStream(new FileInputStream(file))
    val obj = input.readObject().asInstanceOf[HashMap[String,Pool]]
    for(key <- obj.keySet) {
      cachePlanner.poolNameToPool(key) = obj(key)
      cachePlanner.pools += obj(key)
    }
    input.close()
  }
  
  //Function that serializes the queues into a file
  def saveQueuesToFile(cachePlanner: CachePlanner, file: String) {
    val store = new ObjectOutputStream(new FileOutputStream(new File(file)))
    store.writeObject(cachePlanner.poolNameToPool)
    store.close()
  }
  
  
  def printHelp() {
    println("SparkQueriesPlannerDriver -s <spark address> -h <hdfs address> [optional parameters]")
    println("optional parameters:")
    println("-c <nocache (default) | cache> - The planner strategy to use")
    println("-p <periodicity (default 0)> - The periodicity to check the queue")
    println("-q <queries per queue in batch (default -1) > - The number of queries in each queue to look ahead")
    println("-j <min shared jobs (default 3)> - number of shared jobs to consider a dataset as cacheable")
    println("-num_queues <Number of Queues (default 4 (named pooli))> - The number of external queues")
    println("-load <path to binary file to load queues> ")
    println("-save <path to file to save the queue>")
    println("-print - prints queue")
    println("-generate <num queries> <share probability> <period> - Generates a workload (fills the queues) with the given parameter")
    println("-norun - don't run the workload")
  }
  
}
