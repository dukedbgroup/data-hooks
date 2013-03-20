package edu.duke.dbmsplus

import spark._
import SparkContext._

import java.io._

import scala.collection.mutable.HashMap

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
      } else if (args(i) == "-load") {
        loadFile = args(i+1)
        i = i+1
      } else if (args(i) == "-save") {
        saveFile = args(i+1)
        i = i+1
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
        cachePlanner.initialize("pool1", "pool2", "pool3", "pool4")
        prepopulateQueues(cachePlanner)
      } else {
        loadQueriesIntoQueuesFromFile(cachePlanner, loadFile)
      }
      if(saveFile != "") {
        saveQueuesToFile(cachePlanner, saveFile)
      }
      //prepopulateQueues2(cachePlanner)
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
  def generateWorkload() {
    
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
    println("-c <nocache (default) | cache>")
    println("-p <periodicity (default 0)>")
    println("-q <queries per queue in batch (default -1) >")
    println("-j <min shared jobs (default 3)>")
    println("-load <path to binary file to load queues>")
    println("-save <path to file to save the queue>")
  }
  
}
