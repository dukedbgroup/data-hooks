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
    var generateBatches: Int = 1
    var generateNumQueries: Int = 40
    var generateShareProb: Double = .2
    var generateMaxGap: Int = 5
    var generateMaxSharedJobs: Int = 5
    var norun: Boolean = false
    var generatorFlag: Boolean = true
    var programName: String = "Spark Queries Planner"
    var doAnalyze: Boolean = false
    var boundedReorder: Boolean = false
    var cacheStoragePolicy: String = "MEMORY_ONLY"
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
        generateMaxGap = args(i+3).toInt
        generateMaxSharedJobs = args(i+4).toInt
        i = i + 4
      } else if(args(i) == "-generateBatch") {
        generatorFlag = false
        generateBatches = args(i+1).toInt
        generateNumQueries = args(i+2).toInt
        generateShareProb = args(i+3).toDouble
        i = i + 4
        //"-generate2 <num batches> <batchSize> <share probability> - Generates a workload (fills the queues) with the given parameter"        
      } else if(args(i) == "-cacheStoragePolicy") {        
        cacheStoragePolicy = args(i+1)        
        i = i + 1        
      }else if (args(i) == "-norun") {
        norun = true
      } else if (args(i) == "-n") {
        programName = args(i+1)
        i = i + 1
      } else if (args(i) == "-analyze") {
        doAnalyze = true
      } else if (args(i) == "-reorder") {
       if(args(i+1) == "true")
         boundedReorder = true
       else
         boundedReorder = false
       i = i + 1  
      }     
      i = i + 1
    }
    if(sparkAddress == "" || hdfsAddress == "") {
      printHelp()
      System.exit(0)
    } else {
      var planner: Planner = null
      if(!doAnalyze) {
        //For now, we set to always use KryoSerializer and RDD compression
        System.setProperty("spark.serializer", "spark.KryoSerializer")
        System.setProperty("spark.rdd.compress", "true")
        planner = new CachePlanner(programName, sparkAddress, hdfsAddress)
        planner.asInstanceOf[CachePlanner].batchPeriodicity = batchPeriodicity
        planner.asInstanceOf[CachePlanner].maxQueriesPerQueueinBatch = queriesPerQueue
        planner.asInstanceOf[CachePlanner].minSharedjobs = minSharedJobs
        planner.asInstanceOf[CachePlanner].cacheStoragePolicy = cacheStoragePolicy
      } else {
        planner = new CachePlannerAnalyzer
        planner.asInstanceOf[CachePlannerAnalyzer].batchPeriodicity = batchPeriodicity
        planner.asInstanceOf[CachePlannerAnalyzer].maxQueriesPerQueueinBatch = queriesPerQueue
        planner.asInstanceOf[CachePlannerAnalyzer].minSharedjobs = minSharedJobs
      }

      if(loadFile == "") {
        val poolNames = new ArrayBuffer[String]
        for(i <- 1 until (numQueues+1))
          poolNames += ("pool"+i)
          planner.initialize(poolNames: _*)
        //prepopulateQueues(planner)
          //generateWorkload(planner, generateNumQueries, generateShareProb, generatePeriod)
          if(generatorFlag)
            generateWorkload2(planner, generateNumQueries, generateShareProb, generateMaxGap, generateMaxSharedJobs)
          else
            generateWorkloadBatch(planner, generateBatches, generateNumQueries, generateShareProb)
      } else {
        loadQueriesIntoQueuesFromFile(planner, loadFile)
      }
      if(saveFile != "") {
        saveQueuesToFile(planner, saveFile)
      }
      if(printOutQueues)
        printQueues(planner)
      if(!norun && !doAnalyze)  
        if(cacheOption == "nocache")
          planner.asInstanceOf[CachePlanner].start(1)
        else if(cacheOption == "cache") {
          if(!boundedReorder)
            planner.asInstanceOf[CachePlanner].start(2)
          else
            planner.asInstanceOf[CachePlanner].start(3)
        } else {
          if(!boundedReorder)
            planner.asInstanceOf[CachePlanner].start(4)
          else
            planner.asInstanceOf[CachePlanner].start(5)
        }
      if(doAnalyze) 
        if(cacheOption == "nocache")
          planner.asInstanceOf[CachePlannerAnalyzer].analyze(1)
        else
          planner.asInstanceOf[CachePlannerAnalyzer].analyze(2)

    }    
  }
  
  //Query(val input: String, val operation: String, val groupCol: Int, val aggCol: Int, val separator: String, val parallelism: Int)
  def prepopulateQueues(cachePlanner: Planner) {
    cachePlanner.addQueryToPool("pool1", new Query("/tpch/partsupp",Count,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool2", new Query("/tpch/partsupp",Sum,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool3", new Query("/tpch/partsupp",Max,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool4", new Query("/tpch/partsupp",Min,0,1,"\\|",10))    
  }
  
  //Query(val input: String, val operation: String, val groupCol: Int, val aggCol: Int, val separator: String, val parallelism: Int)
  def prepopulateQueues2(cachePlanner: Planner) {
    cachePlanner.addQueryToPool("pool1", new Query("/tpch2/partsupp",CountByKey,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool2", new Query("/tpch2/partsupp",Mean,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool3", new Query("/tpch2/partsupp",Variance,0,1,"\\|",10))
    cachePlanner.addQueryToPool("pool4", new Query("/tpch2/partsupp",Sum,0,1,"\\|",10))    
  }
  
  //This will populate each queue with numQueriesPerQueue queries
  //Assumes that basedatasets are in /tpch/
  //probShare is the probability that you will have a shared dataset within a period (period > 1)
  def generateWorkload(cachePlanner: Planner, numQueriesPerQueue: Int, probShare: Double, period: Int) {
    //randomly picks a dataset, then decide whether there will be sharing
    val datasets = Array("/tpch/partsupp", "/tpch/orders", "/tpch/customer", "/tpch/part", 
                         "/tpch2/partsupp", "/tpch2/orders", "/tpch2/customer", "/tpch2/part",
                         "/tpch3/partsupp", "/tpch3/orders", "/tpch3/customer", "/tpch3/part",
                         "/tpch4/partsupp", "/tpch4/orders", "/tpch4/customer", "/tpch4/part",
                         "/tpch5/partsupp", "/tpch5/orders", "/tpch5/customer", "/tpch5/part")
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
  
  def Desc[T : Ordering] = implicitly[Ordering[T]].reverse
  
  //Utility object that is used by the workloadGenerator
  class GeneratorUtilObj(val dataset: String,val proximity: Int, var numJobs: Int, var lastIndex: Int){    
  }
  
  //This generator will populate the queues, It creates numBatches batches, where each Batch contains NumQueriesPerBatch
  //percShared is the percentage of queries in the batch will be sharing the dataset in each batch
  def generateWorkloadBatch(cachePlanner: Planner, numBatches: Int, numQueriesPerBatch: Int, percShared: Double) {
    val numDistinctDatasets: Int = 80
    val datasets = new ArrayBuffer[String]
    var dIndex: Int = 0
    while (dIndex < numDistinctDatasets/*/2*/) {
      datasets += ("/tpch"+((dIndex%(numDistinctDatasets/2))+1)+"/orders")
      dIndex = dIndex + 1
    }
    
    val random = new Random(Platform.currentTime)
    dIndex = 0
    val numShared = ((numQueriesPerBatch * cachePlanner.pools.size) * percShared).toInt
    var index: Int = 0
    
    for(batchIndex <- 0 until numBatches) {
      
      var bIndex: Int = 0
      for(count <- 0 until numShared) {
        val poolsIndex = index % cachePlanner.pools.size //This is to choose which pool
        val poolIndex = index / cachePlanner.pools.size //This is to choose which element in the pool
        cachePlanner.addQueryToPool("pool"+(poolsIndex+1),new Query(datasets(dIndex),QueryOperation(random.nextInt(QueryOperation.values.size-1)+1),1,0,"\\|",10))
        bIndex = bIndex + 1
        index = index + 1
      }
      dIndex = dIndex + 1
      if(dIndex >= 80) {
        dIndex = 0
      }
      
      while(bIndex < (numQueriesPerBatch * cachePlanner.pools.size)) {
        val poolsIndex = index % cachePlanner.pools.size //This is to choose which pool
        val poolIndex = index / cachePlanner.pools.size //This is to choose which element in the pool
        cachePlanner.addQueryToPool("pool"+(poolsIndex+1),new Query(datasets(dIndex),QueryOperation(random.nextInt(QueryOperation.values.size-1)+1),1,0,"\\|",10))
        bIndex = bIndex + 1
        index = index + 1

        dIndex = dIndex + 1
        if(dIndex >= 80) {
          dIndex = 0
        }        
      }
    }
    
  }
  
  //This will populate each queue with numQueriesPerQueue queries
  //Assumes that basedatasets are in /tpch*/
  //probShare is the probability that you will have a shared dataset
  //maxGap is the maximum temporal spread between jobs that share the same dataset
  //maxSharedJobs is the maximum number of jobs that share the same dataset
  def generateWorkload2(cachePlanner: Planner, numQueriesPerQueue: Int, probShare: Double, maxGap: Int, maxSharedJobs: Int) {
    //randomly picks a dataset, then decide whether there will be sharing
    //val datasets = Array("/tpch/partsupp", "/tpch/orders", "/tpch/customer", "/tpch/part", 
    //                     "/tpch2/partsupp", "/tpch2/orders", "/tpch2/customer", "/tpch2/part",
    //                     "/tpch3/partsupp", "/tpch3/orders", "/tpch3/customer", "/tpch3/part",
    //                     "/tpch4/partsupp", "/tpch4/orders", "/tpch4/customer", "/tpch4/part",
    //                     "/tpch5/partsupp", "/tpch5/orders", "/tpch5/customer", "/tpch5/part")

    val numDistinctDatasets: Int = 80
    val datasets = new ArrayBuffer[String]
    var dIndex: Int = 0
    //println("Printing List of Input Datasets")
    while (dIndex < numDistinctDatasets/*/2*/) {
      //datasets += ("/tpch"+((dIndex%(numDistinctDatasets/2))+1)+"/partsupp")
      datasets += ("/tpch"+((dIndex%(numDistinctDatasets/2))+1)+"/orders")
      //println(("/tpch"+((dIndex%(numDistinctDatasets/2))+1)+"/orders"))
      //println(("/tpch"+((dIndex%(numDistinctDatasets/2))+1)+"/partsupp"))
      dIndex = dIndex + 1
    } //For now we just use partsupp and orders table because the size of these tables are roughly the same, the other tables are either significantly smaller or larger


    val random = new Random(Platform.currentTime)
    
    val outstandingDatasets = new ArrayBuffer[GeneratorUtilObj]
    
    for (index <- 0 until numQueriesPerQueue * cachePlanner.pools.size) {
      val poolsIndex = index % cachePlanner.pools.size //This is to choose which pool
      val poolIndex = index / cachePlanner.pools.size //This is to choose which element in the pool
      
      //First look if there is an outstandingDataset that we can add to the current index
      //We filter by proximity < lastIndex - poolIndex, sorted by the proximity and then the by the number of jobs left (in dec order)
      val filteredOutstandingDatasets = outstandingDatasets.filter(a => {a.proximity <= poolIndex - a.lastIndex}).sortBy(m => (m.proximity))
      if(filteredOutstandingDatasets.size > 0) {
        val currentDataset = filteredOutstandingDatasets(0) //Get the first one
        var groupCol:Int = 0
        var aggCol:Int = 1
        if(currentDataset.dataset.contains("customer")) {
          groupCol = 3
          aggCol = 5
        } else if (currentDataset.dataset.contains("orders")) {
          groupCol = 1
          aggCol = 0
        } else if (currentDataset.dataset.contains("part") && !currentDataset.dataset.contains("partsupp")) {
          groupCol = 3
          aggCol = 7
        }
        cachePlanner.addQueryToPool("pool"+(poolsIndex+1),new Query(currentDataset.dataset,QueryOperation(random.nextInt(QueryOperation.values.size-1)+1),groupCol,aggCol,"\\|",10))
       //println("Using outstanding dataset: "+currentDataset.dataset+", with proximity: "+currentDataset.proximity+", numJobs: "+currentDataset.numJobs+", at last Index: "+poolIndex)
        currentDataset.numJobs = currentDataset.numJobs - 1
        if(currentDataset.numJobs <= 0) {
          //remove this from the outstandingDatasets
          outstandingDatasets -= currentDataset
        } else {
          currentDataset.lastIndex = poolIndex
        }
        
      }
      else { //No outstanding dataset is eligible to be added, so we just pick one

        val chosenDataset = datasets(random.nextInt(datasets.size))
        //we remove from chosenDataset the list of datasets
        datasets -= chosenDataset
        var groupCol:Int = 0
        var aggCol:Int = 1
        if(chosenDataset.contains("customer")) {
          groupCol = 3
          aggCol = 5
        } else if (chosenDataset.contains("orders")) {
          groupCol = 1
          aggCol = 0
        } else if (chosenDataset.contains("part") && !chosenDataset.contains("partsupp")) {
          groupCol = 3
          aggCol = 7
        }
        //println("Chosen Dataset: "+chosenDataset)
        //Check whether the current dataset will be shared
        if(random.nextDouble() < probShare) { //sharing
          //
          var numJobs = random.nextInt(maxSharedJobs)
          if(numJobs < 2)
            numJobs = 2

          val proximity = maxGap//random.nextInt(maxGap + 1)
          cachePlanner.addQueryToPool("pool"+(poolsIndex+1),new Query(chosenDataset,QueryOperation(random.nextInt(QueryOperation.values.size-1)+1),groupCol,aggCol,"\\|",10))
          outstandingDatasets += new GeneratorUtilObj(chosenDataset, proximity, numJobs - 1, poolIndex)
          //println("Will be shared: "+chosenDataset+", with proximity: "+proximity+", numJobs: "+numJobs+", at last Index: "+poolIndex)
        } else {
          //randomly pick an operation
          cachePlanner.addQueryToPool("pool"+(poolsIndex+1),new Query(chosenDataset,QueryOperation(random.nextInt(QueryOperation.values.size-1)+1),groupCol,aggCol,"\\|",10))
        }        
      }           
    } 
  }
  
  
  //prints out the contents of the queues
  def printQueues(cachePlanner: Planner) {
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
  def loadQueriesIntoQueuesFromFile(cachePlanner: Planner, file: String) {
    val input = new ObjectInputStream(new FileInputStream(file))
    val obj = input.readObject().asInstanceOf[HashMap[String,Pool]]
    for(key <- obj.keySet) {
      cachePlanner.poolNameToPool(key) = obj(key)
      cachePlanner.pools += obj(key)
    }
    input.close()
  }
  
  //Function that serializes the queues into a file
  def saveQueuesToFile(cachePlanner: Planner, file: String) {
    val store = new ObjectOutputStream(new FileOutputStream(new File(file)))
    store.writeObject(cachePlanner.poolNameToPool)
    store.close()
  }
  
  
  def printHelp() {
    println("SparkQueriesPlannerDriver -s <spark address> -h <hdfs address> [optional parameters]")
    println("optional parameters:")
    println("-n <name or ID of Program (Default: Spark Queries Planner>")
    println("-c <nocache (default) | cache | cachePartitioned> - The planner strategy to use")
    println("-cacheStoragePolicy <MEMORY_ONLY (default) | MEMORY_ONLY_SER >")
    println("-reorder <false (default) | true > - Whether to re-order the queries that use cached dataset to the front")
    println("-p <periodicity (default 0)> - The periodicity to check the queue")
    println("-q <queries per queue in batch (default -1) > - The number of queries in each queue to look ahead")
    println("-j <min shared jobs (default 3)> - number of shared jobs to consider a dataset as cacheable")
    println("-num_queues <Number of Queues (default 4 (named pooli))> - The number of external queues")
    println("-load <path to binary file to load queues> ")
    println("-save <path to file to save the queue>")
    println("-print - prints queue")
    //println("-generate <num queries> <share probability> <period> - Generates a workload (fills the queues) with the given parameter")
    //probShare: Double, maxGap: Int, maxSharedJobs: Int
    println("-generate <num queries> <share probability> <maxGap> <maxSharedJobs> - Generates a workload (fills the queues) with the given parameter")
    println("-generateBatch <num batches> <batchSize> <percent shared> - Generates a workload (fills the queues) with the given parameter")
    println("-analyze - print analysis stats")
    println("-norun - don't run the workload")
  }
  
}
