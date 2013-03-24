package edu.duke.dbmsplus.planner

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.TreeSet
import scala.math._


import edu.duke.dbmsplus.planner.utils._
import edu.duke.dbmsplus.planner.QueryOperation._

/**
 * An abstact class of planner. It only contains representation of Queues and methods to add queries and initialize queues
 */
abstract class Planner {
  //External Queues
  val poolNameToPool = new HashMap[String, Pool]
  var pools = new ArrayBuffer[Pool]

  //Add a query to the specified pool
  def addQueryToPool(poolName: String, query: Query) {
    pools.synchronized {
      if(poolNameToPool.contains(poolName))
        poolNameToPool(poolName).queryQueue += query
      else
        poolNameToPool("default").queryQueue += query
      pools.notifyAll
    }
  }

  //Initialize the pools with the specified names
  def initialize(names:String*) {
    for(name <- names){
      val pool = new Pool(name)
      pools += pool
      poolNameToPool(name) = pool
    }
  }

}

/**
 * An internal representation of a pool (Queue. It contains an ArrayBuffer of Query
 */
class Pool(val name: String) extends Serializable
{
  var queryQueue = new ArrayBuffer[Query]
}

