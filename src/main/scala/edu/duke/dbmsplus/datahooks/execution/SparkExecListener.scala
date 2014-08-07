/**
 *
 */
package edu.duke.dbmsplus.datahooks.execution

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.scheduler.SparkListenerTaskGettingResult
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved
import org.apache.spark.scheduler.SparkListenerUnpersistRDD
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerApplicationEnd

/**
 * @author mayuresh
 *
 */
class SparkExecListener extends SparkListener {

  @Override
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stage = event.stageInfo
    // stage details
    stage.completionTime
    stage.numTasks
    stage.name
    stage.submissionTime
    stage.stageId
    // extract jobId
    
    // rdd details can be obtained here    
    val rdds = stage.rddInfos
    for(rdd <- rdds) {
      rdd
    }    
  }

  @Override
  override def onStageSubmitted(arg0: SparkListenerStageSubmitted): Unit = {}

  @Override
  override def onTaskStart(arg0: SparkListenerTaskStart): Unit = {}

  @Override
  override def onTaskGettingResult(arg0: SparkListenerTaskGettingResult): Unit = {}

  @Override
  override def onTaskEnd(arg0: SparkListenerTaskEnd): Unit = {}

  @Override
  override def onJobStart(event: SparkListenerJobStart): Unit = {
    event.jobId
    event.stageIds
    // keep a map of stageId to jobId
    
    // map job to query
  }

  @Override
  override def onJobEnd(arg0: SparkListenerJobEnd): Unit = {}

  @Override
  override def onEnvironmentUpdate(arg0: SparkListenerEnvironmentUpdate): Unit = {}

  @Override
  override def onBlockManagerAdded(arg0: SparkListenerBlockManagerAdded): Unit = {}

  @Override
  override def onBlockManagerRemoved(arg0: SparkListenerBlockManagerRemoved): Unit = {}

  @Override
  override def onUnpersistRDD(arg0: SparkListenerUnpersistRDD): Unit = {}

  @Override
  override def onApplicationStart(arg0: SparkListenerApplicationStart): Unit = {}

  @Override
  override def onApplicationEnd(arg0: SparkListenerApplicationEnd): Unit = {}

}