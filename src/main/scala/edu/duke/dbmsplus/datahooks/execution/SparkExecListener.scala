/**
 *
 */
package edu.duke.dbmsplus.datahooks.execution

import java.lang.Override

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.SparkListenerJobStart
import org.apache.spark.scheduler.SparkListenerStageCompleted
import org.apache.spark.scheduler.SparkListenerStageSubmitted
import org.apache.spark.scheduler.SparkListenerTaskEnd
import org.apache.spark.scheduler.SparkListenerTaskGettingResult
import org.apache.spark.scheduler.SparkListenerTaskStart
import org.apache.spark.scheduler.SparkListenerUnpersistRDD

import edu.duke.dbmsplus.datahooks.execution.profile.ProfileDataWriter
import edu.duke.dbmsplus.datahooks.execution.profile.SparkStage
import edu.duke.dbmsplus.datahooks.listener.BigFrameListenerImpl

/**
 * Initialized with 
 * 1. an instance of {@code BigFrameListener} which is 
 * used to extract query identifier before logging details of a stage
 * 2. an instance of {@code ProfileDataWriter} which is 
 * used to log stage details
 * @author mayuresh
 *
 */
class SparkExecListener(bflistener: BigFrameListenerImpl, 
    writer: ProfileDataWriter) extends SparkListener {

  var stageToJob = collection.mutable.Map[Int, Int]()

  @Override
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val stage = new SparkStage()

    // query identifier
    val queryId = bflistener.getQueryIdentifier()
    stage.setWid(queryId(0))
    stage.setCid(queryId(1))
    stage.setQid(queryId(2))
    
    // stage execution details
    val stageId = event.stageInfo.stageId
    stage.setStageId(stageId)
    stage.setName(event.stageInfo.name)
    stage.setNumTasks(event.stageInfo.numTasks)
    stage.setSubmissionTime(event.stageInfo.submissionTime match {
      case Some(time) => time 
      case None => 0L})
    stage.setCompletionTime(event.stageInfo.completionTime match {
      case Some(time) => time
      case None => 0L})
    // extract jobId
    try {
    	stage.setJobId(stageToJob(stageId))
    } catch{
    case e: Exception => stage.setJobId(-1)
    }

    // write the stage
    writer.addSparkStage(stage)
    
    // TODO: rdd details can be obtained here including the number of bytes shuffled  
//    val rdds = event.stageInfo.rddInfos
//    for(rdd <- rdds) {
//      rdd.id
//    }    
    
    // TODO: failed task details
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
    // keep a map of stageId to jobId
    for(stageId <- event.stageIds) {
      stageToJob += (stageId -> event.jobId)
    }
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