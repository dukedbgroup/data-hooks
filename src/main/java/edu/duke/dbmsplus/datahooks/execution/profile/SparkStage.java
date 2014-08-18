/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution.profile;

/**
 * @author mayuresh
 *
 */
public class SparkStage {
	Long qid;
	Long cid;
	Long wid;
	Integer stageId;
	Integer jobId;
	String name;
	Integer numTasks;
	Long submissionTime;
	Long completionTime;
	/**
	 * @return the qid
	 */
	public Long getQid() {
		return qid;
	}
	/**
	 * @param qid the qid to set
	 */
	public void setQid(Long qid) {
		this.qid = qid;
	}
	/**
	 * @return the cid
	 */
	public Long getCid() {
		return cid;
	}
	/**
	 * @param cid the cid to set
	 */
	public void setCid(Long cid) {
		this.cid = cid;
	}
	/**
	 * @return the wid
	 */
	public Long getWid() {
		return wid;
	}
	/**
	 * @param wid the wid to set
	 */
	public void setWid(Long wid) {
		this.wid = wid;
	}
	/**
	 * @return the stageId
	 */
	public Integer getStageId() {
		return stageId;
	}
	/**
	 * @param stageId the stageId to set
	 */
	public void setStageId(Integer stageId) {
		this.stageId = stageId;
	}
	/**
	 * @return the jobId
	 */
	public Integer getJobId() {
		return jobId;
	}
	/**
	 * @param jobId the jobId to set
	 */
	public void setJobId(Integer jobId) {
		this.jobId = jobId;
	}
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * @return the numTasks
	 */
	public Integer getNumTasks() {
		return numTasks;
	}
	/**
	 * @param numTasks the numTasks to set
	 */
	public void setNumTasks(Integer numTasks) {
		this.numTasks = numTasks;
	}
	/**
	 * @return the submissionTime
	 */
	public Long getSubmissionTime() {
		return submissionTime;
	}
	/**
	 * @param submissionTime the submissionTime to set
	 */
	public void setSubmissionTime(Long submissionTime) {
		this.submissionTime = submissionTime;
	}
	/**
	 * @return the completionTime
	 */
	public Long getCompletionTime() {
		return completionTime;
	}
	/**
	 * @param completionTime the completionTime to set
	 */
	public void setCompletionTime(Long completionTime) {
		this.completionTime = completionTime;
	}
}
