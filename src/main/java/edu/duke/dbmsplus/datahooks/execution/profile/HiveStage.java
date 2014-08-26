/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution.profile;

/**
 * @author mayuresh
 *
 */
public class HiveStage {
	Long qid;
	Long cid;
	Long wid;
	String stageId;
	String hadoopJobId;
	String dependentChildren;
	String hiveTaskInfos;
	Long startTime;
	Long endTime;
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
	public String getStageId() {
		return stageId;
	}
	/**
	 * @param stageId the stageId to set
	 */
	public void setStageId(String stageId) {
		this.stageId = stageId;
	}
	/**
	 * @return the hadoopJobId
	 */
	public String getHadoopJobId() {
		return hadoopJobId;
	}
	/**
	 * @param hadoopJobId the hadoopJobId to set
	 */
	public void setHadoopJobId(String hadoopJobId) {
		this.hadoopJobId = hadoopJobId;
	}
	/**
	 * @return the dependentChildren
	 */
	public String getDependentChildren() {
		return dependentChildren;
	}
	/**
	 * @param dependentChildren the dependentChildren to set
	 */
	public void setDependentChildren(String dependentChildren) {
		this.dependentChildren = dependentChildren;
	}
	/**
	 * @return the hiveTaskInfos
	 */
	public String getHiveTaskInfos() {
		return hiveTaskInfos;
	}
	/**
	 * @param hiveTaskInfos the hiveTaskInfos to set
	 */
	public void setHiveTaskInfos(String hiveTaskInfos) {
		this.hiveTaskInfos = hiveTaskInfos;
	}
	/**
	 * @return the startTime
	 */
	public Long getStartTime() {
		return startTime;
	}
	/**
	 * @param startTime the startTime to set
	 */
	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}
	/**
	 * @return the endTime
	 */
	public Long getEndTime() {
		return endTime;
	}
	/**
	 * @param endTime the endTime to set
	 */
	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}
}
