/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution.profile;

/**
 * @author mayuresh
 *
 */
public class Query {
	Long qid;
	Long cid;
	Long wid;
	String queryString;
	String engine;
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
	 * @return the queryString
	 */
	public String getQueryString() {
		return queryString;
	}
	/**
	 * @param queryString the queryString to set
	 */
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}
	/**
	 * @return the engine
	 */
	public String getEngine() {
		return engine;
	}
	/**
	 * @param engine the engine to set
	 */
	public void setEngine(String engine) {
		this.engine = engine;
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
