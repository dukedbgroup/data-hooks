/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution.profile;

/**
 * @author mayuresh
 *
 */
public class Component {
	Long cid;
	Long wid;
	String name;
	String engine;
	Long startTime;
	Long endTime;
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
