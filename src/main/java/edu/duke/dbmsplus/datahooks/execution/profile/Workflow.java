/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution.profile;

/**
 * @author mayuresh
 *
 */
public class Workflow {
	Long wid;
	String name;
	String userName;
	Long startTime;
	Long endTime;
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
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}
	/**
	 * @param userName the userName to set
	 */
	public void setUserName(String userName) {
		this.userName = userName;
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
