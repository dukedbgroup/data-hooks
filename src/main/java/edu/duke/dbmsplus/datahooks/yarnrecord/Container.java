/**
 * 
 */
package edu.duke.dbmsplus.datahooks.yarnrecord;

/**
 * Store Container object from yarn history server. 
 * @author Xiaodan
 */

public class Container {
	
	private String containerId;
	private String containerState;
	private int containerExitStatus;
	private String logUrl;
	private String diagnosticsInfo;
	private long startedTime;
	private long finishedTime;
	private long elapsedTime;
	private int allocatedMB;
	private int allocatedVCores;
	private int priority;
	private String assignNodeId;
	
	public Container() {
		
	}

	public String getContainerId() {
		return containerId;
	}

	public void setContainerId(String containerId) {
		this.containerId = containerId;
	}

	public String getContainerState() {
		return containerState;
	}

	public void setContainerState(String containerState) {
		this.containerState = containerState;
	}

	public int getContainerExitStatus() {
		return containerExitStatus;
	}

	public void setContainerExitStatus(int containerExitStatus) {
		this.containerExitStatus = containerExitStatus;
	}

	public String getLogUrl() {
		return logUrl;
	}

	public void setLogUrl(String logUrl) {
		this.logUrl = logUrl;
	}

	public String getDiagnosticsInfo() {
		return diagnosticsInfo;
	}

	public void setDiagnosticsInfo(String diagnosticsInfo) {
		this.diagnosticsInfo = diagnosticsInfo;
	}

	public long getStartedTime() {
		return startedTime;
	}

	public void setStartedTime(long startedTime) {
		this.startedTime = startedTime;
	}

	public long getFinishedTime() {
		return finishedTime;
	}

	public void setFinishedTime(long finishedTime) {
		this.finishedTime = finishedTime;
	}

	public long getElapsedTime() {
		return elapsedTime;
	}

	public void setElapsedTime(long elapsedTime) {
		this.elapsedTime = elapsedTime;
	}

	public int getAllocatedMB() {
		return allocatedMB;
	}

	public void setAllocatedMB(int allocatedMB) {
		this.allocatedMB = allocatedMB;
	}

	public int getAllocatedVCores() {
		return allocatedVCores;
	}

	public void setAllocatedVCores(int allocatedVCores) {
		this.allocatedVCores = allocatedVCores;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public String getAssignNodeId() {
		return assignNodeId;
	}

	public void setAssignNodeId(String assignNodeId) {
		this.assignNodeId = assignNodeId;
	}
	

}