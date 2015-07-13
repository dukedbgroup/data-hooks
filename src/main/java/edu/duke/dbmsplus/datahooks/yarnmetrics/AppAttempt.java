package edu.duke.dbmsplus.datahooks.yarnrecord;
/**
 * Application attempt Object
 * @author hadoop
 *
 */
public class AppAttempt {
	
	/**
	 * The application attempt Id 
	 */
	private String appAttemptId;
	/**
	 * The ApplicationMaster container Id 
	 */
	private String amContainerId;
	/**
	 * The application attempt state according to the ResourceManager - valid values are members of the YarnApplicationAttemptState enum: FINISHED, FAILED, KILLED 
	 */
	private String appAttemptState;
	/**
	 * The web URL that can be used to track the application 
	 */
	private String trackingUrl;
	/**
	 * The actual web URL of the application 
	 */
	private String originalTrackingUrl;
	/**
	 * Detailed diagnostics information 
	 */
	private String diagnosticsInfo;
	/**
	 * The host of the ApplicationMaster 
	 */
	private String host;
	/**
	 * The rpc port of the ApplicationMaster 
	 */
	private int rpcPort;
	
	public AppAttempt() {
		
	}
	
	public String getAppAttemptId() {
		return appAttemptId;
	}
	
	public void setAppAttemptId(String appAttemptId) {
		this.appAttemptId = appAttemptId;
	}
	
	public String getAmContainerId() {
		return amContainerId;
	}
	
	public void setAmContainerId(String amContainerId) {
		this.amContainerId = amContainerId;
	}
	
	public String getAppAttemptState() {
		return appAttemptState;
	}
	
	public void setAppAttemptState(String appAttemptState) {
		this.appAttemptState = appAttemptState;
	}
	
	public String getTrackingUrl() {
		return trackingUrl;
	}
	
	public void setTrackingUrl(String trackingUrl) {
		this.trackingUrl = trackingUrl;
	}
	
	public String getOriginalTrackingUrl() {
		return originalTrackingUrl;
	}
	
	public void setOriginalTrackingUrl(String originalTrackingUrl) {
		this.originalTrackingUrl = originalTrackingUrl;
	}
	
	public String getDiagnosticsInfo() {
		return diagnosticsInfo;
	}
	
	public void setDiagnosticsInfo(String diagnosticsInfo) {
		this.diagnosticsInfo = diagnosticsInfo;
	}
	
	public String getHost() {
		return host;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public int getRpcPort() {
		return rpcPort;
	}
	
	public void setRpcPort(int rpcPort) {
		this.rpcPort = rpcPort;
	}

}
