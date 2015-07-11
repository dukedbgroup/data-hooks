/**
 * 
 */
package edu.duke.dbmsplus.datahooks.yarnrecord;

/**
 * Store App object from yarn history server. 
 * @author Xiaodan
 */
public class App {

	/**
	 * The application ID
	 */
	private String appId;
	/**
	 * The user who started the application
	 */
	private String user;
	/**
	 * The application name
	 */
	private String name;
	/**
	 * The application type
	 */
	private String type;
	/**
	 * The queue to which the application submitted
	 */
	private String queue;
	/**
	 * The application state according to the ResourceManager - valid values are
	 * members of the YarnApplicationState enum: FINISHED, FAILED, KILLED
	 */
	private String appState;
	/**
	 * The final status of the application if finished - reported by the
	 * application itself - valid values are: UNDEFINED, SUCCEEDED, FAILED,
	 * KILLED
	 */
	private String finalStatus;
	/**
	 * The reported progress of the application as a percent. Long-lived YARN
	 * services may not provide a meaninful value here —or use it as a metric of
	 * actual vs desired container counts
	 */
	private float progress;
	/**
	 * The web URL of the application (via the RM Proxy)
	 */
	private String trackingUrl;
	/**
	 * The actual web URL of the application
	 */
	private String originalTrackingUrl;
	/**
	 * Detailed diagnostics information on a completed application
	 */
	private String diagnosticsInfo;
	/**
	 * The time in which application started (in ms since epoch)
	 */
	private long startedTime;
	/**
	 * The time in which the application finished (in ms since epoch)
	 */
	private long finishedTime;
	/**
	 * The elapsed time since the application started (in ms)
	 */
	private long elapsedTime;
	/**
	 * The sum of memory in MB allocated to the application’s running containers
	 */
	private int allocatedMB;
	/**
	 * The sum of virtual cores allocated to the application’s running
	 * containers
	 */
	private int allocatedVCores;
	/**
	 * The latest application attempt ID
	 */
	private String currentAppAttemptId;
	/**
	 * The host of the ApplicationMaster
	 */
	private String host;
	/**
	 * The RPC port of the ApplicationMaster; zero if no IPC service declared.
	 */
	private int rpcPort;
	/**
	 * Final Application Status
	 */
	private String finalAppStatus;
	/**
	 * Application submitted time
	 */
	private long submittedTime;

	public App() {

	}

	/**
	 * Generate hashCode for appId.
	 */
	public int hashCode() {
		return this.appId.hashCode();
	}

	/**
	 * Compare Apps are equal.
	 */
	public boolean equals(Object other) {
		if (!(other instanceof App))
			return false;
		App otherApp = (App) other;
		return this.hashCode() == otherApp.hashCode();
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}

	public String getAppState() {
		return appState;
	}

	public void setAppState(String appState) {
		this.appState = appState;
	}

	public String getFinalStatus() {
		return finalStatus;
	}

	public void setFinalStatus(String finalStatus) {
		this.finalStatus = finalStatus;
	}

	public float getProgress() {
		return progress;
	}

	public void setProgress(float progress) {
		this.progress = progress;
	}

	public String getTrackingUrl() {
		return trackingUrl;
	}

	public void setTrackingUrl(String trackingUrl) {
		this.trackingUrl = trackingUrl;
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

	public String getCurrentAppAttemptId() {
		return currentAppAttemptId;
	}

	public void setCurrentAppAttemptId(String currentAppAttemptId) {
		this.currentAppAttemptId = currentAppAttemptId;
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

	public String getFinalAppStatus() {
		return finalAppStatus;
	}

	public void setFinalAppStatus(String finalAppStatus) {
		this.finalAppStatus = finalAppStatus;
	}

	public long getSubmittedTime() {
		return submittedTime;
	}

	public void setSubmittedTime(long submittedTime) {
		this.submittedTime = submittedTime;
	}

	public String getOriginalTrackingUrl() {
		return originalTrackingUrl;
	}

	public void setOriginalTrackingUrl(String originalTrackingUrl) {
		this.originalTrackingUrl = originalTrackingUrl;
	}
}