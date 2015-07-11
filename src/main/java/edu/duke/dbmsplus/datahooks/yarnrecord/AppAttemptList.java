package edu.duke.dbmsplus.datahooks.yarnrecord;
/**
 * List of Application attempts.
 * need to rewrite. Add operation of app attempts here.
 * @author Xiaodan
 *
 */
public class AppAttemptList {
	
	private AppAttempt[] appAttemptlist;
	
	public AppAttemptList() {
		
	}

	public AppAttempt[] getAppAttemptlist() {
		return appAttemptlist;
	}

	public void setAppAttemptlist(AppAttempt[] appAttemptlist) {
		this.appAttemptlist = appAttemptlist;
	}
	
}
