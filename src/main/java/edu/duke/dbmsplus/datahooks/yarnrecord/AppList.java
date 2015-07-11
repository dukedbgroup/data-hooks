/**
 * 
 */
package edu.duke.dbmsplus.datahooks.yarnrecord;

import edu.duke.dbmsplus.datahooks.yarnrecord.App;
/**
 * A list of App object.
 * @author hadoop
 *
 */
public class AppList {
	private App[] apps;
	
	AppList(){
		
	}

	public void setApps (App[] appArray) {
		this.apps = appArray;
	}
	
	public App[] getApps () {
		return this.apps;
	}
}
