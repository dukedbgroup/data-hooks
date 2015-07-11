/**
 * 
 */
package edu.duke.dbmsplus.datahooks.yarnrecord;

/**
 * Store Container object from yarn history server. 
 * @author Xiaodan
 */

public class ContainerList {
	private Container[] containerList;
	
	public ContainerList(){
		
	}

	public Container[] getContainerList() {
		return containerList;
	}

	public void setContainerList(Container[] containerList) {
		this.containerList = containerList;
	}
	
}