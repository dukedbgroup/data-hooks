package edu.duke.dbmsplus.datahooks.execution.pojo;

import java.util.ArrayList;
import java.util.List;



public class HiveTaskInfo {
	private String taskId;
	private String taskType;
	
	private List<HiveOperatorInfo> hiveOperatorInfos;
	
	public static final String MAP_TYPE = "MAP";
	
	public HiveTaskInfo(){
		hiveOperatorInfos = new ArrayList<HiveOperatorInfo>();
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public String getTaskType() {
		return taskType;
	}

	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	public List<HiveOperatorInfo> getHiveOperatorInfos() {
		return hiveOperatorInfos;
	}

	public void setHiveOperatorInfos(List<HiveOperatorInfo> hiveOperatorInfos) {
		this.hiveOperatorInfos = hiveOperatorInfos;
	}


	
}
