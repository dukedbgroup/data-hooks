package edu.duke.dbmsplus.datahooks.execution.pojo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class HiveStageInfo {
	public static String MAPRED_STAGE_TYPE = "MAPRED";
	
	private String hadoopJobId;
	
	// Hive notion
	private String stageId;
	
	// Hive notion
	private String stageType;

	// Input paths
	private List<String> inputPaths;
	
	// alias to path
	private Map<String, String> aliasToPath;
	
	// the mapping between alias and table names
	private Map<String, String> aliasToFullTableName;
	
	// Hive Task
	private List<HiveTaskInfo> hiveTaskInfos;

	// Stage graph info
	private boolean isRoot; 
	private List<String>  dependentChildren;  // stages depending on this stage's output
	
	private List<String>  conditionalChildren; // stages which are part of this conditional stage
	
	private List<String>  backupChildren; // backup stages of this stage. Currently it has at most one stage.
	
	// timing
	private long startTime;
	private long endTime;
	
	// recommendation
	private String recommendation;
	
	private List<HiveColumnInfo> partitionColumns;
	
	// 	join info from its operators
	private String joinType; // null if doesn't contain a join
	private List<HiveColumnInfo> joinKeys; // empty if no join
	
	private List<HiveFilterInfo> constFilters; // constant filters that can be used for suggesting partitioning. 
	
	public HiveStageInfo(){
		dependentChildren = new ArrayList<String>();
		conditionalChildren = new ArrayList<String>();
		backupChildren = new ArrayList<String>();
		inputPaths = new ArrayList<String>();
		aliasToPath = new HashMap<String, String>();
		
		hiveTaskInfos = new ArrayList<HiveTaskInfo>();
		aliasToFullTableName = new HashMap<String, String>();
		
		partitionColumns = new ArrayList<HiveColumnInfo>();
		joinType = null;
		joinKeys = new ArrayList<HiveColumnInfo>();
		constFilters = new ArrayList<HiveFilterInfo>();
	}
	
	public boolean hasMapAggr() {
		for (HiveTaskInfo task : hiveTaskInfos) {
			if (!task.getTaskType().equals(HiveTaskInfo.MAP_TYPE)) {
				continue;	// not a map task
			}
			for (HiveOperatorInfo operator : task.getHiveOperatorInfos()) {
				if (operator.containsGroupBy()) {
					return true;
				}
			}
		}
		return false;
	}
	
	public boolean hasMapJoin() {
		for (HiveTaskInfo task : hiveTaskInfos) {
			if (!task.getTaskType().equals(HiveTaskInfo.MAP_TYPE)) {
				continue;	// not a map task
			}
			for (HiveOperatorInfo operator : task.getHiveOperatorInfos()) {
				if (operator.containsMapjoin()) {
					return true;
				}
			}
		}
		return false;
	}
	
	public String getHadoopJobId() {
		return hadoopJobId;
	}

	public void setHadoopJobId(String hadoopJobId) {
		this.hadoopJobId = hadoopJobId;
	}

	public String getStageId() {
		return stageId;
	}

	public void setStageId(String stageId) {
		this.stageId = stageId;
	}

	public String getStageType() {
		return stageType;
	}

	public void setStageType(String stageType) {
		this.stageType = stageType;
	}

	public List<String> getInputPaths() {
		return inputPaths;
	}

	public void setInputPaths(List<String> inputPaths) {
		this.inputPaths = inputPaths;
	}
	
	public Map<String, String> getAliasToPath() {
		return aliasToPath;
	}

	public void setAliasToPath(Map<String, String> aliasToPath) {
		this.aliasToPath = aliasToPath;
	}

	public Map<String, String> getAliasToFullTableName() {
		return aliasToFullTableName;
	}

	public void setAliasToFullTableName(Map<String, String> aliasToFullTableName) {
		this.aliasToFullTableName = aliasToFullTableName;
	}

	public List<HiveColumnInfo> getPartitionColumns() {
		return partitionColumns;
	}

	public void setPartitionColumns(List<HiveColumnInfo> partitionColumns) {
		this.partitionColumns = partitionColumns;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public List<HiveColumnInfo> getJoinKeys() {
		return joinKeys;
	}

	public void setJoinKeys(List<HiveColumnInfo> joinKeys) {
		this.joinKeys = joinKeys;
	}

	/**
	 * Get the filters with constant values. This can be used to suggest partition
	 * scheme. We put the info in stage level as we might need stage/job info to get
	 * the filter selectivity later. 
	 * @return the filters with constant values
	 */
	public List<HiveFilterInfo> getConstFilters() {
		return constFilters;
	}

	public void setConstFilters(List<HiveFilterInfo> constFilters) {
		this.constFilters = constFilters;
	}

	public boolean isRoot() {
		return isRoot;
	}

	public void setRoot(boolean isRoot) {
		this.isRoot = isRoot;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public String getRecommendation() {
		return recommendation;
	}

	public void setRecommendation(String recommendation) {
		this.recommendation = recommendation;
	}

	public List<HiveTaskInfo> getHiveTaskInfos() {
		return hiveTaskInfos;
	}

	public void setHiveTaskInfos(List<HiveTaskInfo> hiveTaskInfos) {
		this.hiveTaskInfos = hiveTaskInfos;
	}
	
	public List<String> getDependentChildren() {
		return dependentChildren;
	}

	public void setDependentChildren(List<String> dependentChildren) {
		this.dependentChildren = dependentChildren;
	}

	public List<String> getConditionalChildren() {
		return conditionalChildren;
	}

	public void setConditionalChildren(List<String> conditionalChildren) {
		this.conditionalChildren = conditionalChildren;
	}

	public List<String> getBackupChildren() {
		return backupChildren;
	}

	public void setBackupChildren(List<String> backupChildren) {
		this.backupChildren = backupChildren;
	}
	
	
}
