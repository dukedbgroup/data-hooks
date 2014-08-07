package edu.duke.dbmsplus.datahooks.execution.pojo;

import java.util.ArrayList;
import java.util.List;

public class HiveOperatorInfo {

	private String operatorId;
	private String operatorType;
	private String comment;
	
	private List<HiveOperatorInfo> childHiveOperatorInfos;
	
	public static String JOIN_TYPE = "JOIN";
	public static String SMB_JOIN = "SMBMAPJOIN";

	public static String MAPJOIN_TYPE = "MAPJOIN";
	public static final String GROUPBY_TYPE = "GROUPBY";
	
	public boolean containsGroupBy() {
		if (operatorType.equals(GROUPBY_TYPE)) {
			return true;
		} else if (childHiveOperatorInfos != null) {
			for (HiveOperatorInfo child : childHiveOperatorInfos) {
				if (child.containsGroupBy()) {
					return true;
				}
			}
		}
		return false;
	}
	
	public boolean containsMapjoin() {
		if (operatorType.equals(MAPJOIN_TYPE)) {
			return true;
		} else if (childHiveOperatorInfos != null) {
			for (HiveOperatorInfo child : childHiveOperatorInfos) {
				if (child.containsMapjoin()) {
					return true;
				}
			}
		}
		return false;
	}
	
	public HiveOperatorInfo(){
		childHiveOperatorInfos = new ArrayList<HiveOperatorInfo>();
	}

	public String getOperatorId() {
		return operatorId;
	}

	public void setOperatorId(String operatorId) {
		this.operatorId = operatorId;
	}

	public String getOperatorType() {
		return operatorType;
	}

	public void setOperatorType(String operatorType) {
		this.operatorType = operatorType;
	}

	public List<HiveOperatorInfo> getChildHiveOperatorInfos() {
		return childHiveOperatorInfos;
	}

	public void setChildHiveOperatorInfos(List<HiveOperatorInfo> childHiveOperatorInfos) {
		this.childHiveOperatorInfos = childHiveOperatorInfos;
	}

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}
}
