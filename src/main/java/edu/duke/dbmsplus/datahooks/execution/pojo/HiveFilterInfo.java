package edu.duke.dbmsplus.datahooks.execution.pojo;

public class HiveFilterInfo extends HiveColumnInfo {
	private String expr;

	public HiveFilterInfo(){
		// empty
	}
	
	public String getExpr() {
		return expr;
	}

	public void setExpr(String expr) {
		this.expr = expr;
	}
	public String toString() {
		return expr;
	}
}
