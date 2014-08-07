package edu.duke.dbmsplus.datahooks.execution.pojo;

public class HiveColumnInfo {
	private String tableAlias;
	private String fullTableName;
	private String columnName;
	
	public HiveColumnInfo(){
		// empty
	}
	
	public String getFullTableName() {
		return fullTableName;
	}

	public void setFullTableName(String fullTableName) {
		this.fullTableName = fullTableName;
	}

	public String getTableAlias() {
		return tableAlias;
	}

	public void setTableAlias(String tableAlias) {
		this.tableAlias = tableAlias;
	}

	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	
}
