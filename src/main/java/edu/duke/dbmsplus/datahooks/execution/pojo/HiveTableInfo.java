package edu.duke.dbmsplus.datahooks.execution.pojo;

import java.util.List;

public class HiveTableInfo {
	public static String SORT_ASC = "ASC";
	public static String SORT_DSC = "DSC";

	private String database;
	private String baseTableName;  // the name without the database name, e.g. 'lineitem'
	private String fullTableName;  // the name with the database name, e.g. 'default.lineitem'

	private List<String> bucketCols;

	private int numBuckets;

	private List<String> partitionCols;

	// the sort column names
	private List<String> sortCols;
	// the sort properties, ASC or DSC, corresponding to each sort column
	private List<String> sortProps;

	private String path;
	
	public HiveTableInfo() {
		// empty
	}

	public String getBaseTableName() {
		return baseTableName;
	}

	public void setBaseTableName(String baseTableName) {
		this.baseTableName = baseTableName;
	}

	public String getFullTableName() {
		return fullTableName;
	}

	public void setFullTableName(String fullTableName) {
		this.fullTableName = fullTableName;
	}

	public List<String> getBucketCols() {
		return bucketCols;
	}

	public void setBucketCols(List<String> bucketCols) {
		this.bucketCols = bucketCols;
	}

	public int getNumBuckets() {
		return numBuckets;
	}

	public void setNumBuckets(int numBuckets) {
		this.numBuckets = numBuckets;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public List<String> getPartitionCols() {
		return partitionCols;
	}

	public void setPartitionCols(List<String> partitionCols) {
		this.partitionCols = partitionCols;
	}

	public List<String> getSortCols() {
		return sortCols;
	}

	public void setSortCols(List<String> sortCols) {
		this.sortCols = sortCols;
	}

	public List<String> getSortProps() {
		return sortProps;
	}

	public void setSortProps(List<String> sortProps) {
		this.sortProps = sortProps;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

}
