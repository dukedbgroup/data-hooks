package edu.duke.dbmsplus.datahooks.querymetadata;

/**
 * Object extracted from SQL describing one selected column
 * @author mkunjir
 *
 */
public class Projection {
	String tabName, colName;
	Long queryId;
	String agg;
	Boolean isArray = false, isDimLookup = false;
	Integer arrayIndex;
	String dimTabName;
	
	/**
	 * @return the tabName
	 */
	public String getTabName() {
		return tabName;
	}
	/**
	 * @param tabName the tabName to set
	 */
	public void setTabName(String tabName) {
		this.tabName = tabName;
	}
	/**
	 * @return the colName
	 */
	public String getColName() {
		return colName;
	}
	/**
	 * @param colName the colName to set
	 */
	public void setColName(String colName) {
		this.colName = colName;
	}
	/**
	 * @return the queryId
	 */
	public Long getQueryId() {
		return queryId;
	}
	/**
	 * @param queryId the queryId to set
	 */
	public void setQueryId(Long queryId) {
		this.queryId = queryId;
	}
	/**
	 * @return the agg
	 */
	public String getAgg() {
		return agg;
	}
	/**
	 * @param agg the agg to set
	 */
	public void setAgg(String agg) {
		this.agg = agg;
	}
	/**
	 * @return the isArray
	 */
	public boolean isArray() {
		return isArray;
	}
	/**
	 * @param isArray the isArray to set
	 */
	public void setArray(boolean isArray) {
		this.isArray = isArray;
	}
	/**
	 * @return the index
	 */
	public int getArrayIndex() {
		return arrayIndex;
	}
	/**
	 * @param index the index to set
	 */
	public void setArrayIndex(int index) {
		this.arrayIndex = index;
	}
	/**
	 * @return the isDimLookup
	 */
	public boolean isDimLookup() {
		return isDimLookup;
	}
	/**
	 * @param isDimLookup the isDimLookup to set
	 */
	public void setDimLookup(boolean isDimLookup) {
		this.isDimLookup = isDimLookup;
	}
	/**
	 * @return the dimTabName
	 */
	public String getDimTabName() {
		return dimTabName;
	}
	/**
	 * @param dimTabName the dimTabName to set
	 */
	public void setDimTabName(String dimTabName) {
		this.dimTabName = dimTabName;
	}
	
}
