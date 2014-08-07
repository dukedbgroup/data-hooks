package edu.duke.dbmsplus.datahooks.querymetadata;

/**
 * @author mkunjir
 *
 */
public class JoinAttribute {
	Long queryId;
	String tabName, colName;
	Boolean isArray = false, isDimLookup = false;
	Integer arrayIndex;
	String dimTabName;

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
	 * @return the isArray
	 */
	public Boolean isArray() {
		return isArray;
	}

	/**
	 * @param isArray the isArray to set
	 */
	public void setArray(Boolean isArray) {
		this.isArray = isArray;
	}

	/**
	 * @return the isDimLookup
	 */
	public Boolean isDimLookup() {
		return isDimLookup;
	}

	/**
	 * @param isDimLookup the isDimLookup to set
	 */
	public void setDimLookup(Boolean isDimLookup) {
		this.isDimLookup = isDimLookup;
	}

	/**
	 * @return the arrayIndex
	 */
	public Integer getArrayIndex() {
		return arrayIndex;
	}

	/**
	 * @param arrayIndex the arrayIndex to set
	 */
	public void setArrayIndex(Integer arrayIndex) {
		this.arrayIndex = arrayIndex;
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
