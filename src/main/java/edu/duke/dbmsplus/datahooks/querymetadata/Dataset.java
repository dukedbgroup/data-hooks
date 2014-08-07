package edu.duke.dbmsplus.datahooks.querymetadata;

/**
 * @author mkunjir
 *
 */
public class Dataset {
	Long queryId;
	String tabName;
	Boolean isTabSample = false;
	Integer selBuckets;
	Integer totalBuckets;
//	String bucketColName;
//	Boolean isArray = false;
//	Integer arrayIndex;
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
	 * @return the isTabSample
	 */
	public Boolean getIsTabSample() {
		return isTabSample;
	}
	/**
	 * @param isTabSample the isTabSample to set
	 */
	public void setIsTabSample(Boolean isTabSample) {
		this.isTabSample = isTabSample;
	}
	/**
	 * @return the selBuckets
	 */
	public Integer getSelBuckets() {
		return selBuckets;
	}
	/**
	 * @param selBuckets the selBuckets to set
	 */
	public void setSelBuckets(Integer selBuckets) {
		this.selBuckets = selBuckets;
	}
	/**
	 * @return the totalBuckets
	 */
	public Integer getTotalBuckets() {
		return totalBuckets;
	}
	/**
	 * @param totalBuckets the totalBuckets to set
	 */
	public void setTotalBuckets(Integer totalBuckets) {
		this.totalBuckets = totalBuckets;
	}
//	/**
//	 * @return the bucketColName
//	 */
//	public String getBucketColName() {
//		return bucketColName;
//	}
//	/**
//	 * @param bucketColName the bucketColName to set
//	 */
//	public void setBucketColName(String bucketColName) {
//		this.bucketColName = bucketColName;
//	}
//	/**
//	 * @return the isArray
//	 */
//	public Boolean getIsArray() {
//		return isArray;
//	}
//	/**
//	 * @param isArray the isArray to set
//	 */
//	public void setIsArray(Boolean isArray) {
//		this.isArray = isArray;
//	}
//	/**
//	 * @return the arrayIndex
//	 */
//	public Integer getArrayIndex() {
//		return arrayIndex;
//	}
//	/**
//	 * @param arrayIndex the arrayIndex to set
//	 */
//	public void setArrayIndex(Integer arrayIndex) {
//		this.arrayIndex = arrayIndex;
//	}
}
