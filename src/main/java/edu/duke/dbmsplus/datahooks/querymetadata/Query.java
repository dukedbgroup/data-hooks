package edu.duke.dbmsplus.datahooks.querymetadata;

/**
 * @author mkunjir
 *
 */
public class Query {
	Long queryId;
	String queryString;

	/**
	 * @return the id
	 */
	public Long getQueryId() {
		return queryId;
	}

	/**
	 * @param id the id to set
	 */
	public void setQueryId(Long id) {
		this.queryId = id;
	}

	/**
	 * @return the queryString
	 */
	public String getQueryString() {
		return queryString;
	}

	/**
	 * @param queryString the queryString to set
	 */
	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}
}
