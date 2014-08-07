package edu.duke.dbmsplus.datahooks.conf;

import java.util.HashMap;
import java.util.Map;

import edu.duke.dbmsplus.datahooks.querymetadata.Dataset;
import edu.duke.dbmsplus.datahooks.querymetadata.Filter;
import edu.duke.dbmsplus.datahooks.querymetadata.GroupingAttribute;
import edu.duke.dbmsplus.datahooks.querymetadata.JoinAttribute;
import edu.duke.dbmsplus.datahooks.querymetadata.Query;
import edu.duke.dbmsplus.datahooks.querymetadata.Projection;

/**
 * table names and schema
 * @author mkunjir
 *
 */
public class QueryMetadataTables {
	public static final String QUERY_TABLE = "queries";
	public static final String PROJECTION_TABLE = "projections";
	public static final String FILTER_TABLE = "filters";
	public static final String GROUPING_TABLE = "grouping_attributes";
	public static final String JOIN_TABLE = "join_attributes";
	public static final String DATASET_TABLE = "datasets";
	
	// The string has to match exactly with the name of 'queryID' field in QUERY_TABLE
	public static String QUERY_ID = "queryId";
	
	@SuppressWarnings("rawtypes")
	static Map<String, Class> TABSCHEMAMAP = new HashMap<String, Class>();
	static {
		TABSCHEMAMAP.put(QUERY_TABLE, Query.class);
		TABSCHEMAMAP.put(PROJECTION_TABLE, Projection.class);
		TABSCHEMAMAP.put(FILTER_TABLE, Filter.class);
		TABSCHEMAMAP.put(GROUPING_TABLE, GroupingAttribute.class);
		TABSCHEMAMAP.put(JOIN_TABLE, JoinAttribute.class);
		TABSCHEMAMAP.put(DATASET_TABLE, Dataset.class);
	}
	
	@SuppressWarnings("rawtypes")
	public static Map<String, Class> getTabSchemaMap() {
		return TABSCHEMAMAP;
	}
	
}
