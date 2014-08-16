/**
 * 
 */
package edu.duke.dbmsplus.datahooks.conf;

import java.util.HashMap;
import java.util.Map;

import edu.duke.dbmsplus.datahooks.execution.profile.Component;
import edu.duke.dbmsplus.datahooks.execution.profile.Query;
import edu.duke.dbmsplus.datahooks.execution.profile.Workflow;


/**
 * table names and schema
 * @author mayuresh
 */
public class ProfileTables {
	public static final String WORKFLOW_TABLE = "workflows";
	public static final String COMPONENT_TABLE = "components";
	public static final String QUERY_TABLE = "executedQueries";
	
	// The string has to match exactly with the name of 'wid' field in WORKFLOW_TABLE
	public static String WORKFLOW_ID = "wid";
	
	@SuppressWarnings("rawtypes")
	static Map<String, Class> TABSCHEMAMAP = new HashMap<String, Class>();
	static {
		TABSCHEMAMAP.put(WORKFLOW_TABLE, Workflow.class);
		TABSCHEMAMAP.put(COMPONENT_TABLE, Component.class);
		TABSCHEMAMAP.put(QUERY_TABLE, Query.class);
	}
	
	@SuppressWarnings("rawtypes")
	public static Map<String, Class> getTabSchemaMap() {
		return TABSCHEMAMAP;
	}
}
