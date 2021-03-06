/**
 * 
 */
package edu.duke.dbmsplus.datahooks.execution.profile;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import edu.duke.dbmsplus.datahooks.conf.MetadataDatabaseCredentials;
import edu.duke.dbmsplus.datahooks.conf.ProfileTables;
import edu.duke.dbmsplus.datahooks.connection.JDBCConnector;

/**
 * FIXME: This class has to be in same package as all profile objects. 
 * The restriction comes from use of reflection.
 * @author mayuresh
 *
 */
public class ProfileDataWriter {

	Connection dbConnection;
	Statement stmt = null;
	//FIXME: Remove this after testing. For testing parsing without writing to db, use "No write". 
	//Any other value would write to db.
	static final String MODE = "write";

	public ProfileDataWriter() {
		if("No write".equals(MODE)) {
			return;
		}
		// set up MySQL database connection
		dbConnection = JDBCConnector.connectMySQL(
				MetadataDatabaseCredentials.CONNECTION_STRING, 
				MetadataDatabaseCredentials.USERNAME, 
				MetadataDatabaseCredentials.PASSWORD);
		try {
			stmt = dbConnection.createStatement();
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Adds given record to 'workflows' table
	 * @param workflow
	 */
	public void addWorkflow(Workflow workflow) {
		System.out.println("Adding workflow: " + workflow.getName());
		addToTable(workflow, ProfileTables.WORKFLOW_TABLE);
	}

	/**
	 * Adds given record to 'components' table
	 * @param component
	 */
	public void addComponent(Component component) {
		System.out.println("Adding component: " + component.getName());
		addToTable(component, ProfileTables.COMPONENT_TABLE);
	}

	/**
	 * Adds given record to 'queries' table
	 * @param query
	 */
	public void addQuery(Query query) {
		System.out.println("Adding query: " + query.getQueryString());
		addToTable(query, ProfileTables.QUERY_TABLE);
	}

	public void addSparkStage(SparkStage stage) {
		System.out.println("Adding stage: " + stage.getStageId());
		addToTable(stage, ProfileTables.SPARK_STAGES_TABLE);
	}

	public void addHiveStage(HiveStage stage) {
		System.out.println("Adding stage: " + stage.getStageId());
		addToTable(stage, ProfileTables.HIVE_STAGES_TABLE);
	}

	/**
	 * Adds a row to given table
	 * @param row
	 * @param tabName
	 */
	private synchronized void addToTable(Object row, String tabName) {
		if("No write".equals(MODE)) {
			return;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO " + tabName + " VALUES (");
		for(Field field: ProfileTables.getTabSchemaMap().get(tabName).getDeclaredFields()) {
			try {
				if(String.class.getName().equals(field.getType().getName())) {
					sb.append("\"" + StringUtils.replace(field.get(row).toString(), "\"", "\\\"") + "\"");
				} else {
					sb.append(field.get(row));
				}
				sb.append(",");
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		sb.deleteCharAt(sb.length()-1);
		sb.append(")");
		try {
			//			System.out.println("Executing query: " + sb.toString());
			stmt.executeUpdate(sb.toString());
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Failing row " + sb.toString());
		}
	}

	/**
	 * Creates all query metadata tables if they don't exist already
	 */
	@SuppressWarnings("rawtypes")
	public void createProfileTables() {
		Map<String, Class> tabToSchema = ProfileTables.getTabSchemaMap();
		StringBuffer sb;
		for(String tabName: tabToSchema.keySet()) {
			sb = new StringBuffer();		
			sb.append("CREATE TABLE IF NOT EXISTS " + tabName + " (");
			for (Field field: tabToSchema.get(tabName).getDeclaredFields()) {
				sb.append(" " + field.getName() + " ");
				if(Integer.class.getName().equals(field.getType().getName())) {
					sb.append("INTEGER");
				} else if(Long.class.getName().equals(field.getType().getName())) {
					sb.append("BIGINT");
				} else if(Boolean.class.getName().equals(field.getType().getName())) {
					sb.append("BOOL");
				} else {
					sb.append("MEDIUMTEXT");
				}
				sb.append(",");
			}
			sb.deleteCharAt(sb.length()-1);	// removing last ','
			//FIXME: No primary key specification supported
			sb.append(")");
			System.out.println("Running query: " + sb.toString());
			try {
				stmt.executeUpdate(sb.toString());
				System.out.println("Created table: " + tabName);
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
	}

	/**
	 * Fetches highest wid from 'workflows' table
	 * If table doesn't exist, creates all the tables and returns 0
	 * @return
	 */
	public synchronized Long fetchLastWorkflowId() {
		if("No write".equals(MODE)) {
			return 0L;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT MAX(" + ProfileTables.WORKFLOW_ID + ") FROM ");
		sb.append(ProfileTables.WORKFLOW_TABLE);
		ResultSet rs = null;
		try {
			//			System.out.println("Running query: " + sb.toString());
			rs = stmt.executeQuery(sb.toString());
			if(rs.next()) {
				return rs.getLong(1);
			} else {
				return 0L;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			// probably table doesn't exist, create it
			createProfileTables();
			return 0L;
		} finally {
			try {
				if(rs!=null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Fetches highest cid from 'components' table
	 * given a wid
	 * @return
	 */
	private synchronized Long fetchLastComponentId(Long wid) {
		if("No write".equals(MODE)) {
			return 0L;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT MAX(" + ProfileTables.COMPONENT_ID + ") FROM ");
		sb.append(ProfileTables.COMPONENT_TABLE);
		sb.append(" WHERE " + ProfileTables.WORKFLOW_ID + "=" + wid);
		ResultSet rs = null;
		try {
			//			System.out.println("Running query: " + sb.toString());
			rs = stmt.executeQuery(sb.toString());
			if(rs.next()) {
				return rs.getLong(1);
			} else {
				return 0L;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return 0L;
		} finally {
			try {
				if(rs!=null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Fetches highest qid from 'queries' table
	 * given a wid and cid
	 * @return
	 */
	private synchronized Long fetchLastQueryId(Long wid, Long cid) {
		if("No write".equals(MODE)) {
			return 0L;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT MAX(" + ProfileTables.QUERY_ID + ") FROM ");
		sb.append(ProfileTables.QUERY_TABLE);
		sb.append(" WHERE " + ProfileTables.WORKFLOW_ID + "=" + wid);
		sb.append(" AND " + ProfileTables.COMPONENT_ID + "=" + cid);
		ResultSet rs = null;
		try {
			//			System.out.println("Running query: " + sb.toString());
			rs = stmt.executeQuery(sb.toString());
			if(rs.next()) {
				return rs.getLong(1);
			} else {
				return 0L;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return 0L;
		} finally {
			try {
				if(rs!=null) {
					rs.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * Used by Hive execution hook to find current query identifier
	 * @return
	 */
	public synchronized Long[] fetchCurrentQueryId() {
		Long lastWorkflowId = fetchLastWorkflowId();
		Long lastComponentId = fetchLastComponentId(lastWorkflowId+1);
		Long lastQueryId = fetchLastQueryId(lastWorkflowId+1, lastComponentId+1);
		return new Long[]{lastWorkflowId+1, lastComponentId+1, lastQueryId+1};
	}
}
