package edu.duke.dbmsplus.datahooks.querymetadata;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import edu.duke.dbmsplus.datahooks.Connection.JDBCConnector;
import edu.duke.dbmsplus.datahooks.conf.MetadataDatabaseCredentials;
import edu.duke.dbmsplus.datahooks.conf.QueryMetadataTables;

/**
 * FIXME: This class has to be in same package as all query metadata objects. 
 * The restriction comes from use of reflection.
 * @author mkunjir
 *
 */
public class QueryMetadataWriter {
	
	Connection dbConnection;
	Statement stmt = null;
	//FIXME: Remove this after testing. For testing parsing without writing to db, use "No write". 
	//Any other value would write to db.
	String mode = "write";

	public QueryMetadataWriter() {
		if("No write".equals(mode)) {
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
	 * Adds given record to 'queries' table
	 * @param query
	 */
	public void addQuery(Query query) {
//		System.out.println("Adding query: " + query.getQueryString());
		addToTable(query, QueryMetadataTables.QUERY_TABLE);
	}

	/**
	 * Adds given record to 'projections' table
	 * @param projection
	 */
	public void addProjection(Projection projection) {
		System.out.println("Adding selection: " + projection.getTabName() + ":" + projection.getColName() 
				+ " agg:" + projection.getAgg() + " indexed:" + projection.isArray() + " dim:" + projection.isDimLookup());
		addToTable(projection, QueryMetadataTables.PROJECTION_TABLE);
	}

	/**
	 * Adds given record to 'filters' table
	 * @param filter
	 */
	public void addFilter(Filter filter) {
		System.out.println("Adding filter: " + filter.getTabName() + ":" + filter.getColName() 
				+ " indexed:" + filter.isArray() + " dim:" + filter.isDimLookup() 
				+ " operator:" + filter.getOperator() + " value: " + filter.getValue());
		addToTable(filter, QueryMetadataTables.FILTER_TABLE);
	}
	
	/**
	 * Adds given record to 'join attributes' table
	 * @param join
	 */
	public void addJoinAttribute(JoinAttribute join) {
		System.out.println("Adding join attribute: " + join.getTabName() + ":" + join.getColName());
		addToTable(join, QueryMetadataTables.JOIN_TABLE);
	}

	/**
	 * Adds given record to 'grouping attributes' table
	 * @param group
	 */
	public void addGroupingAttribute(GroupingAttribute group) {
		System.out.println("Adding grouping attribute: " + group.getTabName() + ":" + group.getColName());
		addToTable(group, QueryMetadataTables.GROUPING_TABLE);
	}

	/**
	 * Adds given dataset to 'datasets' table
	 * @param ds
	 */
	public void addDataset(Dataset ds) {
		System.out.println("Adding dataset: " + ds.getTabName());
		addToTable(ds, QueryMetadataTables.DATASET_TABLE);
	}

	/**
	 * Adds a row to given table
	 * @param row
	 * @param tabName
	 */
	private void addToTable(Object row, String tabName) {
		if("No write".equals(mode)) {
			return;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO " + tabName + " VALUES (");
		for(Field field: QueryMetadataTables.getTabSchemaMap().get(tabName).getDeclaredFields()) {
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
		}
	}
	
	/**
	 * Creates all query metadata tables if they don't exist already
	 */
	@SuppressWarnings("rawtypes")
	public void createQueryMetadataTables() {
		Map<String, Class> tabToSchema = QueryMetadataTables.getTabSchemaMap();
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
	 * Fetches highest query ID from 'queries' table
	 * If table doesn't exist, creates all the tables and returns 0
	 * @return
	 */
	public synchronized Long fetchLastQueryId() {
		if("No write".equals(mode)) {
			return 0L;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT MAX(" + QueryMetadataTables.QUERY_ID + ") FROM ");
		sb.append(QueryMetadataTables.QUERY_TABLE);
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
			createQueryMetadataTables();
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

}
