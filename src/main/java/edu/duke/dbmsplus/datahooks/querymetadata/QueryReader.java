package edu.duke.dbmsplus.datahooks.querymetadata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;

import edu.duke.dbmsplus.datahooks.Connection.JDBCConnector;
import edu.duke.dbmsplus.datahooks.conf.HiveServerCredentials;
import edu.duke.dbmsplus.datahooks.conf.StarfishDatabaseCredentials;
import edu.duke.dbmsplus.datahooks.semantic.Hook;

/**
 * Reads all HiveQL queries from starfish database one-by-one,
 * prepends each with EXPLAIN,
 * runs it on Hive so that hooks could do the analysis.
 * @author mkunjir
 *
 */
public class QueryReader {

	//TODO: read following parameters from a config file
	// complete path of jar of this project
	static String JAR_PATH = "/home/mkunjir/SQLAnalyzer.jar";
	// name of starfish table storing all hive queries
	static String HIVE_QUERIES_TABLE = "hive_queries";
	// name of field in HIVE_QUERIES_TABLE that stores query string
	static String QUERY_STRING_FIELD = "query_string";

	String extractHiveQueriesStmt = "";
	Connection dbConnection = null;
	Statement dbStmt = null;
	Connection hiveConnection = null;
	Statement hiveStmt = null;
	
	//counters
	AtomicLong numDDLQueries = new AtomicLong();
	AtomicLong numExplainQueries = new AtomicLong();
	AtomicLong numRetrievalQueries = new AtomicLong();

	public QueryReader(String startDate, String endDate) {
		extractHiveQueriesStmt = "SELECT " + QUERY_STRING_FIELD 
				+ " FROM hive_queries where query_string!=\"use default\""
				+ " AND start_time>=\"" + startDate  + "\""
				+ " AND start_time<=\"" + endDate + "\"";
		connectToStarfishDB();
		setUpHiveClient();
	}

	/**
	 * Hive 0.12 doesn't keep added jars in session, 
	 * so the queries depending on the jar (i.e. all the queries) fail.
	 * Can't use this until Hive is patched.
	 */
	private void setUpHiveClient() {
		hiveConnection = JDBCConnector.connectHive(
				HiveServerCredentials.CONNECTION_STRING,
				HiveServerCredentials.USERNAME,
				HiveServerCredentials.PASSWORD);
		try {
			hiveStmt = hiveConnection.createStatement();
			hiveStmt.execute("add jar " + JAR_PATH);
			hiveStmt.execute("set hive.exec.driver.run.hooks=" + Hook.class.getName());
			hiveStmt.execute("set hive.semantic.analyzer.hook=" + Hook.class.getName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void connectToStarfishDB() {
		dbConnection = JDBCConnector.connectMySQL(
				StarfishDatabaseCredentials.CONNECTION_STRING, 
				StarfishDatabaseCredentials.USERNAME,
				StarfishDatabaseCredentials.PASSWORD);
		try {
			dbStmt = dbConnection.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * return true if the query is to be pruned
	 * @param query
	 */
	private boolean pruneQuery(String query) {
		if(!StringUtils.containsIgnoreCase(query, "select")) {
			numDDLQueries.incrementAndGet();
			return true;
		}
		if(StringUtils.startsWithIgnoreCase(query, "EXPLAIN") || 
				StringUtils.startsWithIgnoreCase(query, "\nEXPLAIN") || 
				StringUtils.startsWithIgnoreCase(query, "\n\nEXPLAIN")) {
			numExplainQueries.incrementAndGet();
			return true;
		}
		//temporarily ignoring certain tables
		if(!StringUtils.containsIgnoreCase(query, "impressions") &&
				!StringUtils.containsIgnoreCase(query, "rtbids") && 
				!StringUtils.containsIgnoreCase(query, "actions")) {
			return true;
		}
		numRetrievalQueries.incrementAndGet();
		return false;
	}

	/**
	 * Read historical queries from starfish database.
	 */
	public void readQueries() {
		ResultSet result = null;
		try {
			result = dbStmt.executeQuery(extractHiveQueriesStmt);
			while(result.next()) {
				String query = result.getString(1);
//				System.out.println("Query from starfish: " + query);
				// pruning any ddl queries
				//FIXME: including queries that have "select" as part of some string and not as a clause
				if(!pruneQuery(query)) {
					runHiveQuery(query);
				}
			}
			printCounts();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if(result!=null) {
					result.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void printCounts() {
		System.out.println("*Total DDL queries seen: " + numDDLQueries);
		System.out.println("*Total explain queries seen: " + numExplainQueries);
		System.out.println("*Total data retrieval queries seen: " + numRetrievalQueries);
	}

	/**
	 * Temporary method to run query on Hive
	 * TODO: remove once Hive is patched to make hiveserver work with custom jars
	 * @param query
	 */
	private void runQueryInClient(String query) {
		StringBuffer sb = new StringBuffer();
//		sb.append("hive -e \"");
		sb.append("\"");
		sb.append("add jar " + JAR_PATH + ";");
		sb.append("set hive.exec.driver.run.hooks=" + Hook.class.getName() + ";");
		sb.append("set hive.semantic.analyzer.hook=" + Hook.class.getName() + ";");
		sb.append(query);
		sb.append("\"");
		try {
			Process p = Runtime.getRuntime().exec(new String[]{"hive", "-e", sb.toString()});
//			System.out.println("running query: " + sb.toString());
			BufferedReader reader = 
                    new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedWriter writer = 
					new BufferedWriter(new FileWriter("op", true));

			String line = "";			
            while ((line = reader.readLine())!= null) {
            	writer.append(line + "\n");
            }
			p.waitFor();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * prepends query with 'EXPLAIN' and runs on hive
	 * @param query
	 */
	private void runHiveQuery(String query) {
		String explainQuery = "EXPLAIN " + query;
		try {
//			runQueryInClient(explainQuery);
			hiveStmt.execute(explainQuery);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
