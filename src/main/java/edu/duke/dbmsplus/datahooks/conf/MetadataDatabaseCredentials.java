package edu.duke.dbmsplus.datahooks.conf;

/**
 * TODO: read configuration from config files
 * @author mkunjir
 *
 */
public class MetadataDatabaseCredentials {
	public static String DB_NAME = "thoth";
	public static String CONNECTION_STRING = "jdbc:mysql://localhost:3306/" + DB_NAME;
	public static String USERNAME = "root";
	public static String PASSWORD = "database";	

//	public static String CONNECTION_STRING = "jdbc:mysql://127.0.0.1:3306/query_analysis";
//	public static String USERNAME = "root";
//	public static String PASSWORD = "database";	
}
