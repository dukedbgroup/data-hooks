package edu.duke.dbmsplus.datahooks.conf;

/**
 * TODO: read from a config file
 * @author mkunjir
 *
 */
public class HiveServerCredentials {
	public static volatile String CONNECTION_STRING = "jdbc:hive2://localhost:10000/default";
	public static volatile String USERNAME = "hive";
	public static volatile String PASSWORD = "";

}
