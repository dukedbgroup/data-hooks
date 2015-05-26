package edu.duke.dbmsplus.datahooks.conf;

/**
 * TODO: read configuration from config files
 * @author mkunjir
 *
 */
public class MetadataDatabaseCredentials {

	/**
	 * use these keys to set hive configuration parameters which would then 
	 * be accessed by hooks
	 */
	public static final String CONNECTION_STRING_KEY = "thoth.metadatadb.connection";
	public static final String USERNAME_KEY = "thoth.metadatadb.username";
	public static final String PASSWORD_KEY = "thoth.metadatadb.password";

	public static volatile String CONNECTION_STRING = "jdbc:mysql://localhost:3306/thoth";
	public static volatile String USERNAME = "thoth";
	public static volatile String PASSWORD = "thoth";	

}
