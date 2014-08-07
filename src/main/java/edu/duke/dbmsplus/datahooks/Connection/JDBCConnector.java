/**
 * 
 */
package edu.duke.dbmsplus.datahooks.Connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author mkunjir
 *
 */
public class JDBCConnector {
	
	private static String MySQLDriver = "com.mysql.jdbc.Driver";
	private static String HiveDriver = "org.apache.hadoop.hive.jdbc.HiveDriver";

	private static Connection connect(String connectionString, String userName, 
			String password, String driverClass) {

		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException e) {
			System.out.println(driverClass + " is missing.");
			e.printStackTrace();
			return null;
		}

		Connection connection = null;

		try {
			connection = DriverManager.getConnection(connectionString, userName, password);

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}

		return connection;
	}
	
	public static Connection connectMySQL(String connectionString, String userName, 
			String password) {
		return connect(connectionString, userName, password, MySQLDriver);
	}
	
	public static Connection connectHive(String connectionString, String userName,
			String password) {
		return connect(connectionString, userName, password, HiveDriver);
	}
}

