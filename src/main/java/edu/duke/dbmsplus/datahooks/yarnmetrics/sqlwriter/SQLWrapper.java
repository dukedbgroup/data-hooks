package edu.duke.dbmsplus.datahooks.yarnmetrics.sqlwriter;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import edu.duke.dbmsplus.datahooks.connection.JDBCConnector;
import edu.duke.dbmsplus.datahooks.conf.MetadataDatabaseCredentials;

/**
 * Write Yarn metrics to MySQL database.
 * 
 * @author rahulswaminathan, Xiaodan
 *
 */
public class SQLWrapper {

    Connection conn = null;

    /**
     * Use JDBCConector to connect to MySQL
     * 
     * @author rahulswaminathan, Xiaodan
     */
    public SQLWrapper() {
        conn =
                JDBCConnector.connectMySQL(MetadataDatabaseCredentials.CONNECTION_STRING,
                        MetadataDatabaseCredentials.USERNAME, MetadataDatabaseCredentials.PASSWORD);
    }

    /**
     * Removes a row from the specified table.
     * 
     * @param table Table to remove row from.
     * @param tag Tag value of the row to be removed
     * @return True if row removed, false otherwise.
     */
    public boolean removeRow(String table, String tag) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("DELETE from " + table + " WHERE tag='" + tag + "'");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }

        return true;
    }

    /**
     * create tables for statsD output.
     * 
     * @param table
     * @return
     */
    public boolean createTable(String table) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + table
                    + "(tag varchar(255), value varchar(255))");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }

        return true;
    }


    /**
     * Updates a value in a given table, assuming that the table has two columns, the first of which
     * is a string, and the second is an integer.
     * 
     * @param table Table to be updated
     * @param tag Tag to be updated.
     * @param newValue New value of the given tag.
     * @return True on success, false otherwise.
     */
    public boolean updateValue(String table, String tag, int newValue) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("UPDATE " + table + " SET value=" + newValue + " WHERE tag='"
                    + tag + "'");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }

        return true;
    }

    /**
     * Updates a value in a given table, assuming that the table has two columns, the first of which
     * is a string, and the second is an string.
     *
     * @param table Table to be updated
     * @param tag Tag to be updated.
     * @param newValue New value of the given tag.
     * @return True on success, false otherwise.
     */
    public boolean updateValue(String table, String tag, String newValue) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("UPDATE " + table + " SET value='" + newValue + "' WHERE tag='"
                    + tag + "'");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }

        return true;
    }

    /**
     * Inserts a value into a given table, assuming that the table has two columns, the first of
     * which is a string, and the second is an integer.
     * 
     * @param table Table to be edited.
     * @param tag First column of a table
     * @param value Second column of a table
     * @return True on success, false otherwise.
     */
    public boolean insertIntoTable(String table, String tag, int value) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("INSERT INTO " + table + " " + "VALUES ('" + tag + "', "
                    + value + ")");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }

        return true;
    }


    /**
     * Inserts a value into a given table, assuming that the table has two columns, the first of
     * which is a string, and the second is an string.
     *
     * @param table Table to be edited.
     * @param tag First column of a table
     * @param value Second column of a table
     * @return True on success, false otherwise.
     */
    public boolean insertIntoTable(String table, String tag, String value) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("INSERT INTO " + table + " " + "VALUES ('" + tag + "', '"
                    + value + "')");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }

        return true;
    }

    /**
     * Create Cluster metrics Table.
     * 
     * @return True on success, otherwise false
     */
    public boolean createClusterTable() {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + "cluster_metrics"
                    + "(MetricsName varchar(255), RecordTime bigint(20), Value varchar(255))");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }

    /**
     * Write record to cluster metrics table
     * 
     * @param metricsName Name of the metrics of this record
     * @param time the time of getting this record
     * @param value the value of the metrics.
     * @return True on success, otherwise false
     */
    public boolean writeClusterTable(String metricsName, long time, String value) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("INSERT INTO cluster_metrics VALUES" + "('" + metricsName
                    + "', '" + BigInteger.valueOf(time) + "', '" + value + "')");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }

    /**
     * Create scheduler metrics table.
     * 
     * @return true on success, false otherwise.
     */
    public boolean createSchedulerTable() {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement
                    .executeUpdate("CREATE TABLE IF NOT EXISTS "
                            + "scheduler_metrics"
                            + "(QueueName varchar(255), MetricsName varchar(255), RecordTime bigint(20), Value varchar(255))");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }

    /**
     * Write one record to the scheduler metrics table
     * 
     * @param queueName The name of the queue
     * @param metricsName Name of the metrics of this record
     * @param time The time of getting this record
     * @param value Value of the metrics
     * @return true on success, false otherwise
     */
    public boolean writeSchedulerTable(String queueName, String metricsName, long time, String value) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("INSERT INTO scheduler_metrics VALUES" + "('" + queueName
                    + "','" + metricsName + "', '" + BigInteger.valueOf(time) + "', '" + value
                    + "')");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }

    /**
     * Create the applications metrics table.
     * 
     * @return true on success, false otherwise.
     */
    public boolean createAppsTable() {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement
                    .executeUpdate("CREATE TABLE IF NOT EXISTS "
                            + "applications_metrics"
                            + "(AppId varchar(255), MetricsName varchar(255), RecordTime bigint(20), Value varchar(255))");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }

    /**
     * Write one record to the application metrics table
     * 
     * @param AppsId The id of applications
     * @param metricsName Name of the metrics of this record
     * @param time The time of getting this record
     * @param value Value of the metrics
     * @return true on success, false otherwise
     */
    public boolean writeAppsTable(String appsId, String metricsName, long time, String value) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("INSERT INTO applications_metrics VALUES" + "('" + appsId
                    + "','" + metricsName + "', '" + BigInteger.valueOf(time) + "', '" + value
                    + "')");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }
    
    public boolean createNodesAppsTable() {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement
                    .executeUpdate("CREATE TABLE IF NOT EXISTS "
                            + "nodes_apps_metrics"
                            + "(nodeAddress varchar(255), appId varchar(255), MetricsName varchar(255), RecordTime bigint(20), Value varchar(255))");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }
    
    public boolean writeNodesAppsTable(String nodeAddress, String appId, String metricsName, long time, String value) {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement.executeUpdate("INSERT INTO nodes_apps_metrics VALUES" + "('" + nodeAddress + "','" + appId
                    + "','" + metricsName + "', '" + BigInteger.valueOf(time) + "', '" + value
                    + "')");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }
    
    public boolean createNodesContainersTable() {
        Statement statement;
        try {
            statement = conn.createStatement();
            statement
                    .executeUpdate("CREATE TABLE IF NOT EXISTS "
                            + "nodes_containers_metrics"
                            + "(nodeAddress varchar(255), containerId varchar(255), MetricsName varchar(255), RecordTime bigint(20), Value text)");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }
    
    public boolean writeNodesContainersTable(String nodeAddress, String containerId, String metricsName, long time, String value) {
        Statement statement;
        try {
            statement = conn.createStatement();
            if(value.length() >= 255) {
                value = value.substring(0, 255);
            }
            statement.executeUpdate("INSERT INTO nodes_containers_metrics VALUES" + "('" + nodeAddress + "','" + containerId
                    + "','" + metricsName + "', '" + BigInteger.valueOf(time) + "', '" + value
                    + "')");
        } catch (SQLException e) {
            printSQLInformation(e);
            return false;
        }
        return true;
    }

    /**
     * Prints the given table to the console. For test only
     * 
     * @param table Table to be printed.
     */
    public void printTableInformation(String table) {
        Statement statement;
        ResultSet rs;
        try {
            statement = conn.createStatement();
            rs = statement.executeQuery("SELECT * from " + table);

            ResultSetMetaData rsmd = rs.getMetaData();

            int numberOfColumns = rsmd.getColumnCount();

            System.out.println("TABLE: " + table);
            System.out.println("----------------");
            for (int i = 1; i <= numberOfColumns; i++) {
                if (i > 1)
                    System.out.print(" | ");
                String columnName = rsmd.getColumnName(i);
                System.out.print(columnName);
            }
            System.out.println("");
            System.out.println("----------------");

            while (rs.next()) {
                for (int i = 1; i <= numberOfColumns; i++) {
                    if (i > 1)
                        System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue);
                }
                System.out.println("");
            }
        } catch (SQLException e) {
            printSQLInformation(e);
        }

    }

    protected void printSQLInformation(SQLException ex) {
        System.out.println("SQLException: " + ex.getMessage());
        System.out.println("SQLState: " + ex.getSQLState());
        System.out.println("VendorError: " + ex.getErrorCode());
        ex.printStackTrace();
    }
}
