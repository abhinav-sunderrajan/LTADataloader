package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * The class responsible for handling database operations.
 * 
 */
public class DatabaseAccess {

	private Connection connect = null;
	private static final Logger LOGGER = Logger.getLogger(DatabaseAccess.class);
	private final static String DRIVER = "org.postgresql.Driver";
	private PreparedStatement blockExecute;
	private int counter = 0;
	private int batchSize = 100;

	/**
	 * Create and open a database connection using a properties file.
	 * 
	 * @param connectionProperties
	 */
	public DatabaseAccess(final Properties connectionProperties) {
		openDBConnection(connectionProperties);

	}

	/**
	 * Create and open a database connection
	 * 
	 * @param url
	 *            the database URL
	 * @param userName
	 *            the user name
	 * @param password
	 *            the password.
	 */
	public DatabaseAccess(String url, String userName, String password) {
		openDBConnection(url, userName, password);

	}

	/**
	 * 
	 * @param connectionProperties
	 * @return
	 */
	private void openDBConnection(Properties connectionProperties) {

		String url = connectionProperties.getProperty("database.url");
		String dbName = connectionProperties.getProperty("database.name");
		String userName = connectionProperties.getProperty("database.username");
		String password = connectionProperties.getProperty("database.password");
		try {
			Class.forName(DRIVER).newInstance();

			connect = (Connection) DriverManager.getConnection(url + dbName,
					userName, password);

		} catch (Exception e) {
			LOGGER.error(
					"Unable to connect to database. Please check the settings",
					e);
		}

	}

	/**
	 * Open a database connection that is user specific. Used to store the
	 * results of the simulation to the local database of the submittee.
	 * 
	 * @param url
	 * @param userName
	 * @param password
	 */
	private void openDBConnection(String url, String userName,
			String password) {
		try {
			Class.forName(DRIVER).newInstance();

			connect = (Connection) DriverManager.getConnection(url, userName,
					password);

		} catch (Exception e) {
			LOGGER.error(
					"Unable to connect to database. Please check the settings",
					e);
		}

	}

	/**
	 * Return the result set for the SELECT query.
	 * 
	 * @param queryString
	 * @return
	 */
	public ResultSet retrieveQueryResult(String queryString) {

		ResultSet resultSet = null;
		try {
			PreparedStatement preparedStatement = (PreparedStatement) connect
					.prepareStatement(queryString);
			resultSet = preparedStatement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();

		}
		return resultSet;
	}

	/**
	 * Return the result of a query as a query as a cursor. Suitable for large
	 * tables.
	 * 
	 * @param queryString
	 * @param cursorsize
	 * @return
	 */
	public ResultSet retrieveQueryResult(String queryString, int cursorsize) {

		ResultSet resultSet = null;
		try {
			connect.setAutoCommit(false);
			PreparedStatement preparedStatement = (PreparedStatement) connect
					.prepareStatement(queryString);
			preparedStatement.setFetchSize(cursorsize);
			resultSet = preparedStatement.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();

		}
		return resultSet;
	}

	/**
	 * Call for DDL statements i.e. DELETE, UPDATE and INSERT
	 * 
	 * @param queryString
	 */
	public void executeUpdate(String queryString) {
		try {
			PreparedStatement preparedStatement = (PreparedStatement) connect
					.prepareStatement(queryString);
			preparedStatement.execute();

		} catch (SQLException e) {
			System.out.println(queryString);
			LOGGER.error("Error while executing the DDL statement", e);

		}
	}

	public PreparedStatement getPreparedStatement(String queryString) throws SQLException {
		PreparedStatement preparedStatement = (PreparedStatement) connect
				.prepareStatement(queryString);
		return preparedStatement;

	}

	/**
	 * Set up a prepared statement for block execute
	 * 
	 * @param batchSize
	 *            the size of batch for performing batch DDL/DML executes.
	 * @param sql
	 *            the statement to be repeatedly executed.
	 * @throws SQLException
	 */
	public void setBlockExecutePS(String sql, int batchSize)
			throws SQLException {
		blockExecute = connect.prepareStatement(sql);
		this.batchSize = batchSize;
	}

	/**
	 * Execute the block insert.
	 * 
	 * @param vals
	 * @throws SQLException
	 */
	public void executeBlockUpdate(Object... vals) throws SQLException {

		for (int i = 0; i < vals.length; i++) {
			blockExecute.setObject(i + 1, vals[i]);
		}

		blockExecute.addBatch();
		if ((counter + 1) % batchSize == 0) {
			blockExecute.executeBatch();
			blockExecute.clearBatch();
		}
		counter++;

	}

	/**
	 * Close the database connection.
	 * 
	 * @throws SQLException
	 */

	public void closeConnection() throws SQLException {
		connect.close();
	}

	/**
	 * @return the blockExecute
	 */
	public PreparedStatement getBlockExecute() {
		return blockExecute;
	}

}
