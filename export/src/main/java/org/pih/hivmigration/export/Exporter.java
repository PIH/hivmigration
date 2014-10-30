package org.pih.hivmigration.export;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class Exporter {

	//***** SINGLETON SUPPORT *****

	private static Exporter exporter;
	private Exporter() {}

	//***** INSTANCE VARIABLES *****
	private Configuration configuration;

	//***** PUBLIC *****

	public static Exporter initialize(Configuration configuration) {
		exporter = new Exporter();
		exporter.configuration = configuration;
		return exporter;
	}

	public <T> T executeQuery(String sql, ResultSetHandler<T> handler, Object...params) {
		Connection connection = null;
		try {
			QueryRunner runner = new QueryRunner() {
				protected PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException {
					PreparedStatement ps = super.prepareStatement(conn, sql);
					//ps.setFetchSize(Integer.MIN_VALUE);
					return ps;
				}
			};
			connection = openConnection();
			T result =  runner.query(connection, sql, handler, params);
			return result;
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to execute query: " + sql, e);
		}
		finally {
			DbUtils.closeQuietly(connection);
		}
	}

	public <T> T uniqueResult(String sql, Class<T> type) {
		return executeQuery(sql, new ScalarHandler<T>());
	}

	public <T> List<T> listResult(String sql, Class<T> type) {
		return executeQuery(sql, new ColumnListHandler<T>());
	}

	public List<Map<String, Object>> tableResult(String sql) {
		return executeQuery(sql, new MapListHandler());
	}

	public List<String> getAllTables() {
		String allTableQuery = "select distinct(upper(table_name)) from user_tab_columns order by upper(table_name) asc";
		return listResult(allTableQuery, String.class);
	}

	private Connection openConnection() {
		try {
			DatabaseCredentials credentials = configuration.getDatabaseCredentials();
			DbUtils.loadDriver(credentials.getDriver());
			return DriverManager.getConnection(credentials.getUrl(), credentials.getUser(), credentials.getPassword());
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Error retrieving connection to the database", e);
		}
	}

	public Configuration getConfiguration() {
		return configuration;
	}
}
