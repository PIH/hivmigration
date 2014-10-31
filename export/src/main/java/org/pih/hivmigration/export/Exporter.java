package org.pih.hivmigration.export;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.pih.hivmigration.common.User;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Exporter {

	//***** SINGLETON SUPPORT *****

	private static Exporter exporter;
	private Exporter() {}

	//***** INSTANCE VARIABLES *****
	private Configuration configuration;
	private Connection connection;

	//***** PUBLIC *****

	public static Exporter initialize(Configuration configuration) {
		try {
			DatabaseCredentials credentials = configuration.getDatabaseCredentials();
			DbUtils.loadDriver(credentials.getDriver());
			exporter = new Exporter();
			exporter.configuration = configuration;
			exporter.connection = DriverManager.getConnection(credentials.getUrl(), credentials.getUser(), credentials.getPassword());
			return exporter;
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Error initializing exporter", e);
		}
	}

	public void destroy() {
		DbUtils.closeQuietly(connection);
		this.configuration = null;
		this.connection = null;
	}

	public <T> T executeQuery(String sql, ResultSetHandler<T> handler, Object...params) {
		try {
			QueryRunner runner = new QueryRunner();
			T result =  runner.query(connection, sql, handler, params);
			return result;
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to execute query: " + sql, e);
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
		String sql = "select distinct(upper(table_name)) from user_tab_columns order by upper(table_name) asc";
		return listResult(sql, String.class);
	}

	public List<TableColumn> getAllColumns(String table) {
		String sql = "select table_name as tableName, column_name as columnName, data_type as dataType, data_length as length, nullable from user_tab_columns where upper(table_name) = ?";
		return executeQuery(sql, new BeanListHandler<TableColumn>(TableColumn.class), table.toUpperCase());
	}
	public BigDecimal getNumberOfNonNullValues(String table, String column) {
		String sql = "select count(*) from " + table + " where " + column + " is not null";
		return uniqueResult(sql, BigDecimal.class);
	}

	public List<User> getUsers() {
		StringBuilder query = new StringBuilder();
		query.append("select	u.user_id as userId, p.email, n.first_names as firstName, n.last_name as lastName, u.password, u.salt ");
		query.append("from		users u, parties p, persons n ");
		query.append("where		u.user_id = p.party_id ");
		query.append("and		u.user_id = n.person_id ");
		return executeQuery(query.toString(), new BeanListHandler<User>(User.class, new BasicRowProcessor(new GenerousBeanProcessor())));
	}
}
