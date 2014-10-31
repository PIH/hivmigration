package org.pih.hivmigration.export;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DB {

	private static Connection connection;

	//***** PUBLIC *****

	public static void openConnection(DatabaseCredentials credentials) {
		if (connection != null) {
			throw new IllegalStateException("A connection is already established.  Cannot open an already opened connection.");
		}
		try {
			DbUtils.loadDriver(credentials.getDriver());
			connection = DriverManager.getConnection(credentials.getUrl(), credentials.getUser(), credentials.getPassword());
		}
		catch (Exception e) {
			throw new IllegalArgumentException("Error initializing connection to database", e);
		}
	}

	public static void closeConnection() {
		DbUtils.closeQuietly(connection);
		connection = null;
	}

	public static <T> T executeQuery(String sql, ResultSetHandler<T> handler, Object...params) {
		try {
			QueryRunner runner = new QueryRunner();
			T result =  runner.query(connection, sql, handler, params);
			return result;
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to execute query: " + sql, e);
		}
	}

	public static <T> T uniqueResult(String sql, Class<T> type) {
		return executeQuery(sql, new ScalarHandler<T>());
	}

	public static <T> List<T> listResult(String sql, Class<T> type) {
		return executeQuery(sql, new ColumnListHandler<T>());
	}

	public static List<Map<String, Object>> tableResult(String sql) {
		return executeQuery(sql, new MapListHandler());
	}

	public static List<String> getAllTables() {
		String sql = "select distinct(upper(table_name)) from user_tab_columns order by upper(table_name) asc";
		return listResult(sql, String.class);
	}

	public static List<TableColumn> getAllColumns(String table) {
		String sql = "select table_name as tableName, column_name as columnName, data_type as dataType, data_length as length, nullable from user_tab_columns where upper(table_name) = ?";
		return executeQuery(sql, new BeanListHandler<TableColumn>(TableColumn.class), table.toUpperCase());
	}

	public static TableColumn getColumn(String table, String column) {
		String sql = "select table_name as tableName, column_name as columnName, data_type as dataType, data_length as length, nullable from user_tab_columns where upper(table_name) = ? and upper(column_name) = ?";
		return executeQuery(sql, new BeanHandler<TableColumn>(TableColumn.class), table.toUpperCase(), column.toUpperCase());
	}

	public static int getNumberOfNonNullValues(String table, String column) {
		String sql = "select count(*) from " + table + " where " + column + " is not null";
		return uniqueResult(sql, BigDecimal.class).intValue();
	}

	public static int getNumberOfNullValues(String table, String column) {
		String sql = "select count(*) from " + table + " where " + column + " is null";
		return uniqueResult(sql, BigDecimal.class).intValue();
	}

	public static int getNumberOfDistinctNonNullValues(String table, String column) {
		String sql = "select count(distinct("+column+")) from " + table;
		return uniqueResult(sql, BigDecimal.class).intValue();
	}

	public static Map<Object, Integer> getValueBreakdown(String table, String column) {
		return getValueBreakdown(table, column, null);
	}

	public static Map<Object, Integer> getValueBreakdown(String table, String column, final Integer limitToNum) {
		String sql = "select " + column + " as value, count(*) as num from " + table + " group by " + column + " order by num desc";
		return executeQuery(sql, new ResultSetHandler<Map<Object, Integer>>() {
			public Map<Object, Integer> handle(ResultSet resultSet) throws SQLException {
				Map<Object, Integer> ret = new LinkedHashMap<Object, Integer>();
				while (resultSet.next() && (limitToNum == null || ret.size() < limitToNum)) {
					Object value = resultSet.getObject(1);
					BigDecimal num = resultSet.getBigDecimal(2);
					ret.put(value, num.intValue());
				}
				return ret;
			}
		});
	}

	public static Object getMinValue(String table, String column) {
		String sql = "select min("+column+") from " + table;
		return uniqueResult(sql, Object.class);
	}

	public static Object getMaxValue(String table, String column) {
		String sql = "select max("+column+") from " + table;
		return uniqueResult(sql, Object.class);
	}

	public static TableColumnBreakdown getColumnBreakdown(String table, String column) {
		TableColumnBreakdown b = new TableColumnBreakdown();
		b.setTableColumn(getColumn(table, column));
		b.setNumNotNullValues(getNumberOfNonNullValues(table, column));
		b.setNumNullValues(getNumberOfNullValues(table, column));
		b.setNumDistinctNonNullValues(getNumberOfDistinctNonNullValues(table, column));
		b.setMostFrequentValues(getValueBreakdown(table, column, 20));
		b.setMinValue(getMinValue(table, column));
		b.setMaxValue(getMaxValue(table, column));
		return b;
	}


	public static List<User> getUsers() {
		StringBuilder query = new StringBuilder();
		query.append("select	u.user_id as userId, p.email, n.first_names as firstName, n.last_name as lastName, u.password, u.salt ");
		query.append("from		users u, parties p, persons n ");
		query.append("where		u.user_id = p.party_id ");
		query.append("and		u.user_id = n.person_id ");
		return executeQuery(query.toString(), new BeanListHandler<User>(User.class, new BasicRowProcessor(new GenerousBeanProcessor())));
	}
}
