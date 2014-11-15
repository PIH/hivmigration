package org.pih.hivmigration.export;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.pih.hivmigration.common.util.ListMap;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
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

	public static void executeUpdate(String sql, Object...params) {
		try {
			QueryRunner runner = new QueryRunner();
			runner.update(connection, sql, params);
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to execute query: " + sql, e);
		}
	}

	public static <T> ListMap<Integer, T> listMapResult(StringBuilder sql, final Class<T> type, final JoinData... joinData) {
		return listMapResult(sql, type, Arrays.asList(joinData));
	}

	public static <T> ListMap<Integer, T> listMapResult(StringBuilder sql, final Class<T> type, final List<JoinData> joinData) {
		return executeQuery(sql.toString(), new ResultSetHandler<ListMap<Integer, T>>() {
			public ListMap<Integer, T> handle(ResultSet resultSet) throws SQLException {
				ListMap<Integer, T> ret = new ListMap<Integer, T>();
				ResultSetMetaData md = resultSet.getMetaData();
				while (resultSet.next()) {
					Map<String, Object> row = new HashMap<String, Object>();

					// Add column data
					for (int i=1; i<=md.getColumnCount(); i++) {
						row.put(md.getColumnName(i), resultSet.getObject(i));
					}
					// Add join data
					if (joinData != null) {
						for (JoinData jd : joinData) {
							Integer foreignKey = ExportUtil.convertValue(resultSet.getObject(jd.getColumnName()), Integer.class);
							Object joinedValue = jd.getPropertyValues().get(foreignKey);
							row.put(jd.getPropertyName(), joinedValue);
						}
					}

					T object = ExportUtil.toObject(type, row);
					Integer key = ExportUtil.convertValue(resultSet.getObject(1), Integer.class);

					ret.putInList(key, object);
				}
				return ret;
			}
		});
	}

	public static <T> Map<Integer, T> mapResult(StringBuilder sql, final Class<T> type, final JoinData... joinData) {
		return mapResult(sql, type, Arrays.asList(joinData));
	}

	public static <T> Map<Integer, T> mapResult(StringBuilder sql, final Class<T> type, final List<JoinData> joinData) {
		return executeQuery(sql.toString(), new ResultSetHandler<Map<Integer, T>>() {
			public Map<Integer, T> handle(ResultSet resultSet) throws SQLException {
				Map<Integer, T> ret = new LinkedHashMap<Integer, T>();
				ResultSetMetaData md = resultSet.getMetaData();
				while (resultSet.next()) {
					Map<String, Object> row = new HashMap<String, Object>();

					// Add column data
					for (int i=1; i<=md.getColumnCount(); i++) {
						row.put(md.getColumnName(i), resultSet.getObject(i));
					}
					// Add join data
					if (joinData != null) {
						for (JoinData jd : joinData) {
							Integer foreignKey = ExportUtil.convertValue(resultSet.getObject(jd.getColumnName()), Integer.class);
							Object joinedValue = jd.getPropertyValues().get(foreignKey);
							row.put(jd.getPropertyName(), joinedValue);
						}
					}

					Integer key = ExportUtil.convertValue(resultSet.getObject(1), Integer.class);
					T object = ExportUtil.toObject(type, row);
					ret.put(key, object);
				}
				return ret;
			}
		});
	}

	public static <T> T uniqueResult(String sql, Class<T> type, Object... arguments) {
		Object result = executeQuery(sql, new ScalarHandler<Object>(), arguments);
		return ExportUtil.convertValue(result, type);
	}

	public static <T> List<T> listResult(String sql, Class<T> type, Object... arguments) {
		return executeQuery(sql, new ColumnListHandler<T>(), arguments);
	}

	public static List<Map<String, Object>> tableResult(String sql, Object... arguments) {
		return executeQuery(sql, new MapListHandler(), arguments);
	}

	public static <T extends Enum> ListMap<Integer, T> enumResult(StringBuilder sql, final Class<T> type, Object...arguments) {
		return executeQuery(sql.toString(), new ResultSetHandler<ListMap<Integer, T>>() {
			public ListMap<Integer, T> handle(ResultSet resultSet) throws SQLException {
				ListMap<Integer, T> ret = new ListMap<Integer, T>();
				while (resultSet.next()) {
					Integer key = ExportUtil.convertValue(resultSet.getObject(1), Integer.class);
					T value = ExportUtil.convertValue(resultSet.getObject(2), type);
					ret.putInList(key, value);
				}
				return ret;
			}
		}, arguments);
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
		return uniqueResult(sql, Integer.class);
	}

	public static int getNumberOfNullValues(String table, String column) {
		String sql = "select count(*) from " + table + " where " + column + " is null";
		return uniqueResult(sql, Integer.class);
	}

	public static int getNumberOfDistinctNonNullValues(String table, String column) {
		String sql = "select count(distinct("+column+")) from " + table;
		return uniqueResult(sql, Integer.class);
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

	public static List<Map<String, Object>> getForeignKeysToTable(String tableName, String columnName) {
		StringBuilder query = new StringBuilder();
		query.append("select 	cons.table_name as tableName, cols.column_name as columnName ");
		query.append("from		user_constraints cons, user_cons_columns cols, user_constraints cons_r, user_cons_columns cols_r ");
		query.append("where		cons.constraint_name = cols.constraint_name ");
		query.append("and		cons.r_constraint_name = cons_r.constraint_name(+) ");
		query.append("and		cons.r_constraint_name = cols_r.constraint_name(+) ");
		query.append("and		cons.constraint_type = 'R' ");
		query.append("and		upper(cons_r.table_name) = ? ");
		query.append("and		upper(cols_r.column_name) = ? ");
		query.append("order by	cons.table_name, cols.column_name");
		return tableResult(query.toString(), tableName.toUpperCase(), columnName.toUpperCase());
	}

	public static List<String> getForeignKeysToTable(String tableName) {
		StringBuilder query = new StringBuilder();
		query.append("select 	distinct cons.table_name ");
		query.append("from		user_constraints cons, user_cons_columns cols, user_constraints cons_r, user_cons_columns cols_r ");
		query.append("where		cons.constraint_name = cols.constraint_name ");
		query.append("and		cons.r_constraint_name = cons_r.constraint_name(+) ");
		query.append("and		cons.r_constraint_name = cols_r.constraint_name(+) ");
		query.append("and		cons.constraint_type = 'R' ");
		query.append("and		upper(cons_r.table_name) = ? ");
		query.append("order by	cons.table_name");
		return listResult(query.toString(), String.class, tableName.toUpperCase());
	}
}
