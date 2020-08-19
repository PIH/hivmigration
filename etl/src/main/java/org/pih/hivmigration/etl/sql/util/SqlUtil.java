package org.pih.hivmigration.etl.sql.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.time.StopWatch;

public class SqlUtil {

	public static Properties getOracleConnectionProperties() {
		Properties properties = new Properties();
		properties.put("url", "jdbc:oracle:thin:@localhost:1521:XE");
		properties.put("user", "hiv");
		properties.put("password", "hiv");
		return properties;
	}

	public static Properties getMysqlConnectionProperties() {
		Properties properties = new Properties();
		properties.put("url", "jdbc:mysql://localhost:3308/openmrs?rewriteBatchedStatements=true");
		properties.put("user", "root");
		properties.put("password", "root");
		return properties;
	}

	public static Connection getConnection(Properties p) throws SQLException {
		return DriverManager.getConnection(p.getProperty("url"), p);
	}

	public static void executeMysql(String update) throws SQLException {
		QueryRunner qr = new QueryRunner();
		try (Connection connection = getConnection(getMysqlConnectionProperties())) {
			// Parse the source query into statements
			List<String> stmts = SqlStatementParser.parseSqlIntoStatements(update, ";");
			for (Iterator<String> sqlIterator = stmts.iterator(); sqlIterator.hasNext();) {
				String sqlStatement = sqlIterator.next();
				qr.update(connection, sqlStatement);
			}
		}
	}

	public static void loadFromOracleToMySql(String targetStatement, String sourceQuery) throws Exception {

		Connection sourceConnection = null;
		Connection targetConnection = null;

		try {
			sourceConnection = getConnection(getOracleConnectionProperties());
			targetConnection = getConnection(getMysqlConnectionProperties());
			boolean originalTargetAutocommit = targetConnection.getAutoCommit();
			try {
				targetConnection.setAutoCommit(false);

				// Parse the source query into statements
				List<String> stmts = SqlStatementParser.parseSqlIntoStatements(sourceQuery, ";");

				// Iterate over each statement, and execute.  The final statement is expected to select the data out.
				for (Iterator<String> sqlIterator = stmts.iterator(); sqlIterator.hasNext();) {
					String sqlStatement = sqlIterator.next();
					Statement statement = null;
					try {
						System.out.println("Executing: " + sqlStatement);
						StopWatch sw = new StopWatch();
						sw.start();
						statement = sourceConnection.createStatement();
						statement.execute(sqlStatement);
						System.out.println("Statement executed");
						if (!sqlIterator.hasNext()) {
							System.out.println("This is the last statement, treat it as the extraction query");
							ResultSet resultSet = null;
							try {
								resultSet = statement.getResultSet();
								if (resultSet != null) {
									int columnCount = resultSet.getMetaData().getColumnCount();
									System.out.println("Query is returning results with " + columnCount + " columns.  Importing now...");

									int batchSize = 200;
									int batchesProcessed = 0;
									int rowsToProcess = 0;

									System.out.println("Importing batches of " + batchSize);
									try (PreparedStatement stmt = targetConnection.prepareStatement(targetStatement)) {
										while (resultSet.next()) {
											for (int i = 1; i <= columnCount; i++) {
												stmt.setObject(i, resultSet.getObject(i));
											}
											stmt.addBatch();
											rowsToProcess++;
											if (rowsToProcess % batchSize == 0) {
												stmt.executeBatch();
												targetConnection.commit();
												batchesProcessed++;
												rowsToProcess = 0;
												System.out.println("Rows committed: " + batchesProcessed * batchSize);
											}
										}
										if (rowsToProcess > 0) {
											statement.executeBatch();
											targetConnection.commit();
										}
									}
									System.out.println("Import completed: " + (batchesProcessed * batchSize + rowsToProcess) + " rows");
								}
								else {
									throw new RuntimeException("Invalid SQL extraction, no result set found");
								}
							}
							finally {
								DbUtils.closeQuietly(resultSet);
							}
						}
						sw.stop();
						System.out.println("Statement executed in: " + sw.toString());
					}
					finally {
						DbUtils.closeQuietly(statement);
					}
				}
			}
			finally {
				sourceConnection.rollback();
				targetConnection.setAutoCommit(originalTargetAutocommit);
			}
		}
		finally {
			DbUtils.closeQuietly(targetConnection);
			DbUtils.closeQuietly(sourceConnection);
		}
	}

}
