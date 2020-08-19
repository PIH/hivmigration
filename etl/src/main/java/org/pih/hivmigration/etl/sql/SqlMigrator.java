package org.pih.hivmigration.etl.sql;

import java.io.File;
import java.io.FileInputStream;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.hivmigration.etl.sql.util.SqlStatementParser;
import org.springframework.util.StringUtils;

abstract class SqlMigrator {

    private static final Log log = LogFactory.getLog(SqlMigrator.class);

    public static final String MIGRATION_PROPERTIES_FILE = "MIGRATION_PROPERTIES_FILE";

    abstract void migrate();
    abstract void revert();

    Properties getMigrationProperties() {
        Properties p = new Properties();
        String migrationPropertiesFilePath = System.getenv(MIGRATION_PROPERTIES_FILE);
        if (!StringUtils.isEmpty(migrationPropertiesFilePath)) {
            log.info("Loading migration properties from: " + migrationPropertiesFilePath);
            File f = new File(migrationPropertiesFilePath);
            try (FileInputStream fis = new FileInputStream(f)) {
                p.load(fis);
            }
            catch (Exception e) {
                throw new IllegalStateException("Unable to load migration properties file located at " + migrationPropertiesFilePath);
            }
        }
        return p;
    }

    Properties getOracleConnectionProperties() {
        Properties mp = getMigrationProperties();
        Properties properties = new Properties();
        properties.put("url", mp.getProperty("oracle.url", "jdbc:oracle:thin:@localhost:1521:XE"));
        properties.put("user", mp.getProperty("oracle.username", "hiv"));
        properties.put("password", mp.getProperty("oracle.password", "hiv"));
        return properties;
    }

    Properties getMysqlConnectionProperties() {
        Properties mp = getMigrationProperties();
        Properties properties = new Properties();
        properties.put("url", mp.getProperty("mysql.url", "jdbc:mysql://localhost:3308/openmrs?rewriteBatchedStatements=true"));
        properties.put("user", mp.getProperty("mysql.username", "root"));
        properties.put("password", mp.getProperty("mysql.password", "root"));
        return properties;
    }

    Connection getConnection(Properties p) throws SQLException {
        return DriverManager.getConnection(p.getProperty("url"), p);
    }

    void executeMysql(String update) throws SQLException {
        QueryRunner qr = new QueryRunner();
        try (Connection connection = getConnection(getMysqlConnectionProperties())) {
            List<String> stmts = SqlStatementParser.parseSqlIntoStatements(update, ";");
            for (String sqlStatement : stmts) {
                qr.update(connection, sqlStatement);
            }
        }
    }

    void loadFromOracleToMySql(String targetStatement, String sourceQuery) throws Exception {

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
                        log.debug("Executing: " + sqlStatement);
                        StopWatch sw = new StopWatch();
                        sw.start();
                        statement = sourceConnection.createStatement();
                        statement.execute(sqlStatement);
                        log.debug("Statement executed");
                        if (!sqlIterator.hasNext()) {
                            log.debug("This is the last statement, treat it as the extraction query");
                            ResultSet resultSet = null;
                            try {
                                resultSet = statement.getResultSet();
                                if (resultSet != null) {
                                    int columnCount = resultSet.getMetaData().getColumnCount();
                                    log.info("Oracle extraction query is returning results with " + columnCount + " columns.  Importing now...");

                                    int batchSize = 200;
                                    int batchesProcessed = 0;
                                    int rowsToProcess = 0;

                                    log.info("Importing batches of " + batchSize);
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
                                                log.info("Rows committed: " + batchesProcessed * batchSize);
                                            }
                                        }
                                        if (rowsToProcess > 0) {
                                            statement.executeBatch();
                                            targetConnection.commit();
                                        }
                                    }
                                    log.info("Import completed: " + (batchesProcessed * batchSize + rowsToProcess) + " rows");
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
                        log.info("Statement executed in: " + sw.toString());
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