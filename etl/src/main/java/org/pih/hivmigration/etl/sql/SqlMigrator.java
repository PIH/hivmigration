package org.pih.hivmigration.etl.sql;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.hivmigration.etl.sql.util.SqlStatementParser;
import org.springframework.util.StringUtils;

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

abstract class SqlMigrator {

    private static final Log log = LogFactory.getLog(SqlMigrator.class);

    public static final String MIGRATION_PROPERTIES_FILE = "MIGRATION_PROPERTIES_FILE";

    public Properties properties;

    private int rowLimit = -1;

    public void setRowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
    }

    abstract void migrate();
    abstract void revert();

    Properties getMigrationProperties() {
        if (properties == null) {
            properties = new Properties();
            String migrationPropertiesFilePath = System.getenv(MIGRATION_PROPERTIES_FILE);
            if (!StringUtils.isEmpty(migrationPropertiesFilePath)) {
                log.info("Loading migration properties from: " + migrationPropertiesFilePath);
                File f = new File(migrationPropertiesFilePath);
                try (FileInputStream fis = new FileInputStream(f)) {
                    properties.load(fis);
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to load migration properties file located at " + migrationPropertiesFilePath);
                }
            }
        }
        return properties;
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
        properties.put("url", "jdbc:mysql://" + mp.getProperty("mysql.host", "localhost")
                + ":" + mp.getProperty("mysql.port", "3308") + "/"
                + mp.getProperty("mysql.database", "openmrs") + "?rewriteBatchedStatements=true");
        properties.put("user", mp.getProperty("mysql.username", "root"));
        properties.put("password", mp.getProperty("mysql.password", "root"));
        return properties;
    }

    Connection getConnection(Properties p) throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to find mysql driver");
        }
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

    void executeMysql(String name, String update) throws SQLException {
        log.info("Executing: " + name);
        StopWatch sw = new StopWatch();
        sw.start();
        executeMysql(update);
        sw.stop();
        log.debug("Took " + sw.toString());
    }

    Object selectMysql(String select, ResultSetHandler resultSetHandler) throws SQLException {
        QueryRunner qr = new QueryRunner();
        try (Connection connection = getConnection(getMysqlConnectionProperties())) {
            return qr.query(connection, select, resultSetHandler);
        }
    }

    void setAutoIncrement(String table, String select) throws SQLException {
        Object nextId = this.selectMysql(select, new ScalarHandler());
        String sql = "ALTER TABLE " + table + " AUTO_INCREMENT = " + nextId.toString();
        this.executeMysql(sql);
    }

    void loadFromOracleToMySql(String targetStatement, String sourceQuery) throws Exception {
        loadFromOracleToMySql(targetStatement, sourceQuery, rowLimit);
    }

    void loadFromOracleToMySql(String targetStatement, String sourceQuery, int rowLimit) throws Exception {

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
                                                if (batchSize * batchesProcessed % 50000 < batchSize) {
                                                    log.info("Rows committed: " + batchesProcessed * batchSize);
                                                }
                                            }
                                            if (rowLimit > 0 && batchSize * batchesProcessed + rowsToProcess >= rowLimit) {
                                                break;
                                            };
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

    public void clearTable(String tableName) throws SQLException {
        clearTable(tableName, false);
    }

    public void clearTable(String tableName, boolean disableForeignKeyChecks) throws SQLException {
        executeMysql((disableForeignKeyChecks ? "SET FOREIGN_KEY_CHECKS=0;\n" : "SET FOREIGN_KEY_CHECKS=1;\n")
                + "DROP TABLE IF EXISTS move_tmp_old;\n"
                + "DROP TABLE IF EXISTS move_tmp;\n"
                + "CREATE TABLE move_tmp LIKE " + tableName + ";\n"
                + "RENAME TABLE " + tableName + " TO move_tmp_old, move_tmp TO " + tableName + ";\n"
                + "DROP TABLE move_tmp_old;\n"
                + (disableForeignKeyChecks ? "SET FOREIGN_KEY_CHECKS=1;\n" : ""));
    }

}
