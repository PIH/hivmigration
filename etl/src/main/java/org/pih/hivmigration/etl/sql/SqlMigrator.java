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
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pih.hivmigration.etl.sql.util.FileParser;
import org.pih.hivmigration.etl.sql.util.SqlStatementParser;
import org.springframework.util.StringUtils;

abstract class SqlMigrator {

    private final Log log = LogFactory.getLog(getClass());

    public static final String MIGRATION_PROPERTIES_FILE = "MIGRATION_PROPERTIES_FILE";

    public Properties properties;

    protected int rowLimit = -1;

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
        executeMysql(update, true);
    }

    void executeMysql(String update, boolean logStatements) throws SQLException {
        QueryRunner qr = new QueryRunner();
        try (Connection connection = getConnection(getMysqlConnectionProperties())) {
            List<String> stmts = SqlStatementParser.parseSqlIntoStatements(update, ";");
            for (String sqlStatement : stmts) {
                if (logStatements) {
                    log.info("Executing: SQL '" + abbreviate(sqlStatement) + "'");
                }
                qr.update(connection, sqlStatement);
            }
        }
    }

    void executeMysql(String name, String update) throws SQLException {
        log.info("Executing: " + name);
        StopWatch sw = new StopWatch();
        sw.start();
        executeMysql(update, false);
        sw.stop();
        log.debug("Took " + sw.toString());
    }

    Object selectOracle(String select, ResultSetHandler resultSetHandler) throws SQLException {
        QueryRunner qr = new QueryRunner();
        try (Connection connection = getConnection(getOracleConnectionProperties())) {
            return qr.query(connection, select, resultSetHandler);
        }
    }

    Object selectMysql(String select, ResultSetHandler resultSetHandler) throws SQLException {
        QueryRunner qr = new QueryRunner();
        try (Connection connection = getConnection(getMysqlConnectionProperties())) {
            return qr.query(connection, select, resultSetHandler);
        }
    }

    void setAutoIncrement(String table, String select) throws SQLException {
        Object nextId = this.selectMysql(select, new ScalarHandler());
        String sql = "ALTER TABLE " + table + " AUTO_INCREMENT = " + (nextId != null ? nextId.toString() : "1");
        this.executeMysql(sql);
    }

    void loadFromCSVtoMySql(String targetStatement, String sourceCSV) throws Exception{

        log.info("Loading: " + sourceCSV);
        Connection targetConnection = null;

        List<List<String>> rows = FileParser.loadCSV(sourceCSV);

        try {
            targetConnection = getConnection(getMysqlConnectionProperties());
            boolean originalTargetAutocommit = targetConnection.getAutoCommit();

            try {
                targetConnection.setAutoCommit(false);

                if (rows != null) {

                    int batchSize = 200;
                    int batchesProcessed = 0;
                    int rowsToProcess = 0;

                    Iterator<List<String>> rowsIterator = rows.iterator();
                    log.info("Importing batches of " + batchSize);
                    try (PreparedStatement stmt = targetConnection.prepareStatement(targetStatement)) {
                        while (rowsIterator.hasNext()) {
                            // assumes each row is the same size and matches target statement
                            List<String> row = rowsIterator.next();
                            for (int i = 0; i < row.size(); i++) {
                                stmt.setObject(i + 1, row.get(i));
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
                        }
                        if (rowsToProcess > 0) {
                            stmt.executeBatch();
                            targetConnection.commit();
                        }
                    }
                    log.info("Import completed: " + (batchesProcessed * batchSize + rowsToProcess) + " rows");
                }

            }
            finally {
                targetConnection.setAutoCommit(originalTargetAutocommit);
            }
        }
        finally {
            DbUtils.closeQuietly(targetConnection);
        }
    }

    void loadFromOracleToMySql(String targetStatement, String sourceQuery) throws Exception {
        loadFromOracleToMySql(targetStatement, sourceQuery, rowLimit);
    }

    void loadFromOracleToMySql(String targetStatement, String sourceQuery, int rowLimit) throws Exception {

        log.info("Executing: Load from Oracle to MySQL '" + abbreviate(targetStatement) + "'");
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

                                    log.info("    Importing batches of " + batchSize);
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
                                                    log.info("    Rows committed: " + batchesProcessed * batchSize);
                                                }
                                            }
                                            if (rowLimit > 0 && batchSize * batchesProcessed + rowsToProcess >= rowLimit) {
                                                break;
                                            };
                                        }
                                        if (rowsToProcess > 0) {
                                            stmt.executeBatch();
                                            targetConnection.commit();
                                        }
                                    }
                                    log.info("    Import completed: " + (batchesProcessed * batchSize + rowsToProcess) + " rows");
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
                        log.info("    Statement executed in: " + sw.toString());
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

    public boolean tableExists(String tableName) throws Exception {

        String select = "select count(*) from information_schema.tables " +
                "where table_schema='" + (getMigrationProperties().get("mysql.database") != null ? getMigrationProperties().get("mysql.database") : "openmrs") + "' " +
                "and table_name = '" + tableName + "';";

        Long count = (Long) selectMysql(select,  new ScalarHandler());

        return count == 1;
    }

    public void clearTable(String tableName) throws SQLException {
        executeMysql("Deleting entries from table '" + tableName + "'",
                "SET FOREIGN_KEY_CHECKS = 0;\n TRUNCATE TABLE " + tableName + ";\n SET FOREIGN_KEY_CHECKS = 1;");
        executeMysql("ALTER TABLE " + tableName + " AUTO_INCREMENT = 1;");
    }

    private String abbreviate(String str) {
        String cleanStr = str.replaceAll("\\s+", " ").trim();
        if (cleanStr.length() > 80) {
            return cleanStr.substring(0, 80) + "...";
        } else {
            return cleanStr;
        }
    }

}
