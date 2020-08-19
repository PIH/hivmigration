package org.pih.hivmigration.etl.spark;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;

/**
 * Main class for migrating from Oracle
 */
public abstract class HivMigrator implements Serializable {

    private static final Log log = LogFactory.getLog(HivMigrator.class);

    /**
     * Execute the migration
     */
	public abstract void migrate(SparkSession spark);

    protected Properties getOracleConnectionProperties() {
        Properties properties = new Properties();
        properties.put("url", "jdbc:oracle:thin:@localhost:1521:XE");
        properties.put("user", "hiv");
        properties.put("password", "hiv");
        return properties;
    }

	protected Properties getMysqlConnectionProperties() {
        Properties properties = new Properties();
        properties.put("url", "jdbc:mysql://localhost:3308/openmrs");
        properties.put("user", "root");
        properties.put("password", "root");
        return properties;
    }

    /**
     * @return the results of the passed query as a Dataset.
     * Query can either be a named query in the extract directory or the name of a table
     */
    protected Dataset<Row> loadFromOracle(SparkSession spark, String tableOrView) {
	    Properties p = getOracleConnectionProperties();
        return spark.read().jdbc(p.getProperty("url"), tableOrView, p);
    }

    /**
     * @return the results of the passed query as a Dataset.
     * Query can either be a named query in the extract directory or the name of a table
     */
    protected Dataset<Row> loadFromMysql(SparkSession spark, String tableOrView) {
        Properties p = getMysqlConnectionProperties();
        return spark.read().jdbc(p.getProperty("url"), tableOrView, p);
    }

    protected void writeToMysql(Dataset<Row> data, SaveMode mode, String table) {
        Properties p = getMysqlConnectionProperties();
        data.write().mode(mode).jdbc(p.getProperty("url"), "hivmigration_users", p);
    }

    protected <T> T singleValue(Dataset<Row> data) {
        Iterator<Row> i = data.toLocalIterator();
        if (!i.hasNext()) {
            return null;
        }
        Row row = i.next();
        return (T)row.get(0);
    }

    protected void executeMysql(String update) {
        Properties p = getMysqlConnectionProperties();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(p.getProperty("url"), p);
            statement = connection.createStatement();
            statement.executeUpdate(update);
        }
        catch (Exception e) {
            throw new RuntimeException("Error executing mysql update", e);
        }
        finally {
            try {
                statement.close();
            }
            catch (Exception e) {}
            try {
                connection.close();
            }
            catch (Exception e) {}
        }
    }

    protected void executeOracleUpdateFromResource(String resourceName) {
        Properties p = getOracleConnectionProperties();
        String sql = readStringFromResource(resourceName);
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(p.getProperty("url"), p);
            statement = connection.createStatement();
            statement.executeUpdate(sql);
        }
        catch (Exception e) {
            throw new RuntimeException("Error executing oracle update", e);
        }
        finally {
            try {
                statement.close();
            }
            catch (Exception e) {}
            try {
                connection.close();
            }
            catch (Exception e) {}
        }
    }

    /**
     * Given a location on the classpath, return the contents of this resource as a String
     */
    protected String readStringFromResource(String resourceName) {
        InputStream is = null;
        try {
            is = HivMigrator.class.getResourceAsStream(resourceName);
            return IOUtils.toString(is, "UTF-8");
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Unable to load resource: " + resourceName, e);
        }
        finally {
            IOUtils.closeQuietly(is);
        }
    }

    protected void writeToCsv(SaveMode mode, String filename, Dataset<Row> dataset) {
        dataset.write()
            .mode(mode)
            .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
            .option("header", "true")
            .csv("/tmp/migration/" + filename + ".csv");
    }

    protected void printDataset(Dataset<Row> dataset) {
        int total = 0;
        for (Iterator<Row> userDataIterator = dataset.toLocalIterator(); userDataIterator.hasNext();) {
            if (total++ == 0) {
                for (String fieldName : dataset.schema().fieldNames()) {
                    System.out.print(fieldName + "\t");
                }
                System.out.println("");
            }
            Row r = userDataIterator.next();
            for (String fieldName : r.schema().fieldNames()) {
                int index = r.fieldIndex(fieldName);
                System.out.print(r.get(index) + "\t");
            }
            System.out.println("");
        }
    }
}
