package org.pih.hivmigration.etl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * Main class for executing the HIV migration using Spark
 */
public class SparkMigrator {

    private static final Log log = LogFactory.getLog(SparkMigrator.class);

    /**
     * Run the application
     */
	public static void main(String[] args) {

        log.info("Starting up HIV Migrator");

        SparkSession spark = SparkSession
                .builder()
                .appName("HIV Migration Application")
                .config("spark.master", "local")
                .getOrCreate();

        // Register custom functions
        spark.udf().register("uuid", new Functions.Uuid(), DataTypes.StringType);
        spark.udf().register("substringbefore", new Functions.SubstringBefore(), DataTypes.StringType);

        // Execute migrations

        UserMigrator userMigrator = new UserMigrator();
        userMigrator.migrate(spark);
    }
}
