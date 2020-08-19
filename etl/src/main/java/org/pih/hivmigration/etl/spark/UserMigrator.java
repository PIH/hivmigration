package org.pih.hivmigration.etl.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * Main class for executing the HIV migration using Spark
 */
public class UserMigrator extends HivMigrator {

    private static final Log log = LogFactory.getLog(UserMigrator.class);

    /**
     * Run the application
     */
	public void migrate(SparkSession spark) {

        log.info("User Migration");

        // Create view of user data in oracle
        executeOracleUpdateFromResource("/views/migration_users.sql");

        Integer maxUserId = 100; //singleValue(loadFromMysql(spark, "(select max(user_id) from users) MU"));
        Integer maxPersonId = 100; //singleValue(loadFromMysql(spark, "(select max(person_id) from person) MP"));

        // Load users from a view defined in the HIVEMR that contains all relevant user data
        // Add derived columns using user-defined functions to fill out the necessary properties for OpenMRS

        Dataset<Row> userData = loadFromOracle(spark, "migration_users")
                .select("SOURCE_USER_ID", "EMAIL", "FIRST_NAME", "LAST_NAME", "PASSWORD", "SALT", "MEMBER_STATE")
                .withColumn("USER_ID", col("SOURCE_USER_ID").$plus(maxUserId))
                .withColumn("PERSON_ID", col("SOURCE_USER_ID").$plus(maxPersonId))
                .withColumn("PERSON_UUID", callUDF("uuid", lit("user_person"), col("SOURCE_USER_ID")))
                .withColumn("PERSON_NAME_UUID", callUDF("uuid", lit("user_person_name"), col("SOURCE_USER_ID")))
                .withColumn("USER_UUID", callUDF("uuid", lit("user"), col("SOURCE_USER_ID")))
                .withColumn("DATE_CREATED", current_date())
                .withColumn("CREATOR", lit(1))
                .withColumn("USERNAME", concat_ws("-", callUDF("substringbefore",col("EMAIL"), lit("@")), col("SOURCE_USER_ID")))
                .withColumn("RETIRED", col("MEMBER_STATE").isin("deleted", "banned"))
                .withColumn("RETIRED_BY", when(col("MEMBER_STATE").isin("deleted", "banned"), 1))
                .withColumn("DATE_RETIRED", when(col("MEMBER_STATE").isin("deleted", "banned"), current_date()))
                .withColumn("RETIRE_REASON", when(col("MEMBER_STATE").isin("deleted", "banned"), col("MEMBER_STATE")))
                ;

        // Export full user data for later use

        writeToCsv(SaveMode.Overwrite, "source-users", userData);

        // Export to persons

        writeToCsv(SaveMode.Append, "import-persons", userData
                .select("PERSON_ID", "PERSON_UUID", "DATE_CREATED", "CREATOR")
                .withColumnRenamed("PERSON_UUID", "UUID")
        );

        // Export to person names

        writeToCsv(SaveMode.Append, "import-person-names", userData
                .select("PERSON_ID", "DATE_CREATED", "CREATOR", "FIRST_NAME", "LAST_NAME")
                .withColumnRenamed("PERSON_NAME_UUID", "UUID")
                .withColumnRenamed("FIRST_NAME", "GIVEN_NAME")
                .withColumnRenamed("LAST_NAME", "FAMILY_NAME")
                .withColumn("PREFERRED", lit(true))
        );

        // Export to users

        writeToCsv(SaveMode.Overwrite, "import-users", userData
                .select("USER_ID","PERSON_ID", "USER_UUID", "DATE_CREATED", "CREATOR", "USERNAME", "PASSWORD", "SALT", "RETIRED", "RETIRED_BY", "DATE_RETIRED", "RETIRE_REASON")
                .withColumnRenamed("USER_UUID", "UUID")
                .withColumn("SYSTEM_ID", col("USERNAME"))
        );

        // Export to user properties

        writeToCsv(SaveMode.Overwrite, "import-user-properties", userData
                .select("USER_ID", "EMAIL")
                .withColumnRenamed("EMAIL", "PROPERTY_VALUE")
                .withColumn("PROPERTY", lit("notificationAddress"))
        );
    }
}
