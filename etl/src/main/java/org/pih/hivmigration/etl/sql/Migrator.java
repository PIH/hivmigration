package org.pih.hivmigration.etl.sql;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Main class for executing the HIV migration using Spark
 */
public class Migrator {

    private static final Log log = LogFactory.getLog(Migrator.class);

    /**
     * Run the application
     */
	public static void main(String[] args) {
        log.info("Starting up HIV Migrator");
        revert(new UserMigrator());
        migrate(new UserMigrator());
    }

    public static void migrate(SqlMigrator migrator) {
	    log.info("Executing migrator: " + migrator);
        StopWatch sw = new StopWatch();
        sw.start();
        migrator.migrate();
        sw.stop();
        log.info("Migrator " + migrator + " completed in " + sw.toString());
    }

    public static void revert(SqlMigrator migrator) {
        log.info("Executing revert for migrator: " + migrator);
        StopWatch sw = new StopWatch();
        sw.start();
        migrator.revert();
        sw.stop();
        log.info("Migrator " + migrator + " revertion completed in " + sw.toString());
    }
}
