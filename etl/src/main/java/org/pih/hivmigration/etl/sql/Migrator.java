package org.pih.hivmigration.etl.sql;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.sound.midi.SysexMessage;
import java.lang.reflect.InvocationTargetException;

/**
 * Main class for executing the HIV migration using Spark
 */
public class Migrator {

    private static final Log log = LogFactory.getLog(Migrator.class);
    private static boolean shouldRevert = true;

    /**
     * Run the application
     */
	public static void main(String[] args) {
        log.info("Starting up HIV Migrator");
        String revertStr = System.getenv("REVERT");
        shouldRevert = revertStr.equals("1");
        String step = System.getenv("STEP");
        if (step != null && !step.isEmpty()) {
            String className = "org.pih.hivmigration.etl.sql." + step + "Migrator";
            try {
                Class cls = Class.forName(className);
                SqlMigrator clsInstance = (SqlMigrator) cls.getDeclaredConstructor().newInstance();
                run(clsInstance);
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                log.error("Invalid value for environment variable STEP provided: '" + step
                        + "'. If provided, it must be the prefix to a migrator name, e.g. 'User'.");
                log.error(e);
                System.exit(1);
            }
        } else {
            run(new UserMigrator());
        }
    }

    public static void run(SqlMigrator migrator) {
        if (shouldRevert) {
            revert(migrator);
        }
        migrate(migrator);
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
